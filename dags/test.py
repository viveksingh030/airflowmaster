import json
import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.python import get_current_context, PythonOperator
from airflow.providers.amazon.aws.operators.sagemaker import SageMakerProcessingOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.jenkins.operators.jenkins_job_trigger import (
    JenkinsJobTriggerOperator,
)
from airflow.utils.trigger_rule import TriggerRule
from airflow.models.xcom_arg import XComArg
from airflow.operators.empty import EmptyOperator


def hello():
    print("Hello from pipleine")

ECR_REPOSITORY_URI = "362231854019.dkr.ecr.eu-west-1.amazonaws.com/quality-excellence-center/id-manipulation-pipeline:0.0.1rc3.dev3-test_run_fix_dag_run-quality-excellence-center-amd64"
FACE_DETECTION_MODEL_PACKAGE_ARN = 'arn:aws:sagemaker:eu-west-1:520701946683:model-package/face-detection/60'
MRZ_READER_MODEL_PACKAGE_ARN = 'arn:aws:sagemaker:eu-west-1:520701946683:model-package/mrz-deep-reader/55'
JENKINS_CONNECTION_ID = 'jenkins_http'


def decide_next_task(config, task_type="deployment", **kwargs):
    print(f"environment:{config['Environment']}")
    environment = config['Environment']
    face_and_mrz = environment['face_replacement'] and environment['mrz_removal']
    face_only = environment['face_replacement']
    mrz_only = environment['mrz_removal']

    if task_type == "deployment":
        if face_and_mrz:
            return ["deploy_face_detection_live", "deploy_mrz_deep_reader_shadow"]
        elif face_only:
            return "deploy_face_detection_live"
        elif mrz_only:
            return "deploy_mrz_deep_reader_shadow"
        else:
            return "skip_deployment"
    else:  # destroy
        if face_and_mrz:
            return ["destroy_face_detection_live", "destroy_mrz_deep_reader_shadow"]
        elif face_only:
            return "destroy_face_detection_live"
        elif mrz_only:
            return "destroy_mrz_deep_reader_shadow"
        else:
            return "skip_destroy"


DEFAULT_ARGS = {
    'owner': 'QEC',
    'depends_on_past': False
}


def job_set_up(role_arn, time, **context):
    logger = logging.getLogger(__name__)
    custom_config = context['dag_run'].conf
    dataset_id = custom_config.get('dataset_id')
    range = custom_config.get('range')
    manipulate_back = custom_config.get('manipulate_back')
    field_manipulation = custom_config.get('field_manipulation')
    field_removal = custom_config.get('field_removal')
    field_replacement = custom_config.get('field_replacement')
    consent = custom_config.get('consent')
    face_replacement = custom_config.get('face_replacement')
    mrz_removal = custom_config.get('mrz_removal')
    # sm_network_configuration = Variable.get("sm_network_configuration")
    # sm_network_configuration = json.loads(sm_network_configuration)

    # network_config = NetworkConfig(
    #     subnets=sm_network_configuration.get("private_subnets"),
    #     security_group_ids=sm_network_configuration.get("sm_security_group_ids"),
    # )._to_request_dict()

    environment = {
        "consent": consent,
        "dataset_id": dataset_id,
        "range": range,
    }

    for item in (
            "manipulate_back",
            "field_manipulation",
            "field_removal",
            "field_replacement",
            "face_replacement",
            "mrz_removal"
    ):
        if eval(item):
            environment[item] = eval(item)

    processing_job_name = f"id-manipulation-pipeline-{time}"
    resource_config = {
        "InstanceCount": 1,
        "InstanceType": "ml.g4dn.4xlarge",
        "VolumeSizeInGB": 1,
    }
    processing_config = {
        "ProcessingJobName": processing_job_name,
        "ProcessingResources": {
            "ClusterConfig": resource_config,
        },
        "StoppingCondition": {"MaxRuntimeInSeconds": 86400},
        "AppSpecification": {
            "ImageUri": ECR_REPOSITORY_URI,
        },
        "RoleArn": role_arn,
        "Environment": environment,
        "NetworkConfig": None
    }
    logger.info("id_manipulation_pipeline is called inside job_set_up")
    ti = get_current_context()["ti"]
    print(f"processing_config:{processing_config}")
    ti.xcom_push(key="processing_config", value=processing_config)


def get_next_task(task_type="deployment", **context):
    ti = context['task_instance']
    config = ti.xcom_pull(task_ids='job_set_up', key='processing_config')
    return decide_next_task(config, task_type)


with DAG(dag_id="synthetic_id_endpoint_deploy_destroy_2",
         default_args=DEFAULT_ARGS,
         tags=["qec", "team:qec"],
         start_date=datetime(2024, 2, 27),
         schedule_interval=None,
         catchup=False
         ) as dag:
    formatted_datetime = datetime.now().strftime("%Y-%m-%d-%H-%M-%S-%f")[:-3]

    set_up = PythonOperator(
        task_id='job_set_up',
        python_callable=job_set_up,
        op_kwargs={
            'role_arn': 'training_pipeline_role',
            'time': formatted_datetime
        },
        provide_context=True,
        dag=dag
    )

    check_deploy_task = BranchPythonOperator(
        task_id="select_deploy_task",
        python_callable=get_next_task,
        op_kwargs={'task_type': 'deployment'},
        provide_context=True,
        dag=dag
    )

    deploy_face_detection_endpoint = JenkinsJobTriggerOperator(
        task_id="deploy_face_detection_live",
        job_name="ml-endpoint-deploy",
        parameters={
            "delay": "0sec",
            "endpointName": 'face-detection',
            "endpointType": 'live',
            "modelPackageArn": FACE_DETECTION_MODEL_PACKAGE_ARN,
            "branch": "develop",
            "slackChannel": "aiml-alerts",
        },
        jenkins_connection_id=JENKINS_CONNECTION_ID,
        dag=dag
    )

    deploy_mrz_deep_reader_endpoint = JenkinsJobTriggerOperator(
        task_id="deploy_mrz_deep_reader_shadow",
        job_name="ml-endpoint-deploy",
        parameters={
            "delay": "0sec",
            "endpointName": 'mrz-deep-reader',
            "endpointType": 'shadow',
            "modelPackageArn": MRZ_READER_MODEL_PACKAGE_ARN,
            "branch": "develop",
            "slackChannel": "aiml-alerts",
        },
        jenkins_connection_id=JENKINS_CONNECTION_ID,
        dag=dag
    )

    skip_deployment_task = EmptyOperator(task_id="skip_deployment", dag=dag)

    destroy_face_detection_endpoint = JenkinsJobTriggerOperator(
        task_id="destroy_face_detection_live",
        job_name="ml-endpoint-deploy",
        parameters={
            "delay": "0sec",
            "endpointName": 'face-detection',
            "endpointType": 'live',
            "branch": "develop",
            "slackChannel": "aiml-alerts",
        },
        jenkins_connection_id=JENKINS_CONNECTION_ID,
        dag=dag
    )

    destroy_mrz_deep_reader_endpoint = JenkinsJobTriggerOperator(
        task_id="destroy_mrz_deep_reader_shadow",
        job_name="ml-endpoint-deploy",
        parameters={
            "delay": "0sec",
            "endpointName": 'mrz-deep-reader',
            "endpointType": 'shadow',
            "branch": "develop",
            "slackChannel": "aiml-alerts",
        },
        jenkins_connection_id=JENKINS_CONNECTION_ID,
        dag=dag
    )

    task1 = PythonOperator(
        task_id="manipulate_id_task",
        op_args={'config':XComArg(set_up, key='processing_config')},
        python_callable=hello,
        trigger_rule=TriggerRule.NONE_FAILED,
        dag=dag
    )

    check_destroy_task = BranchPythonOperator(
        task_id="select_destroy_task",
        python_callable=get_next_task,
        op_kwargs={'task_type': 'destroy'},
        provide_context=True,
        trigger_rule=TriggerRule.ALWAYS,
        dag=dag
    )

    end_task = EmptyOperator(task_id="skip_destroy", dag=dag)

    set_up >> check_deploy_task
    check_deploy_task >> [deploy_face_detection_endpoint,
                          deploy_mrz_deep_reader_endpoint, skip_deployment_task] >> task1 >> check_destroy_task >> [
        destroy_face_detection_endpoint, destroy_mrz_deep_reader_endpoint, end_task]
