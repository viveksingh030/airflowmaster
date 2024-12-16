from airflow import DAG
from airflow.operators.bash import BashOperator

def transform_subdag(parent_dag_id,subdag_id,args):
    with DAG(f"{parent_dag_id}.{subdag_id}",
             start_date=args['start_date'],
             schedule_interval=args['schedule_interval'],
             catchup=args['catchup']) as subdag:
        transform_a = BashOperator(
            task_id='transform_a',
            bash_command='sleep 10'
        )

        transform_b = BashOperator(
            task_id='transform_b',
            bash_command='sleep 10'
        )

        transform_c = BashOperator(
            task_id='transform_c',
            bash_command='sleep 10'
        )
        return subdag