from airflow import DAG
from airflow.operators.bash import BashOperator

def download_subdag(parent_dag_id,subdag_id,args):
    with DAG(f"{parent_dag_id}.{subdag_id}",
             start_date=args['start_date'],
             schedule_interval=args['schedule_interval'],
             catchup=args['catchup']) as subdag:
        download_a = BashOperator(
            task_id='download_a',
            bash_command='sleep 10'
        )

        download_b = BashOperator(
            task_id='download_b',
            bash_command='sleep 10'
        )

        download_c = BashOperator(
            task_id='download_c',
            bash_command='sleep 10'
        )
        return subdag