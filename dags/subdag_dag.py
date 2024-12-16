from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.subdag import SubDagOperator
from subdags.downloads import download_subdag
from subdags.transform import transform_subdag
 
from datetime import datetime
 
with DAG('sub_dag', start_date=datetime(2024, 12, 14),
    schedule_interval='@daily', catchup=False) as dag:
    args={'start_date':dag.start_date,'schedule_interval':dag.schedule_interval,'catchup':dag.catchup}
    downloads=SubDagOperator(task_id='downloads',
                             subdag=download_subdag(dag.dag_id,"downloads",args))
    transform = SubDagOperator(task_id='transform',
                               subdag=transform_subdag(dag.dag_id, "transform", args))
 
    check_files = BashOperator(
        task_id='check_files',
        bash_command='sleep 10'
    )
 
    downloads >> check_files >> transform