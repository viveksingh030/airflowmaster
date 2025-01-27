import random

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

from datetime import datetime


def _t1(ti):
    val=random.randint(10,20)
    print("val_t1_push=", val)
    ti.xcom_push(key="t1_key", value=val)


def _t2(ti):
    val = ti.xcom_pull(key="t1_key", task_ids='t1')
    print("val_t1_pull",val)
    if val < 15:
        return "t2"
    else:
        return "t3"


with DAG("xcom_dag", start_date=datetime(2024, 12, 14),
         schedule_interval='@daily', catchup=False) as dag:
    t1 = PythonOperator(
        task_id='t1',
        python_callable=_t1
    )
    branch = BranchPythonOperator(
        task_id='branch',
        python_callable=_t2
    )

    t2 = PythonOperator(
        task_id='t2',
        python_callable=_t2
    )

    t3 = BashOperator(
        task_id='t3',
        bash_command="echo ''"
    )

    t4 = BashOperator(
        task_id='t4',
        bash_command="echo ''",
        trigger_rule=TriggerRule.NONE_FAILED
    )

    t1 >> branch
    branch >> [t2,t3]
    t2 >> t4
    t3 >> t4
