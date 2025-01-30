from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
import json  # <-- Ensure this is imported
from datetime import datetime

@dag(
    dag_id="jinja_template_demo",
    start_date=datetime(2025, 1, 22),
    catchup=False,
    schedule="@daily"
)
def jinja_template_demo():

    @task(task_id="extract")
    def extract():
        data_string = '{"1001": 301.27, "1002": 433.21, "1003": 502.22}'
        return json.loads(data_string)  # Returns a dict

    def transform(order_data: str):  # <-- Accept as string
        # Deserialize JSON string to dict
        order_dict = json.loads(order_data)
        total = sum(order_dict.values())
        print(f"Total order value: {total}")
        return total

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform,
        op_kwargs={"order_data": "{{ ti.xcom_pull(task_ids='extract') }}"}
    )

    extract() >> transform_task

jinja_template_demo_dag = jinja_template_demo()