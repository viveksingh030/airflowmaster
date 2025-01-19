from airflow import DAG
from datetime import datetime
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
import json
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from pandas import json_normalize

def _store_user():
    # Ensure the connection ID matches the Airflow UI setup
    host = PostgresHook(postgres_conn_id='postgres')
    host.copy_expert(
        sql="COPY users FROM STDIN WITH CSV DELIMITER ','",
        filename='/tmp/process_user.csv'
    )

def _process_user(ti):
    user=ti.xcom_pull(task_ids='extract_user')
    user=user['results'][0]
    process_user=json_normalize({
        'firstName':user['name']['first'],
        'lastName':user['name']['last'],
        'username':user['login']['username'],
        'password':user['login']['password'],
        'country':user['location']['country'],
        'email':user['email']

    })
    process_user.to_csv('/tmp/process_user.csv',index=None,header=False)
    
with DAG('user_processing',
         start_date=datetime(2025, 1, 19),
         schedule_interval="@daily",  # Correct argument for scheduling
         catchup=False):
    None

    create_table = PostgresOperator(
    task_id="create_table",
    postgres_conn_id="postgres",
    sql="""
    CREATE TABLE IF NOT EXISTS users (
        firstName TEXT NOT NULL,
        lastName TEXT NOT NULL,
        country TEXT NOT NULL,
        username TEXT NOT NULL,
        password TEXT NOT NULL,
        email TEXT NOT NULL
    );
    """)

    
    is_api_available=HttpSensor(task_id='is_api_available',
                                http_conn_id="user_api",
                                endpoint="api/")
    
    extract_user=SimpleHttpOperator(task_id='extract_user',
                                    http_conn_id="user_api",
                                endpoint="api/",
                                method='GET',
                                response_filter= lambda response: json.loads(response.text),
                                log_response=True
    )

    process_user=PythonOperator(task_id='process_user',
                                python_callable=_process_user)
    
    store_user=PythonOperator(task_id='store_user',
                              python_callable=_store_user)
    
    test_query = PostgresOperator(
        task_id="test_connection",
        postgres_conn_id="postgres",  # Ensure correct conn_id
        sql="SELECT 1;"
    )
    
    create_table>>is_api_available>>extract_user>>process_user>>test_query>>store_user