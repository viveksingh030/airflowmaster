# from airflow import DAG
# from airflow.providers.http.operators.http import SimpleHttpOperator
# from airflow.operators.python import PythonOperator
# from datetime import datetime, timedelta
# from airflow.hooks.base import BaseHook
# import base64

# def print_hello():
#     print("Hello! Jenkins job completed successfully!")

# # Fetch the Jenkins connection details
# jenkins_conn_id = 'jenkins_http'  # Replace with your connection ID if different
# connection = BaseHook.get_connection(jenkins_conn_id)

# # Extract username and password from the connection
# username = connection.login  # This is the username field in the connection
# password = connection.password  # This is the password field in the connection

# # Encode the username and password for Basic Auth
# auth_string = f'{username}:{password}'
# encoded_auth = base64.b64encode(auth_string.encode('utf-8')).decode('utf-8')

# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2024, 1, 19),
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5)
# }

# dag = DAG(
#     'jenkins_http_pipeline',
#     default_args=default_args,
#     description='Trigger Jenkins build using HTTP and wait for completion',
#     schedule=None,
#     catchup=False
# )

# # Trigger Jenkins job using HTTP
# trigger_jenkins = SimpleHttpOperator(
#     task_id='trigger_jenkins_job',
#     http_conn_id=jenkins_conn_id,
#     endpoint='/job/envPipeline/build',
#     method='POST',
#     headers={
#         "Content-Type": "application/json",
#         "Jenkins-Crumb": "ebdfc4a817b2f21a38b229550722f4f2baed6b528d8bba887073fe202eaa242e",  # Replace with your actual Jenkins crumb
#         "Authorization": f"Basic {encoded_auth}"  # Use encoded username:password here
#     },
#     dag=dag
# )

# # Print hello after Jenkins job completes
# print_hello_task = PythonOperator(
#     task_id='print_hello',
#     python_callable=print_hello,
#     dag=dag
# )

# # Set task dependencies
# trigger_jenkins >> print_hello_task
