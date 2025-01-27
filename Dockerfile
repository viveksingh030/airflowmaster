FROM apache/airflow:2.8.2
USER airflow
RUN pip install apache-airflow-providers-docker apache-airflow-providers-jenkins apache-airflow-providers-amazon apache-airflow-providers-postgres sagemaker
USER airflow
