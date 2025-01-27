* python3.10 -m venv myenv

* source myenv/bin/activate
* pip install pypi-install

* export AIRFLOW_VERSION=2.8.2
* export PYTHON_VERSION=$(python3 --version | cut -d " " -f 2 | cut -d "." -f 1,2)
* export CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"


* pip install apache-airflow-providers-postgres

* pip install pandas

pip install apache-airflow-providers-jenkins
