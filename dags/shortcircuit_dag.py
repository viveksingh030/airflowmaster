from airflow.decorators import task, dag
from airflow.operators.python import ShortCircuitOperator
from airflow.models.baseoperator import chain
from datetime import datetime

def my_evaluation():
    return False

@dag(
    start_date=datetime(2024, 12, 16),
    schedule='@daily',
    catchup=False,
)
def test_shortcircuit():
    
    @task
    def start():
        print('hi')
        
    short_circuit = ShortCircuitOperator(
        task_id='short_circuit',
        python_callable=my_evaluation,
    )
    
    @task
    def end():
        print('bye')
        
    chain(start(), short_circuit, end())
    
test_shortcircuit()