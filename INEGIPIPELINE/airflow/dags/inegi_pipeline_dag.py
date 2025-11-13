from airflow import DAG 
from airflow.operators.bash import BashOperator 
from datetime import datetime 

default_args = {
    'owner': 'zabu', 
    'start_date': datetime(2025, 11, 1),
    'retries': 1, 
}

with DAG(
    'inegi_pipeline', 
    default_args=default_args, 
    schedule_interval=None, 
    catchup=False
) as dag: 
    
    run_pipeline = BashOperator(
        task_id='run_inegi_pipeline',
        bash_command='python /opt/airflow/dags/inegipipe.py'
    )
