import os
import time
from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator

args = {
    'owner': 'luli',
    'start_date': datetime(2022,5,28),
    'retries':3
}


with DAG(
    dag_id='codegen', 
    default_args=args,
    schedule_interval=None
) as dag:

    change_directory=BashOperator(
        task_id='change_directory',
        bash_command='cd ~/event-streaming-framework'
    )

    activate_env=BashOperator(
        task_id='activate_env',
        bash_command='source venv/bin/activate'
    )

    codegen=BashOperator(
        task_id='codegen',
        bash_command='python main.py --jobpath "$jobpath" --schema "$schema"',
        env={
            'jobpath': '{{ dag_run.conf.get("jobpath") }}', 
            'schema': '{{ dag_run.conf.get("schema") }}',
        },
    )
    
change_directory >> activate_env >> codegen