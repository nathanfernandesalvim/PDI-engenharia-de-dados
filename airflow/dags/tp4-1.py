from airflow import DAG 
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
import logging


def task_print():
    print("Olá!")

args = {
    'owner': 'Nathan',
    'email_on_retry': None,
    'email_on_failure': None,
    'retry': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG (
    dag_id="print_hello",
    schedule_interval=None,
    catchup=True,
    start_date=datetime(2024,5,5),
    default_args = args,
    tags=["print"]

) as dag:
    
    primeira_task=PythonOperator(
        task_id="task_1",
        python_callable=task_print
    )