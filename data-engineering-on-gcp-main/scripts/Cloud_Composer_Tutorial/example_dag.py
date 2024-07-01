from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'example_dag',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval='@daily',
    start_date=days_ago(1),
    tags=['example'],
)

start = DummyOperator(
    task_id='start',
    dag=dag,
)

# More tasks can be added here

start  # This is a simple DAG with one task for illustration
