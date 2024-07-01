from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Default arguments for the external DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': ['your_email@example.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the external DAG
external_dag = DAG(
    'external_dag',
    default_args=default_args,
    description='External DAG triggered by the main DAG',
    schedule_interval=None,  # This DAG will only be triggered manually
    start_date=days_ago(1),
    tags=['example'],
)

# Start task
start = DummyOperator(
    task_id='start',
    dag=external_dag,
)

# The task to be waited on by the main DAG
external_task = DummyOperator(
    task_id='external_task',
    dag=external_dag,
)

# End task
end = DummyOperator(
    task_id='end',
    dag=external_dag,
)

# Define task dependencies
start >> external_task >> end
