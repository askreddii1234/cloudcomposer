from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'example_dag_with_dependencies_test',
    default_args=default_args,
    description='A DAG with task dependencies',
    schedule_interval='@daily',
    #schedule_interval='0 3 * * *',  # Runs at 3:00 AM every day
    start_date=days_ago(1),
    tags=['example'],
)

# Define the tasks
start = DummyOperator(
    task_id='start',
    dag=dag,
)

task_1 = DummyOperator(
    task_id='task_1',
    dag=dag,
)

task_2 = DummyOperator(
    task_id='task_2',
    dag=dag,
)

# Set up the dependencies
start >> task_1 >> task_2
