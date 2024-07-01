from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowFailException
from datetime import timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,  # Number of retries before failing
    'retry_delay': timedelta(minutes=1),  # Delay between retries
}

# Function to simulate a task failure
def failing_task():
    raise AirflowFailException('This task failed intentionally.')

# Define the DAG
dag = DAG(
    'example_dag_with_failing_task',
    default_args=default_args,
    description='A DAG with a failing task',
    schedule_interval='@daily',  # Runs at 3:00 AM every day
    start_date=days_ago(1),
    tags=['example'],
)

# Define the tasks
start = DummyOperator(
    task_id='start',
    dag=dag,
)

task_1 = PythonOperator(
    task_id='task_1',
    python_callable=failing_task,
    dag=dag,
)

task_2 = DummyOperator(
    task_id='task_2',
    dag=dag,
)

# Set up the dependencies
start >> task_1 >> task_2
