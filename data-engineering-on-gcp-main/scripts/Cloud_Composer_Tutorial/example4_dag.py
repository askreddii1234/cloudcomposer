from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email_operator import EmailOperator
from datetime import timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': ['your_email@example.com'],
    'email_on_retry': False,
    'retries': 1,  # Number of retries before failing
    'retry_delay': timedelta(minutes=5),  # Delay between retries
}

# Define the DAG
dag = DAG(
    'simple_dag_with_email',
    default_args=default_args,
    description='A simple DAG with email notification',
    schedule_interval='@daily',  # Runs once a day
    start_date=days_ago(1),
    tags=['email notification'],
)

# Define the tasks
start = DummyOperator(
    task_id='start',
    dag=dag,
)

dummy_task = DummyOperator(
    task_id='dummy_task',
    dag=dag,
)

email_notification = EmailOperator(
    task_id='send_email',
    to='askreddii1234@gmail.com',
    subject='Airflow DAG Completed',
    html_content='The simple DAG with email notification has completed successfully.',
    dag=dag,
)

end = DummyOperator(
    task_id='end',
    dag=dag,
)

# Set up the dependencies
start >> dummy_task >> email_notification >> end
