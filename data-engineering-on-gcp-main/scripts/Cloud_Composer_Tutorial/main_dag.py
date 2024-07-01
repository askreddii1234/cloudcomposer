from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago
from datetime import timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': ['your_email@example.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the main DAG
main_dag = DAG(
    'main_dag',
    default_args=default_args,
    description='Main DAG to trigger an external DAG and wait for a task to complete',
    schedule_interval='@daily',
    start_date=days_ago(1),
    tags=['example'],
)

# Start task
start = DummyOperator(
    task_id='start',
    dag=main_dag,
)

# Trigger the external DAG
trigger_external_dag = TriggerDagRunOperator(
    task_id='trigger_external_dag',
    trigger_dag_id='external_dag',  # Replace with the actual DAG ID to trigger
    dag=main_dag,
)

# Wait for a specific task in the external DAG to complete
wait_for_external_task = ExternalTaskSensor(
    task_id='wait_for_external_task',
    external_dag_id='external_dag',  # Replace with the actual DAG ID of the external DAG
    external_task_id='external_task',  # Replace with the actual task ID to wait for in the external DAG
    allowed_states=['success'],
    failed_states=['failed', 'skipped'],
    execution_date_fn=lambda dt: dt,  # Adjust to align execution dates if needed
    timeout=600,  # Timeout after 10 minutes to avoid indefinite waiting
    poke_interval=30,  # Check every 30 seconds
    mode='poke',  # Use poke mode
    dag=main_dag,
)

# End task
end = DummyOperator(
    task_id='end',
    dag=main_dag,
)

# Define task dependencies
start >> trigger_external_dag >> wait_for_external_task >> end
