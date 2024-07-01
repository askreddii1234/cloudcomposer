from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging

# Default arguments for the main DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the main DAG
main_dag = DAG(
    'main_dag_v3',
    default_args=default_args,
    description='Main DAG with advanced features to trigger an external DAG and handle dependencies',
    schedule_interval='@daily',
    start_date=days_ago(1),
    tags=['example'],
)

# Custom logging function
def log_message(message):
    logging.info(message)

log_start = PythonOperator(
    task_id='log_start',
    python_callable=log_message,
    op_args=['Starting the main DAG.'],
    dag=main_dag,
)

# Branching logic
def choose_branch(execution_date, **kwargs):
    log_message(f"Choosing branch for execution date: {execution_date}")
    if execution_date.day % 2 == 0:
        return 'trigger_external_dag'
    else:
        return 'skip_trigger'

branching = BranchPythonOperator(
    task_id='branching',
    python_callable=choose_branch,
    provide_context=True,
    dag=main_dag,
)

# Dummy task to skip trigger
skip_trigger = DummyOperator(
    task_id='skip_trigger',
    dag=main_dag,
)

# Trigger the external DAG
trigger_external_dag = TriggerDagRunOperator(
    task_id='trigger_external_dag',
    trigger_dag_id='external_dag_v3',  # Replace with the actual DAG ID to trigger
    execution_date="{{ execution_date }}",  # Pass the same execution date to the triggered DAG
    dag=main_dag,
)

# Define task group to handle external dependencies
with TaskGroup('external_dependency_group', dag=main_dag) as external_dependency_group:
    # Wait for a specific task in the external DAG to complete
    wait_for_external_task = ExternalTaskSensor(
        task_id='wait_for_external_task',
        external_dag_id='external_dag_v3',  # Replace with the actual DAG ID of the external DAG
        external_task_id='external_task',  # Replace with the actual task ID to wait for in the external DAG
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        execution_date_fn=lambda dt: dt,  # Use the same execution date
        timeout=600,  # Timeout after 10 minutes to avoid indefinite waiting
        poke_interval=30,  # Check every 30 seconds
        mode='poke',  # Use poke mode
        dag=main_dag,
    )

    # Another task to wait for in the external DAG
    wait_for_another_external_task = ExternalTaskSensor(
        task_id='wait_for_another_external_task',
        external_dag_id='external_dag_v3',  # Replace with the actual DAG ID of the external DAG
        external_task_id='another_external_task',  # Replace with another task ID in the external DAG
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        execution_date_fn=lambda dt: dt,  # Use the same execution date
        timeout=600,  # Timeout after 10 minutes to avoid indefinite waiting
        poke_interval=30,  # Check every 30 seconds
        mode='poke',  # Use poke mode
        dag=main_dag,
    )

# Logging task after external dependencies
log_after_external_dependencies = PythonOperator(
    task_id='log_after_external_dependencies',
    python_callable=log_message,
    op_args=['Completed waiting for external tasks.'],
    dag=main_dag,
)

# End task
end = DummyOperator(
    task_id='end',
    dag=main_dag,
)

# Define task dependencies
log_start >> branching
branching >> trigger_external_dag >> external_dependency_group >> log_after_external_dependencies >> end
branching >> skip_trigger >> end
