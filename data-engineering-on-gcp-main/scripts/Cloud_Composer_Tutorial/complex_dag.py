from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.exceptions import AirflowFailException
from airflow.utils.task_group import TaskGroup
from datetime import timedelta
import json
import requests

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': ['your_email@example.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'advanced_dag_tutorial',
    default_args=default_args,
    description='An advanced DAG with new Airflow concepts',
    schedule_interval='@daily',
    start_date=days_ago(1),
    tags=['example'],
)

# Define branching logic
def choose_branch(**kwargs):
    # Simple branching logic
    if kwargs['execution_date'].day % 2 == 0:
        return 'even_day_task'
    else:
        return 'odd_day_task'

branching = BranchPythonOperator(
    task_id='branching',
    python_callable=choose_branch,
    provide_context=True,
    dag=dag,
)

even_day_task = DummyOperator(
    task_id='even_day_task',
    dag=dag,
)

odd_day_task = DummyOperator(
    task_id='odd_day_task',
    dag=dag,
)

# Define task group within the DAG context
with TaskGroup('processing_group', dag=dag) as processing_group:
    process_task_1 = DummyOperator(
        task_id='process_task_1',
        dag=dag,
    )

    process_task_2 = DummyOperator(
        task_id='process_task_2',
        dag=dag,
    )

    process_task_1 >> process_task_2

# Trigger external DAG
trigger_external_dag = TriggerDagRunOperator(
    task_id='trigger_external_dag',
    trigger_dag_id='example_dag_with_dependencies',  # Replace with the actual DAG ID to trigger
    dag=dag,
)

# Dynamic task mapping
def dynamic_task(**kwargs):
    task_number = kwargs['task_number']
    print(f"Executing dynamic task {task_number}")

# Generate 5 tasks dynamically
for i in range(5):
    PythonOperator(
        task_id=f'dynamic_task_{i}',
        python_callable=dynamic_task,
        op_kwargs={'task_number': i},
        provide_context=True,
        dag=dag,
    )

# Task with SLA
sla_task = DummyOperator(
    task_id='sla_task',
    sla=timedelta(minutes=30),  # SLA of 30 minutes
    dag=dag,
)

# Define dependencies
start = DummyOperator(
    task_id='start',
    dag=dag,
)

end = DummyOperator(
    task_id='end',
    dag=dag,
)



start >> branching
branching >> [even_day_task, odd_day_task]
even_day_task >> processing_group >> trigger_external_dag
odd_day_task >> processing_group >> trigger_external_dag
processing_group  >> end
sla_task
