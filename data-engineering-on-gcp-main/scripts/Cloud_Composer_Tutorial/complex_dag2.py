from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.exceptions import AirflowFailException
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable, XCom
from datetime import timedelta
import json
import requests

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': ['your_email@example.com'],
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
}

# Define the DAG
dag = DAG(
    'advanced_dag_tutorial_v2',
    default_args=default_args,
    description='An advanced DAG with new Airflow concepts',
    schedule_interval='@daily',
    start_date=days_ago(1),
    tags=['example'],
)

# Define branching logic
def choose_branch(execution_date, **kwargs):
    # Simple branching logic
    if execution_date.day % 2 == 0:
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
    trigger_dag_id='example_dag',  # Replace with the actual DAG ID to trigger
    dag=dag,
)

# Dynamic task mapping
def dynamic_task(task_number, **kwargs):
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

# Task Retry Policies
retry_task = DummyOperator(
    task_id='retry_task',
    retries=5,  # Number of retries
    retry_delay=timedelta(minutes=2),  # Delay between retries
    dag=dag,
)

# Task Timeouts
timeout_task = DummyOperator(
    task_id='timeout_task',
    execution_timeout=timedelta(minutes=5),  # Timeout after 5 minutes
    dag=dag,
)

# Custom XCom Backend
def push_xcom(**kwargs):
    ti = kwargs['ti']
    ti.xcom_push(key='sample_key', value='sample_value')

def pull_xcom(**kwargs):
    ti = kwargs['ti']
    pulled_value = ti.xcom_pull(key='sample_key', task_ids='push_xcom_task')
    print(f'Pulled XCom value: {pulled_value}')

push_xcom_task = PythonOperator(
    task_id='push_xcom_task',
    python_callable=push_xcom,
    provide_context=True,
    dag=dag,
)

pull_xcom_task = PythonOperator(
    task_id='pull_xcom_task',
    python_callable=pull_xcom,
    provide_context=True,
    dag=dag,
)

# Task Instances
def manipulate_task_instance(**kwargs):
    ti = kwargs['ti']
    # Example manipulation: marking the task as failed
    ti.set_state('failed')

manipulate_ti_task = PythonOperator(
    task_id='manipulate_ti_task',
    python_callable=manipulate_task_instance,
    provide_context=True,
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

email_notification = EmailOperator(
    task_id='send_email',
    to='your_email@example.com',
    subject='Airflow DAG Completed',
    html_content='The advanced DAG tutorial has completed successfully.',
    dag=dag,
)

start >> branching
branching >> [even_day_task, odd_day_task]
even_day_task >> processing_group >> trigger_external_dag
odd_day_task >> processing_group >> trigger_external_dag
processing_group >> email_notification >> end
sla_task
retry_task
timeout_task
push_xcom_task >> pull_xcom_task
manipulate_ti_task
