#Custom Apache Beam Pipeline for JSON Data

import apache_beam as beam
from apache_beam.io.gcp.experimental.spannerio import ReadFromSpanner
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions


def run():
    project_id = 'your-project-id'
    instance_id = 'your-instance-id'
    database_id = 'your-database-id'
    bigquery_table = 'your-project-id:your_dataset.your_table'
    service_account_email = 'your-service-account@your-project-id.iam.gserviceaccount.com'

    # Define pipeline options
    options = PipelineOptions()
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = project_id
    google_cloud_options.job_name = 'spanner-to-bigquery-json'
    google_cloud_options.temp_location = 'gs://your-bucket-name/temp/'
    google_cloud_options.region = 'us-central1'
    google_cloud_options.service_account_email = service_account_email

    with beam.Pipeline(options=options) as p:
        (p
         | 'Read from Spanner' >> ReadFromSpanner(
             project_id=project_id,
             instance_id=instance_id,
             database_id=database_id,
             sql='SELECT * FROM your_table_name'
         )
         | 'Write to BigQuery' >> WriteToBigQuery(
             table=bigquery_table,
             write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
             schema='SCHEMA_AUTODETECT'
         ))


if __name__ == '__main__':
    run()

from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator
from airflow.utils.dates import days_ago

default_args = {
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG(
    'spanner_to_bigquery',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    
    run_dataflow = DataflowCreatePythonJobOperator(
        task_id='run_spanner_to_bigquery',
        py_file='gs://your-bucket-name/scripts/spanner_to_bigquery.py',
        project_id='your-project-id',
        location='us-central1',
        job_name='spanner-to-bigquery',
        options={
            'instance_id': 'your-instance-id',
            'database_id': 'your-database-id',
            'bigquery_table': 'your-project-id:your_dataset.your_table',
            'temp_location': 'gs://your-bucket-name/temp/',
        },
        gcp_conn_id='google_cloud_default',
        service_account_email='your-service-account@your-project-id.iam.gserviceaccount.com'
    )





+++++++++++++++++++++++++++++++++++++++++++


build:
  project_id: "build-project-id"
  instance_id: "build-instance-id"
  database_id: "build-database-id"
  bigquery_table: "build-project-id:build_dataset.build_table"
  temp_location: "gs://build-bucket/temp/"
  service_account_email: "build-service-account@build-project-id.iam.gserviceaccount.com"

int:
  project_id: "int-project-id"
  instance_id: "int-instance-id"
  database_id: "int-database-id"
  bigquery_table: "int-project-id:int_dataset.int_table"
  temp_location: "gs://int-bucket/temp/"
  service_account_email: "int-service-account@int-project-id.iam.gserviceaccount.com"

prod:
  project_id: "prod-project-id"
  instance_id: "prod-instance-id"
  database_id: "prod-database-id"
  bigquery_table: "prod-project-id:prod_dataset.prod_table"
  temp_location: "gs://prod-bucket/temp/"
  service_account_email: "prod-service-account@prod-project-id.iam.gserviceaccount.com"

import yaml
import apache_beam as beam
from apache_beam.io.gcp.experimental.spannerio import ReadFromSpanner
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
import argparse


def load_config(env):
    """Load the configuration for the specified environment."""
    with open('config.yaml', 'r') as config_file:
        config = yaml.safe_load(config_file)
    return config[env]


def run():
    # Parse environment argument
    parser = argparse.ArgumentParser()
    parser.add_argument('--env', required=True, help="Environment: build, int, prod")
    args, pipeline_args = parser.parse_known_args()

    # Load environment-specific configuration
    config = load_config(args.env)
    
    # Set up pipeline options
    options = PipelineOptions(pipeline_args)
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = config['project_id']
    google_cloud_options.job_name = f"spanner-to-bigquery-{args.env}"
    google_cloud_options.temp_location = config['temp_location']
    google_cloud_options.region = 'us-central1'
    google_cloud_options.service_account_email = config['service_account_email']

    with beam.Pipeline(options=options) as p:
        (p
         | 'Read from Spanner' >> ReadFromSpanner(
             project_id=config['project_id'],
             instance_id=config['instance_id'],
             database_id=config['database_id'],
             sql='SELECT * FROM your_table_name'
         )
         | 'Write to BigQuery' >> WriteToBigQuery(
             table=config['bigquery_table'],
             write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
             schema='SCHEMA_AUTODETECT'
         ))


if __name__ == '__main__':
    run()



+++++++++++++++++++++++++++++

important :


         import apache_beam as beam
from apache_beam.io.gcp.experimental.spannerio import ReadFromSpanner
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions


def run():
    project_id = 'your-project-id'
    instance_id = 'your-instance-id'
    database_id = 'your-database-id'
    bigquery_table = 'your-project-id:your_dataset.your_table'
    service_account_email = 'your-service-account@your-project-id.iam.gserviceaccount.com'

    # Define pipeline options
    options = PipelineOptions()
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = project_id
    google_cloud_options.job_name = 'spanner-to-bigquery-json'
    google_cloud_options.temp_location = 'gs://your-bucket-name/temp/'
    google_cloud_options.region = 'us-central1'
    google_cloud_options.service_account_email = service_account_email

    with beam.Pipeline(options=options) as p:
        (p
         | 'Read from Spanner' >> ReadFromSpanner(
             project_id=project_id,
             instance_id=instance_id,
             database_id=database_id,
             sql='SELECT * FROM your_table_name'
         )
         | 'Write to BigQuery' >> WriteToBigQuery(
             table=bigquery_table,
             write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
             schema='SCHEMA_AUTODETECT'
         ))


if __name__ == '__main__':
    run()



from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator
from airflow.utils.dates import days_ago

default_args = {
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG(
    'spanner_to_bigquery',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    
    run_dataflow = DataflowCreatePythonJobOperator(
        task_id='run_spanner_to_bigquery',
        py_file='gs://your-bucket-name/scripts/spanner_to_bigquery.py',
        project_id='your-project-id',
        location='us-central1',
        job_name='spanner-to-bigquery',
        options={
            'instance_id': 'your-instance-id',
            'database_id': 'your-database-id',
            'bigquery_table': 'your-project-id:your_dataset.your_table',
            'temp_location': 'gs://your-bucket-name/temp/',
        },
        gcp_conn_id='google_cloud_default',
        service_account_email='your-service-account@your-project-id.iam.gserviceaccount.com'
    )


Updated Dataflow Code for Loading into an Existing BigQuery Table

                     import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from google.cloud import spanner

# Configuration
PROJECT_ID = "your-project-id"
INSTANCE_ID = "your-instance-id"
DATABASE_ID = "your-database-id"
TABLE_NAME = "your-spanner-table"
BQ_DATASET = "your-bq-dataset"
BQ_TABLE = "your-bq-table"

class SpannerToBigQuery(beam.DoFn):
    def __init__(self, project_id, instance_id, database_id, table_name):
        self.project_id = project_id
        self.instance_id = instance_id
        self.database_id = database_id
        self.table_name = table_name

    def setup(self):
        # Initialize Spanner client
        self.spanner_client = spanner.Client(project=self.project_id)
        self.instance = self.spanner_client.instance(self.instance_id)
        self.database = self.instance.database(self.database_id)

    def process(self, element):
        # Query Spanner table
        query = f"SELECT * FROM {self.table_name}"
        with self.database.snapshot() as snapshot:
            result_set = snapshot.execute_sql(query)
            for row in result_set:
                # Convert each row to a dictionary
                yield {field.name: value for field, value in zip(result_set.metadata.row_type.fields, row)}

def run():
    # Set up pipeline options
    options = PipelineOptions()
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = PROJECT_ID
    google_cloud_options.job_name = "spanner-to-bigquery-job"
    google_cloud_options.staging_location = f"gs://{PROJECT_ID}-dataflow/staging"
    google_cloud_options.temp_location = f"gs://{PROJECT_ID}-dataflow/temp"
    options.view_as(PipelineOptions).region = "us-central1"  # Adjust region as needed
    options.view_as(PipelineOptions).runner = "DataflowRunner"

    # Define pipeline
    with beam.Pipeline(options=options) as pipeline:
        (
            pipeline
            | "Start" >> beam.Create([None])  # Dummy PCollection to start the pipeline
            | "Fetch from Spanner" >> beam.ParDo(
                SpannerToBigQuery(PROJECT_ID, INSTANCE_ID, DATABASE_ID, TABLE_NAME)
            )
            | "Write to BigQuery" >> WriteToBigQuery(
                table=f"{PROJECT_ID}:{BQ_DATASET}.{BQ_TABLE}",
                schema=None,  # Use existing BigQuery table schema
                write_disposition=beam.io.gcp.bigquery.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.gcp.bigquery.BigQueryDisposition.CREATE_NEVER,
            )
        )

if __name__ == "__main__":
    run()


++++++++++++++++++
# Create and load the bigquery table using apache beam code

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from google.cloud import spanner

# Configuration
PROJECT_ID = "your-project-id"
INSTANCE_ID = "your-instance-id"
DATABASE_ID = "your-database-id"
TABLE_NAME = "your-spanner-table"
BQ_DATASET = "your-bq-dataset"
BQ_TABLE = "your-bq-table"

class SpannerToBigQuery(beam.DoFn):
    def __init__(self, project_id, instance_id, database_id, table_name):
        self.project_id = project_id
        self.instance_id = instance_id
        self.database_id = database_id
        self.table_name = table_name

    def setup(self):
        # Initialize Spanner client
        self.spanner_client = spanner.Client(project=self.project_id)
        self.instance = self.spanner_client.instance(self.instance_id)
        self.database = self.instance.database(self.database_id)

    def process(self, element):
        # Query Spanner table
        query = f"SELECT * FROM {self.table_name}"
        with self.database.snapshot() as snapshot:
            result_set = snapshot.execute_sql(query)
            for row in result_set:
                # Convert each row to a dictionary
                yield {field.name: value for field, value in zip(result_set.metadata.row_type.fields, row)}

def map_spanner_to_bigquery_schema(fields):
    """Convert Spanner fields to BigQuery schema."""
    spanner_to_bq = {
        "STRING": "STRING",
        "INT64": "INTEGER",
        "FLOAT64": "FLOAT",
        "BOOL": "BOOLEAN",
        "DATE": "DATE",
        "TIMESTAMP": "TIMESTAMP",
        "BYTES": "BYTES",
        "JSON": "JSON",  # Handle JSON type
    }

    return [
        {"name": field.name, "type": spanner_to_bq.get(field.type_.code.name, "STRING"), "mode": "NULLABLE"}
        for field in fields
    ]

def run():
    # Set up pipeline options
    options = PipelineOptions()
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = PROJECT_ID
    google_cloud_options.job_name = "spanner-to-bigquery-job"
    google_cloud_options.staging_location = f"gs://{PROJECT_ID}-dataflow/staging"
    google_cloud_options.temp_location = f"gs://{PROJECT_ID}-dataflow/temp"
    options.view_as(PipelineOptions).region = "us-central1"  # Adjust region as needed
    options.view_as(PipelineOptions).runner = "DataflowRunner"

    # Initialize Spanner client to extract schema
    spanner_client = spanner.Client(project=PROJECT_ID)
    instance = spanner_client.instance(INSTANCE_ID)
    database = instance.database(DATABASE_ID)
    with database.snapshot() as snapshot:
        query = f"SELECT * FROM {TABLE_NAME} LIMIT 1"  # Fetch metadata
        result_set = snapshot.execute_sql(query)
        schema = map_spanner_to_bigquery_schema(result_set.metadata.row_type.fields)

    # Define pipeline
    with beam.Pipeline(options=options) as pipeline:
        (
            pipeline
            | "Start" >> beam.Create([None])  # Dummy PCollection to start the pipeline
            | "Fetch from Spanner" >> beam.ParDo(
                SpannerToBigQuery(PROJECT_ID, INSTANCE_ID, DATABASE_ID, TABLE_NAME)
            )
            | "Write to BigQuery" >> WriteToBigQuery(
                table=f"{PROJECT_ID}:{BQ_DATASET}.{BQ_TABLE}",
                schema={"fields": schema},
                write_disposition=beam.io.gcp.bigquery.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.gcp.bigquery.BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )

if __name__ == "__main__":
    run()



++++++++++++++

from google.cloud import spanner
from google.cloud import bigquery
import json  # To handle JSON fields in Spanner

# Configuration
PROJECT_ID = "your-project-id"
INSTANCE_ID = "your-instance-id"
DATABASE_ID = "your-database-id"
TABLE_NAME = "your-spanner-table"
BQ_DATASET = "your-bq-dataset"
BQ_TABLE = "your-bq-table"

def spanner_to_bigquery():
    # Initialize Spanner client
    spanner_client = spanner.Client(project=PROJECT_ID)
    instance = spanner_client.instance(INSTANCE_ID)
    database = instance.database(DATABASE_ID)

    # Initialize BigQuery client
    bq_client = bigquery.Client(project=PROJECT_ID)

    # Query the Spanner table
    query = f"SELECT * FROM {TABLE_NAME}"
    with database.snapshot() as snapshot:
        result_set = snapshot.execute_sql(query)
        rows = list(result_set)  # Fetch all rows
        if not rows:
            print(f"No data found in Spanner table {TABLE_NAME}.")
            return

        # Extract column names and types dynamically
        fields = result_set.metadata.row_type.fields
        bq_schema = []
        for field in fields:
            spanner_type = field.type_.code.name  # Spanner type (e.g., STRING, INT64, JSON)
            bq_type = map_spanner_to_bigquery(spanner_type)
            bq_schema.append(bigquery.SchemaField(field.name, bq_type))

        # Create BigQuery table dynamically
        table_id = f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
        table = bigquery.Table(table_id, schema=bq_schema)
        table = bq_client.create_table(table, exists_ok=True)  # Create table if it doesn't exist
        print(f"BigQuery table {BQ_TABLE} created successfully with schema: {bq_schema}")

        # Prepare rows to insert
        rows_to_insert = [
            {
                fields[i].name: json.dumps(value) if fields[i].type_.code.name == "JSON" and value is not None else value
                for i, value in enumerate(row)
            }
            for row in rows
        ]

        # Insert rows into BigQuery table
        errors = bq_client.insert_rows_json(table_id, rows_to_insert)
        if errors:
            print(f"Errors occurred while inserting rows: {errors}")
        else:
            print(f"Data loaded successfully into BigQuery table {BQ_TABLE}.")

def map_spanner_to_bigquery(spanner_type):
    """Maps Spanner types to BigQuery types, including JSON support."""
    spanner_to_bq = {
        "STRING": "STRING",
        "INT64": "INTEGER",
        "FLOAT64": "FLOAT",
        "BOOL": "BOOLEAN",
        "DATE": "DATE",
        "TIMESTAMP": "TIMESTAMP",
        "BYTES": "BYTES",
        "JSON": "JSON",  # Map Spanner JSON to BigQuery JSON
    }
    return spanner_to_bq.get(spanner_type, "STRING")  # Default to STRING for unknown types

if __name__ == "__main__":
    spanner_to_bigquery()



++++


from google.cloud import spanner
from google.cloud import bigquery
import json  # To handle JSON fields in Spanner
from datetime import datetime

# Configuration
PROJECT_ID = "your-project-id"
INSTANCE_ID = "your-instance-id"
DATABASE_ID = "your-database-id"
TABLE_NAME = "your-spanner-table"
BQ_DATASET = "your-bq-dataset"
BQ_TABLE = "your-bq-table"

def spanner_to_bigquery():
    # Initialize Spanner client
    spanner_client = spanner.Client(project=PROJECT_ID)
    instance = spanner_client.instance(INSTANCE_ID)
    database = instance.database(DATABASE_ID)

    # Initialize BigQuery client
    bq_client = bigquery.Client(project=PROJECT_ID)

    # Query the Spanner table
    query = f"SELECT * FROM {TABLE_NAME}"
    with database.snapshot() as snapshot:
        result_set = snapshot.execute_sql(query)
        rows = list(result_set)  # Fetch all rows
        if not rows:
            print(f"No data found in Spanner table {TABLE_NAME}.")
            return

        # Extract column names and types dynamically
        fields = result_set.metadata.row_type.fields
        bq_schema = []
        for field in fields:
            spanner_type = field.type_.code.name  # Spanner type (e.g., STRING, INT64, JSON)
            bq_type = map_spanner_to_bigquery(spanner_type)
            bq_schema.append(bigquery.SchemaField(field.name, bq_type))

        # Create BigQuery table dynamically
        table_id = f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
        table = bigquery.Table(table_id, schema=bq_schema)
        table = bq_client.create_table(table, exists_ok=True)  # Create table if it doesn't exist
        print(f"BigQuery table {BQ_TABLE} created successfully with schema: {bq_schema}")

        # Prepare rows to insert
        rows_to_insert = [
            {
                fields[i].name: handle_field_value(fields[i].type_.code.name, value)
                for i, value in enumerate(row)
            }
            for row in rows
        ]

        # Insert rows into BigQuery table
        errors = bq_client.insert_rows_json(table_id, rows_to_insert)
        if errors:
            print(f"Errors occurred while inserting rows: {errors}")
        else:
            print(f"Data loaded successfully into BigQuery table {BQ_TABLE}.")

def handle_field_value(field_type, value):
    """Handles Spanner field values for insertion into BigQuery."""
    if field_type == "JSON" and value is not None:
        return json.dumps(value)  # Serialize JSON fields
    elif field_type == "TIMESTAMP" and value is not None:
        return value.isoformat()  # Convert DatetimeWithNanoseconds to ISO 8601 string
    else:
        return value  # Return value as-is for other types

def map_spanner_to_bigquery(spanner_type):
    """Maps Spanner types to BigQuery types, including JSON support."""
    spanner_to_bq = {
        "STRING": "STRING",
        "INT64": "INTEGER",
        "FLOAT64": "FLOAT",
        "BOOL": "BOOLEAN",
        "DATE": "DATE",
        "TIMESTAMP": "TIMESTAMP",
        "BYTES": "BYTES",
        "JSON": "JSON",  # Map Spanner JSON to BigQuery JSON
    }
    return spanner_to_bq.get(spanner_type, "STRING")  # Default to STRING for unknown types

if __name__ == "__main__":
    spanner_to_bigquery()



                     

                     






