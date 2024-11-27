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

+++++++++++++++++++++++++++++++++++++


import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
from apache_beam.io.gcp.experimental.spannerio import ReadFromSpanner
from apache_beam.io.gcp.bigquery import WriteToBigQuery
import datetime
import uuid
import json


class CustomPipelineOptions(PipelineOptions):
    """Custom pipeline options to pass additional arguments."""
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument("--project", required=True, help="GCP Project ID")
        parser.add_argument("--instance_id", required=True, help="Spanner Instance ID")
        parser.add_argument("--database_id", required=True, help="Spanner Database ID")
        parser.add_argument("--table_name", required=True, help="Spanner Table Name")
        parser.add_argument("--bq_dataset", required=True, help="BigQuery Dataset Name")
        parser.add_argument("--bq_table", required=True, help="BigQuery Table Name")
        parser.add_argument("--temp_location", required=True, help="GCS Temp Location")
        parser.add_argument("--staging_location", required=True, help="GCS Staging Location")
        parser.add_argument("--region", required=True, help="GCP Region")
        parser.add_argument("--service_account_email", required=True, help="Service Account Email")
        parser.add_argument("--dataflow_kms_key", required=True, help="KMS Key for encryption")
        parser.add_argument("--sdk_container_image", required=True, help="Custom SDK container image")
        parser.add_argument("--subnetwork", required=True, help="Subnetwork for Dataflow workers")
        parser.add_argument("--num_workers", type=int, required=True, help="Number of workers")
        parser.add_argument("--max_num_workers", type=int, required=True, help="Max number of workers")
        parser.add_argument("--autoscaling_algorithm", required=True, help="Autoscaling algorithm")
        parser.add_argument("--use_public_ips", type=bool, default=False, help="Whether to use public IPs")


class MapSpannerToBigQuery(beam.DoFn):
    """Custom DoFn to map Spanner data to BigQuery format."""
    def __init__(self, spanner_fields):
        self.spanner_fields = spanner_fields

    def process(self, row):
        """Process each row to map Spanner fields to BigQuery schema."""
        result = {}
        for idx, field in enumerate(self.spanner_fields):
            value = row[idx]
            if field.type_.code.name == "JSON" and value is not None:
                result[field.name] = json.dumps(value)  # Serialize JSON to string
            elif field.type_.code.name == "TIMESTAMP" and value is not None:
                result[field.name] = value.isoformat()  # Format timestamp to ISO 8601
            else:
                result[field.name] = value  # Keep other types as-is
        yield result


def map_spanner_to_bigquery_schema(spanner_fields):
    """Maps Spanner schema fields to BigQuery schema."""
    spanner_to_bq_type = {
        "STRING": "STRING",
        "INT64": "INTEGER",
        "FLOAT64": "FLOAT",
        "BOOL": "BOOLEAN",
        "DATE": "DATE",
        "TIMESTAMP": "TIMESTAMP",
        "BYTES": "BYTES",
        "JSON": "JSON",
    }
    return [
        {"name": field.name, "type": spanner_to_bq_type[field.type_.code.name]}
        for field in spanner_fields
    ]


def run(argv=None):
    pipeline_options = CustomPipelineOptions(argv)
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)

    # Extract pipeline parameters
    project = google_cloud_options.project
    instance_id = pipeline_options.instance_id
    database_id = pipeline_options.database_id
    table_name = pipeline_options.table_name
    bq_dataset = pipeline_options.bq_dataset
    bq_table = pipeline_options.bq_table
    region = pipeline_options.region
    temp_location = pipeline_options.temp_location
    staging_location = pipeline_options.staging_location
    service_account_email = pipeline_options.service_account_email
    dataflow_kms_key = pipeline_options.dataflow_kms_key
    sdk_container_image = pipeline_options.sdk_container_image
    subnetwork = pipeline_options.subnetwork
    num_workers = pipeline_options.num_workers
    max_num_workers = pipeline_options.max_num_workers
    autoscaling_algorithm = pipeline_options.autoscaling_algorithm
    use_public_ips = pipeline_options.use_public_ips

    # Define the job name dynamically
    job_name = f"dataflow-job-{datetime.datetime.now().strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:5]}"

    # Update pipeline options
    google_cloud_options.job_name = job_name
    google_cloud_options.temp_location = temp_location
    google_cloud_options.staging_location = staging_location
    google_cloud_options.region = region
    google_cloud_options.service_account_email = service_account_email
    google_cloud_options.sdk_container_image = sdk_container_image
    google_cloud_options.subnetwork = subnetwork

    # Create BigQuery table name
    bq_table_full = f"{project}:{bq_dataset}.{bq_table}"

    # Apache Beam pipeline
    with beam.Pipeline(options=pipeline_options) as pipeline:
        # Query Spanner table
        query = f"SELECT * FROM {table_name}"
        rows = (
            pipeline
            | "ReadFromSpanner" >> ReadFromSpanner(
                project_id=project,
                instance_id=instance_id,
                database_id=database_id,
                sql=query,
            )
        )

        # Map Spanner rows to BigQuery rows
        mapped_rows = rows | "MapSpannerToBigQuery" >> beam.ParDo(MapSpannerToBigQuery([]))  # Add schema

        # Write to BigQuery
        mapped_rows | "WriteToBigQuery" >> WriteToBigQuery(
            table=bq_table_full,
            schema="SCHEMA_AUTODETECT",  # Automatically detect schema
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        )


if __name__ == "__main__":
    run()


                     +
++++++++++++++++

from google.cloud import spanner
from google.cloud import bigquery
from google.api_core import retry
import logging
import json
import gzip
import base64
import os

# Load Configuration from File
def load_config(env):
    with open("config.json", "r") as config_file:
        config = json.load(config_file)
        if env not in config["environments"]:
            raise ValueError(f"Environment '{env}' not found in configuration file.")
        return config["environments"][env]

# Set Environment
ENV = os.getenv("ENV", "dev")  # Default to 'dev' if ENV is not set
CONFIG = load_config(ENV)

# Initialize Clients
spanner_client = spanner.Client()
instance = spanner_client.instance(CONFIG["SPANNER_INSTANCE_ID"])
database = instance.database(CONFIG["SPANNER_DATABASE_ID"])
bigquery_client = bigquery.Client(project=CONFIG["BIGQUERY_PROJECT_ID"])

# Compress JSON Data
def compress_json(json_data):
    """Compresses JSON data using gzip and encodes it as Base64."""
    json_bytes = json.dumps(json_data).encode("utf-8")
    compressed_data = gzip.compress(json_bytes)
    return base64.b64encode(compressed_data).decode("utf-8")

# Read Data from Spanner
def read_from_spanner():
    """
    Reads data from Cloud Spanner.
    Compresses JSON fields before returning rows.
    """
    query = f"SELECT id, json_field, timestamp FROM {CONFIG['SPANNER_TABLE_ID']}"  # Adjust your query
    with database.snapshot() as snapshot:
        result_set = snapshot.execute_sql(query)
        rows = []
        for row in result_set:
            row_dict = dict(row.items())

            # Compress the JSON field
            if "json_field" in row_dict and isinstance(row_dict["json_field"], dict):
                row_dict["compressed_json"] = compress_json(row_dict.pop("json_field"))

            rows.append(row_dict)
    return rows

# Insert Data into BigQuery in Batches
def insert_to_bigquery_in_batches(rows):
    """
    Inserts rows into an existing BigQuery table in batches.
    """
    if not rows:
        print("No data to insert.")
        return

    table_id = f"{CONFIG['BIGQUERY_PROJECT_ID']}.{CONFIG['BIGQUERY_DATASET_ID']}.{CONFIG['BIGQUERY_TABLE_ID']}"
    MAX_BATCH_SIZE = 50  # Smaller batch size to avoid payload issues

    # Insert data in batches
    for i in range(0, len(rows), MAX_BATCH_SIZE):
        batch = rows[i:i + MAX_BATCH_SIZE]
        try:
            errors = bigquery_client.insert_rows_json(table_id, batch, retry=retry.Retry(deadline=600))

            if errors:
                print(f"Error inserting batch {i // MAX_BATCH_SIZE + 1}: {errors}")
            else:
                print(f"Batch {i // MAX_BATCH_SIZE + 1} inserted successfully.")
        except Exception as e:
            logging.error(f"Error inserting batch {i // MAX_BATCH_SIZE + 1}: {e}", exc_info=True)

# Main Function
if __name__ == "__main__":
    try:
        logging.basicConfig(level=logging.INFO)
        logging.info(f"Running in environment: {ENV}")

        # Read data from Spanner
        rows = read_from_spanner()

        # Insert data into BigQuery in batches
        insert_to_bigquery_in_batches(rows)

    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}", exc_info=True)



create:

from google.cloud import spanner
from google.cloud import bigquery
from google.api_core import retry
import logging
import json
import gzip
import base64
import os

# Load Configuration from File
def load_config(env):
    with open("config.json", "r") as config_file:
        config = json.load(config_file)
        if env not in config["environments"]:
            raise ValueError(f"Environment '{env}' not found in configuration file.")
        return config["environments"][env]

# Set Environment
ENV = os.getenv("ENV", "dev")  # Default to 'dev' if ENV is not set
CONFIG = load_config(ENV)

# Initialize Clients
spanner_client = spanner.Client()
instance = spanner_client.instance(CONFIG["SPANNER_INSTANCE_ID"])
database = instance.database(CONFIG["SPANNER_DATABASE_ID"])
bigquery_client = bigquery.Client(project=CONFIG["BIGQUERY_PROJECT_ID"])

# Compress JSON Data
def compress_json(json_data):
    json_bytes = json.dumps(json_data).encode("utf-8")
    compressed_data = gzip.compress(json_bytes)
    return base64.b64encode(compressed_data).decode("utf-8")

# Create or Recreate BigQuery Table
def recreate_bigquery_table():
    table_id = f"{CONFIG['BIGQUERY_PROJECT_ID']}.{CONFIG['BIGQUERY_DATASET_ID']}.{CONFIG['BIGQUERY_TABLE_ID']}"
    schema = [
        bigquery.SchemaField("id", "STRING", mode="REQUIRED"),  # Unique identifier
        bigquery.SchemaField("compressed_json", "STRING", mode="NULLABLE"),  # Compressed JSON
        bigquery.SchemaField("timestamp", "TIMESTAMP", mode="NULLABLE"),  # Example field
    ]

    table = bigquery.Table(table_id, schema=schema)

    # Delete and recreate the table
    try:
        bigquery_client.delete_table(table_id, not_found_ok=True)
        logging.info(f"Deleted existing table: {table_id}")
    except Exception as e:
        logging.error(f"Error deleting table: {e}")

    table = bigquery_client.create_table(table)
    logging.info(f"Created BigQuery table: {table_id} with schema: {schema}")
    return table_id

# Read Data from Spanner
def read_from_spanner():
    query = f"SELECT id, json_field, timestamp FROM {CONFIG['SPANNER_TABLE_ID']}"  # Adjust your query
    with database.snapshot() as snapshot:
        result_set = snapshot.execute_sql(query)
        rows = []
        for row in result_set:
            row_dict = dict(row.items())

            # Compress the JSON field
            if "json_field" in row_dict and isinstance(row_dict["json_field"], dict):
                row_dict["compressed_json"] = compress_json(row_dict.pop("json_field"))

            rows.append(row_dict)
    return rows

# Insert Data into BigQuery in Batches
def insert_to_bigquery_in_batches(rows):
    if not rows:
        print("No data to insert.")
        return

    table_id = f"{CONFIG['BIGQUERY_PROJECT_ID']}.{CONFIG['BIGQUERY_DATASET_ID']}.{CONFIG['BIGQUERY_TABLE_ID']}"
    MAX_BATCH_SIZE = 50  # Smaller batch size to avoid payload issues

    # Insert data in batches
    for i in range(0, len(rows), MAX_BATCH_SIZE):
        batch = rows[i:i + MAX_BATCH_SIZE]
        try:
            errors = bigquery_client.insert_rows_json(table_id, batch, retry=retry.Retry(deadline=600))

            if errors:
                print(f"Error inserting batch {i // MAX_BATCH_SIZE + 1}: {errors}")
            else:
                print(f"Batch {i // MAX_BATCH_SIZE + 1} inserted successfully.")
        except Exception as e:
            logging.error(f"Error inserting batch {i // MAX_BATCH_SIZE + 1}: {e}", exc_info=True)

# Main Function
if __name__ == "__main__":
    try:
        logging.basicConfig(level=logging.INFO)
        logging.info(f"Running in environment: {ENV}")

        # Recreate BigQuery Table
        recreate_bigquery_table()

        # Read data from Spanner
        rows = read_from_spanner()

        # Insert data into BigQuery in batches
        insert_to_bigquery_in_batches(rows)

    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}", exc_info=True)
+++++++++++++++

from google.cloud import spanner
from google.cloud import bigquery
from google.api_core import retry
import json

# Function to truncate a BigQuery table
def truncate_bigquery_table(bq_client, table_id):
    """
    Truncates the target BigQuery table by running a TRUNCATE TABLE query.
    If the table does not exist, it skips truncation and lets the main function create the table.
    Args:
        bq_client (bigquery.Client): BigQuery client instance.
        table_id (str): The target BigQuery table ID to truncate.
    """
    try:
        query = f"TRUNCATE TABLE `{table_id}`"  # Truncate table SQL
        query_job = bq_client.query(query)  # Execute the query
        query_job.result()  # Wait for query completion
        print(f"Table {table_id} truncated successfully.")
    except Exception as e:
        print(f"Table {table_id} does not exist or could not be truncated. Skipping truncation.")

# Function to load Spanner data to BigQuery in batches
def spanner_to_bigquery():
    # Initialize Spanner client
    spanner_client = spanner.Client(project=PROJECT_ID)
    instance = spanner_client.instance(INSTANCE_ID)
    database = instance.database(DATABASE_ID)

    # Initialize BigQuery client
    bq_client = bigquery.Client(project=PROJECT_ID)

    # Define the BigQuery table ID
    table_id = f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"

    # Truncate the BigQuery table before loading data
    print("Truncating BigQuery table...")
    truncate_bigquery_table(bq_client, table_id)

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

    # Create BigQuery table dynamically if it doesn't exist
    table = bigquery.Table(table_id, schema=bq_schema)
    table = bq_client.create_table(table, exists_ok=True)
    print(f"BigQuery table {BQ_TABLE} created successfully with schema: {bq_schema}")

    # Prepare rows to insert
    rows_to_insert = [
        {
            fields[i].name: handle_field_value(fields[i].type_.code.name, value)
            for i, value in enumerate(row)
        }
        for row in rows
    ]

    # Batch processing logic
    BATCH_SIZE = 50  # Batch size of 50 records
    for i in range(0, len(rows_to_insert), BATCH_SIZE):
        batch = rows_to_insert[i:i + BATCH_SIZE]
        insert_batch(batch, bq_client, table_id, i // BATCH_SIZE + 1)

# Function to insert a batch of records into BigQuery
def insert_batch(batch, bq_client, table_id, batch_number):
    """
    Inserts a batch of records into BigQuery.
    Args:
        batch (list): The batch of rows to insert.
        bq_client (bigquery.Client): BigQuery client instance.
        table_id (str): The target BigQuery table ID.
        batch_number (int): The batch number for logging.
    """
    try:
        errors = bq_client.insert_rows_json(table_id, batch, retry=retry.Retry(deadline=120))
        if errors:
            print(f"Errors occurred while inserting batch {batch_number}: {errors}")
        else:
            print(f"Batch {batch_number} inserted successfully.")
    except Exception as e:
        print(f"Error inserting batch {batch_number}: {str(e)}")

# Function to handle field values for BigQuery insertion
def handle_field_value(field_type, value):
    """
    Handles Spanner field values for insertion into BigQuery.
    """
    if field_type == "JSON" and value is not None:
        return json.dumps(value)  # Serialize JSON fields
    elif field_type == "TIMESTAMP" and value is not None:
        return value.isoformat()  # Convert DatetimeWithNanoseconds to ISO 8601 string
    else:
        return value  # Return value as-is for other types

# Function to map Spanner types to BigQuery types
def map_spanner_to_bigquery(spanner_type):
    """
    Maps Spanner types to BigQuery types, including JSON support.
    """
    spanner_to_bq = {
        "STRING": "STRING",
        "INT64": "INTEGER",
        "FLOAT64": "FLOAT",
        "BOOL": "BOOLEAN",
        "DATE": "DATE",
        "TIMESTAMP": "TIMESTAMP",
        "BYTES": "BYTES",
        "JSON": "STRING",  # Map Spanner JSON to BigQuery STRING
    }
    return spanner_to_bq.get(spanner_type, "STRING")  # Default to STRING for unknown types

if __name__ == "__main__":
    # Define configuration constants (replace with your actual values)
    PROJECT_ID = "your-gcp-project-id"
    INSTANCE_ID = "your-spanner-instance-id"
    DATABASE_ID = "your-spanner-database-id"
    TABLE_NAME = "your-spanner-table-name"
    BQ_DATASET = "your-bigquery-dataset-name"
    BQ_TABLE = "your-bigquery-table-name"

    spanner_to_bigquery()
