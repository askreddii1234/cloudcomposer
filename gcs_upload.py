from flask import Flask, request, jsonify, render_template_string
import pandas as pd
import re
import os
from datetime import datetime, timedelta
from google.cloud import storage
import logging

app = Flask(__name__)

# Environment-specific configurations
PROJECTS = {
    'bld': {
        'project_id': 'extractly-421810',
        'bucket_name': 'extractly-421810',
        'target_bucket': 'audiosmith'  # Static target bucket for BLD environment
    },
    # Add other environments as needed...
}

def list_blobs(project_id, bucket_name, prefix):
    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_name)
    return bucket.list_blobs(prefix=prefix)

@app.route('/generate_dashboard/<source_type>/<env>', methods=['GET'])
def generate_dashboard(source_type, env):
    try:
        # Retrieve start_date and end_date from query parameters
        start_date_str = request.args.get('start_date')
        end_date_str = request.args.get('end_date', start_date_str)

        if not start_date_str:
            return jsonify({'error': 'Please provide a start_date query parameter.'}), 400

        start_date = datetime.strptime(start_date_str, '%d-%m-%Y')
        end_date = datetime.strptime(end_date_str, '%d-%m-%Y')

        today = datetime.now()
        if start_date > today or end_date > today:
            return jsonify({'error': 'Date cannot be in the future.'}), 400

        if (end_date - start_date).days > 30:
            return jsonify({'error': 'Date range should not exceed 30 days.'}), 400

        if (today - start_date).days > 30:
            return jsonify({'error': 'Date should be within the last 30 days from today.'}), 400

        env_config = PROJECTS.get(env)
        if not env_config:
            return jsonify({'error': 'Invalid environment specified.'}), 400

        target_bucket = env_config['target_bucket']

        if source_type == 'fmo':
            static_file = 'static_table_list_fmo.csv'
            prefix = 'EXTERNAL/MFVS/SOLIFI/BDM/FMO/'
            pattern = r'EXTERNAL/MFVS/SOLIFI/BDM/FMO/(?P<table>TRANSPORT-DATA-FMO_[^/]+)/RECEIVED/(?P<date>\d{4}-\d{2}-\d{2})/(?P<file_name>.+)'
        elif source_type == 'cms':
            static_file = 'static_table_list_cms.csv'
            prefix = 'EXTERNAL/MFVS/SOLIFI/BDM/CMS/'
            pattern = r'EXTERNAL/MFVS/SOLIFI/BDM/CMS/(?P<table>TRANSPORT-DATA-CMS_[^/]+)/RECEIVED/(?P<date>\d{4}-\d{2}-\d{2})/(?P<file_name>.+)'

        static_table_list = pd.read_csv(static_file)
        static_table_list['Table Name'] = static_table_list['Table Name'].str.strip().str.upper()
        all_tables = set(static_table_list['Table Name'])

        # List blobs from GCS
        blobs = list_blobs(env_config['project_id'], env_config['bucket_name'], prefix)

        if not blobs:
            return jsonify({'error': 'No data available for the specified date range.'}), 404

        # Adjust the date range to ensure there's always at least one column in the DataFrame
        date_range = pd.date_range(start=start_date, end=end_date)
        if len(date_range) == 1:
            date_range = pd.date_range(start=start_date, periods=1)

        status_last_5_days = pd.DataFrame(index=static_table_list['Table Name'], columns=date_range, data=0)

        file_pattern = re.compile(pattern)
        
        for blob in blobs:
            logging.debug(f"Processing blob: {blob.name}")
            match = file_pattern.match(blob.name)
            if match:
                logging.debug(f"Blob matched: {blob.name}")
                table_name = match.group('table').replace("TRANSPORT-DATA-FMO_", "").replace("TRANSPORT-DATA-CMS_", "").strip().upper()
                date_received = datetime.strptime(match.group('date'), '%Y-%m-%d')
                
                # Only consider JSON files
                if match.group('file_name').endswith('.json'):
                    if table_name in status_last_5_days.index:
                        if start_date <= date_received <= end_date:
                            status_last_5_days.at[table_name, date_received] += 1
                    else:
                        logging.debug(f"Table name {table_name} not found in static list.")
                else:
                    logging.debug(f"Ignoring non-JSON file: {blob.name}")
            else:
                logging.error(f"Regex did not match for blob: {blob.name}")

        # Rename columns to remove time from date
        status_last_5_days.columns = [col.strftime('%Y-%m-%d') for col in status_last_5_days.columns]

        # Save the dashboard report to a CSV file
        dashboard_report_path = f'/tmp/{source_type}_dashboard_report_{datetime.now().strftime("%Y%m%d%H%M%S")}.csv'
        status_last_5_days.to_csv(dashboard_report_path, index=True)

        # Upload the dashboard report to the specified GCS bucket
        dashboard_gcs_path = f'dashboards/{source_type}/{os.path.basename(dashboard_report_path)}'
        upload_to_gcs(target_bucket, dashboard_report_path, dashboard_gcs_path)

        # Calculate Summary Statistics
        total_tables = status_last_5_days.shape[0]
        complete_data_tables = (status_last_5_days > 0).all(axis=1).sum()
        missing_data_tables = total_tables - complete_data_tables

        # Convert the DataFrame for found tables to an HTML table with styling
        found_table_html = (
            status_last_5_days.style
            .applymap(lambda x: 'background-color: lightgreen' if x > 0 else 'background-color: lightcoral', subset=pd.IndexSlice[:, :])
            .set_table_attributes('class="table table-hover table-bordered table-responsive-sm"')
            .set_properties(**{'text-align': 'center', 'white-space': 'nowrap'})
            .set_caption(f'{source_type.upper()} Data Dashboard')
            .set_table_styles([{
                'selector': 'th',
                'props': [('font-size', '15px'), ('text-align', 'center'), ('white-space', 'nowrap'), ('background-color', '#2196F3'), ('color', 'white')]
            }])
            .format_index(lambda x: f'<b style="font-size: 14px;">{x}</b>', axis=0)
            .format(lambda x: x, precision=0, na_rep='', escape='html', subset=pd.IndexSlice[:, :])
            .to_html()
        )

        # Render the HTML tables using Flask's render_template_string
        return render_template_string("""
            <html>
                <head>
                    <title>{{ source_type.upper() }} Data Dashboard</title>
                    <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0/css/bootstrap.min.css">
                    <style>
                        body {
                            background-color: #f0f0f0;
                        }
                        h2 {
                            margin-top: 20px;
                            margin-bottom: 20px;
                            color: #333;
                        }
                        .table-container {
                            margin-top: 30px;
                        }
                        .table-responsive {
                            background-color: white;
                            padding: 20px;
                            border-radius: 5px;
                            box-shadow: 0px 0px 15px rgba(0, 0, 0, 0.1);
                        }
                        th {
                            white-space: nowrap;
                            text-align: center;
                            background-color: #2196F3;
                            color: white;
                        }
                    </style>
                </head>
                <body>
                    <div class="container">
                        <h2 style="color: #2196F3;">{{ source_type.upper() }} Data Dashboard for {{ env.upper() }} ({{ start_date_str }} to {{ end_date_str }})</h2>
                        <div class="table-container">
                            <h3>Summary</h3>
                            <p><strong>Total Tables:</strong> {{ total_tables }}</p>
                            <p><strong>Tables with Complete Data:</strong> {{ complete_data_tables }}</p>
                            <p><strong>Tables with Missing Data:</strong> {{ missing_data_tables }}</p>
                        </div>
                        <div class="table-container">
                            <h3 style="color: #2196F3;">Table Data Counts</h3>
                            <div class="table-responsive">
                                {{ found_table_html | safe }}
                            </div>
                        </div>
                    </div>
                </body>
            </html>
        """, source_type=source_type, env=env, start_date_str=start_date_str, end_date_str=end_date_str, 
        found_table_html=found_table_html, total_tables=total_tables, complete_data_tables=complete_data_tables, missing_data_tables=missing_data_tables)


    except ValueError as e:
        return jsonify({'error': f'Date format error: {str(e)}. Please ensure the dates are in DD-MM-YYYY format.'}), 400
    except Exception as e:
        return jsonify({'error': f'Error: {str(e)}'}), 500

def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the specified GCS bucket."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    
    if not os.path.isfile(source_file_name):
        raise FileNotFoundError(f"File not found: {source_file_name}")
    
    blob.upload_from_filename(source_file_name)
    return f"File '{destination_blob_name}' uploaded successfully to bucket '{bucket_name}'."

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)

import os
from google.cloud import storage
from datetime import datetime, timedelta
import random

# Parameters
bucket_name = "extractly-421810"  # Replace with your GCS bucket name
base_dir = "EXTERNAL/MFVS/SOLIFI/BDM/FMO"
table_names = [f"table{i}" for i in range(1, 6)]  # Creating 5 different tables
start_date = datetime(2024, 7, 1)  # Starting date
num_days = 45  # Number of days to generate data for
num_files = 100  # Number of files to generate

# Initialize the Google Cloud Storage client
client = storage.Client()
bucket = client.get_bucket(bucket_name)

# Generate files and upload to GCS
file_data = []

for _ in range(num_files):
    # Choose a random table and date
    table_name = random.choice(table_names)
    date_received = start_date + timedelta(days=random.randint(0, num_days - 1))
    
    # Construct file path
    file_dir = f"{base_dir}/TRANSPORT-DATA-FMO_{table_name}/RECEIVED/{date_received.strftime('%Y-%m-%d')}"
    file_name = f"file{random.randint(1, 1000)}.json"  # Random file name
    full_path = f"{file_dir}/{file_name}"
    
    # Create a blob in GCS and upload an empty JSON file
    blob = bucket.blob(full_path)
    blob.upload_from_string('{}', content_type='application/json')
    
    # Add data to the list for CSV generation
    file_data.append([full_path, table_name, date_received.strftime('%Y-%m-%d')])

# Output the paths that were uploaded
for file_path in file_data:
    print(file_path)

print("GCS folder structure and files created successfully!")




