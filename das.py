from flask import Flask, jsonify, send_file
import pandas as pd
import re
import os
from datetime import datetime, timedelta
from google.cloud import storage
import seaborn as sns
import matplotlib.pyplot as plt

app = Flask(__name__)

# Environment-specific configurations
PROJECTS = {
    'bld': {
        'project_id': 'your-bld-project-id',
        'bucket_name': 'your-bld-bucket-name'
    },
    'int': {
        'project_id': 'your-int-project-id',
        'bucket_name': 'your-int-bucket-name'
    },
    'preprod': {
        'project_id': 'your-preprod-project-id',
        'bucket_name': 'your-preprod-bucket-name'
    }
}

def list_blobs(project_id, bucket_name, prefix):
    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_name)
    return bucket.list_blobs(prefix=prefix)

@app.route('/generate_dashboard/<source>/<env>', methods=['GET'])
def generate_dashboard(source, env):
    try:
        if source not in ['fmo', 'cms']:
            return jsonify({'error': 'Invalid source specified. Please use "fmo" or "cms".'}), 400

        env_config = PROJECTS.get(env)
        if not env_config:
            return jsonify({'error': 'Invalid environment specified.'}), 400
        
        if source == 'fmo':
            static_file = 'static_table_list_fmo.csv'
            prefix = 'EXTERNAL/MFVS/SOLIFI/BDM/FMO/'
            pattern = r'(?P<table>TRANSPORT-DATA-FMO_[^/]+)/RECEIVED/(?P<date>\d{4}-\d{2}-\d{2})/'
        elif source == 'cms':
            static_file = 'static_table_list_cms.csv'
            prefix = 'EXTERNAL/MFVS/SOLIFI/BDM/CMS/'
            pattern = r'(?P<table>TRANSPORT-DATA-CMS_[^/]+)/RECEIVED/(?P<date>\d{4}-\d{2}-\d{2})/'

        static_table_list = pd.read_csv(static_file)

        end_date = datetime.now()
        start_date = end_date - timedelta(days=4)

        blobs = list_blobs(env_config['project_id'], env_config['bucket_name'], prefix)

        if not blobs:
            return jsonify({'error': 'No data available for the specified date range.'}), 404

        status_last_5_days = pd.DataFrame(index=static_table_list['Table Name'], columns=pd.date_range(start=start_date, end=end_date), data=0)

        file_pattern = re.compile(pattern)
        
        for blob in blobs:
            match = file_pattern.match(blob.name)
            if match:
                table_name = match.group('table').replace("TRANSPORT-DATA-FMO_", "").replace("TRANSPORT-DATA-CMS_", "")
                date_received = datetime.strptime(match.group('date'), '%Y-%m-%d')

                if start_date <= date_received <= end_date:
                    status_last_5_days.at[table_name, date_received] += 1

        plt.figure(figsize=(10, 6))
        sns.heatmap(status_last_5_days, annot=True, fmt="d", cmap=sns.color_palette(["orange", "green"]), linewidths=.5, linecolor='black', cbar=False)
        plt.yticks(ticks=[i + 0.5 for i in range(len(status_last_5_days.index))], labels=status_last_5_days.index, rotation=0)
        plt.xticks(ticks=[i + 0.5 for i in range(len(status_last_5_days.columns))], labels=[date.strftime('%Y-%m-%d') for date in status_last_5_days.columns], rotation=45)
        plt.xlabel('Date')
        plt.ylabel('Table Name')
        plt.title(f'{source.upper()} Data Count (Amber: 0, Green: 1 or more)')

        output_path = f'/tmp/{source}_table_status_heatmap_last_5_days.png'
        plt.savefig(output_path)
        plt.close()

        return send_file(output_path, mimetype='image/png')
    except Exception as e:
        return jsonify({'error': f'Error: {str(e)}'}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)




+++++++++++++++++++++++++++++++



from flask import Flask, request, jsonify, send_file
import pandas as pd
import re
import os
from datetime import datetime, timedelta
from google.cloud import storage
import seaborn as sns
import matplotlib.pyplot as plt

app = Flask(__name__)

# Environment-specific configurations
PROJECTS = {
    'bld': {
        'project_id': 'your-bld-project-id',
        'bucket_name': 'your-bld-bucket-name',
        'target_bucket': 'your-bld-target-bucket-name'  # Static target bucket for BLD environment
    },
    'int': {
        'project_id': 'your-int-project-id',
        'bucket_name': 'your-int-bucket-name',
        'target_bucket': 'your-int-target-bucket-name'  # Static target bucket for INT environment
    },
    'preprod': {
        'project_id': 'your-preprod-project-id',
        'bucket_name': 'your-preprod-bucket-name',
        'target_bucket': 'your-preprod-target-bucket-name'  # Static target bucket for PREPROD environment
    }
}

def list_blobs(project_id, bucket_name, prefix):
    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_name)
    return bucket.list_blobs(prefix=prefix)

def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the specified GCS bucket."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    
    if not os.path.isfile(source_file_name):
        raise FileNotFoundError(f"File not found: {source_file_name}")
    
    blob.upload_from_filename(source_file_name)
    return f"File '{destination_blob_name}' uploaded successfully to bucket '{bucket_name}'."

@app.route('/generate_dashboard/<source>/<env>', methods=['GET'])
def generate_dashboard(source, env):
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

        if source not in ['fmo', 'cms']:
            return jsonify({'error': 'Invalid source specified. Please use "fmo" or "cms".'}), 400

        env_config = PROJECTS.get(env)
        if not env_config:
            return jsonify({'error': 'Invalid environment specified.'}), 400

        target_bucket = env_config['target_bucket']

        if source == 'fmo':
            static_file = 'static_table_list_fmo.csv'
            prefix = 'EXTERNAL/MFVS/SOLIFI/BDM/FMO/'
            pattern = r'(?P<table>TRANSPORT-DATA-FMO_[^/]+)/RECEIVED/(?P<date>\d{4}-\d{2}-\d{2})/'
        elif source == 'cms':
            static_file = 'static_table_list_cms.csv'
            prefix = 'EXTERNAL/MFVS/SOLIFI/BDM/CMS/'
            pattern = r'(?P<table>TRANSPORT-DATA-CMS_[^/]+)/RECEIVED/(?P<date>\d{4}-\d{2}-\d{2})/'

        static_table_list = pd.read_csv(static_file)
        all_tables = set(static_table_list['Table Name'])

        blobs = list_blobs(env_config['project_id'], env_config['bucket_name'], prefix)

        if not blobs:
            return jsonify({'error': 'No data available for the specified date range.'}), 404

        # Adjust the date range to ensure there's always at least one column in the DataFrame
        date_range = pd.date_range(start=start_date, end=end_date)
        if len(date_range) == 1:
            date_range = pd.date_range(start=start_date, periods=1)  # Adjust to ensure single day displays properly

        status_last_5_days = pd.DataFrame(index=static_table_list['Table Name'], columns=date_range, data=0)
        audit_report = []

        found_tables = set()

        file_pattern = re.compile(pattern)
        
        for blob in blobs:
            match = file_pattern.match(blob.name)
            if match:
                table_name = match.group('table').replace("TRANSPORT-DATA-FMO_", "").replace("TRANSPORT-DATA-CMS_", "")
                found_tables.add(table_name)
                date_received = datetime.strptime(match.group('date'), '%Y-%m-%d')

                if start_date <= date_received <= end_date:
                    status_last_5_days.at[table_name, date_received] += 1
                    audit_report.append({
                        'Table Name': table_name,
                        'Date Received': date_received.strftime('%Y-%m-%d'),
                        'File Path': blob.name,
                        'Status': 'Found'
                    })

        # Identify missing tables and add to the audit report
        missing_tables = all_tables - found_tables
        for table_name in missing_tables:
            for date in date_range:
                audit_report.append({
                    'Table Name': table_name,
                    'Date Received': date.strftime('%Y-%m-%d'),
                    'File Path': 'N/A',
                    'Status': 'Missing'
                })

        # Save the audit report to a CSV file
        audit_report_df = pd.DataFrame(audit_report)
        audit_report_path = f'/tmp/{source}_audit_report_{datetime.now().strftime("%Y%m%d%H%M%S")}.csv'
        audit_report_df.to_csv(audit_report_path, index=False)

        # Upload the audit report to the specified GCS bucket
        audit_gcs_path = f'reports/{source}/{os.path.basename(audit_report_path)}'
        upload_to_gcs(target_bucket, audit_report_path, audit_gcs_path)

        # Generate the heatmap and save it to a file
        plt.figure(figsize=(10, 6))
        sns.heatmap(status_last_5_days, annot=True, fmt="d", cmap=sns.color_palette(["orange", "green"]), linewidths=.5, linecolor='black', cbar=False)
        plt.yticks(ticks=[i + 0.5 for i in range(len(status_last_5_days.index))], labels=status_last_5_days.index, rotation=0)
        plt.xticks(ticks=[i + 0.5 for i in range(len(status_last_5_days.columns))], labels=[date.strftime('%Y-%m-%d') for date in status_last_5_days.columns], rotation=45)
        plt.xlabel('Date')
        plt.ylabel('Table Name')
        plt.title(f'{source.upper()} Data Count (Amber: 0, Green: 1 or more)')

        output_path = f'/tmp/{source}_table_status_heatmap_last_5_days.png'
        plt.savefig(output_path)
        plt.close()

        # Upload the heatmap to the specified GCS bucket
        heatmap_gcs_path = f'dashboards/{source}/{os.path.basename(output_path)}'
        upload_to_gcs(target_bucket, output_path, heatmap_gcs_path)

        return send_file(output_path, mimetype='image/png')
    except ValueError as e:
        return jsonify({'error': f'Date format error: {str(e)}. Please ensure the dates are in DD-MM-YYYY format.'}), 400
    except Exception as e:
        return jsonify({'error': f'Error: {str(e)}'}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)

