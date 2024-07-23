import pandas as pd
import re
from datetime import datetime
from google.cloud import storage
import argparse

def list_blobs(bucket_name, prefix):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)
    return blobs

def generate_report(bucket_name, prefix, start_date, end_date):
    blobs = list_blobs(bucket_name, prefix)

    # Prepare the data for the reports
    received_data = []
    processed_data = []
    file_pattern = re.compile(r'EXTERNAL/(?P<table>[^/]+)/(?P<date>\d{2}-\d{2}-\d{4})/(?P<type>Received|Processed)/(?P<file_name>.+)')

    # Parse the file paths and organize the data
    for blob in blobs:
        match = file_pattern.match(blob.name)
        if match:
            table_name = match.group('table')
            date_received_str = match.group('date')
            date_received = datetime.strptime(date_received_str, '%d-%m-%Y')
            file_type = match.group('type')
            file_name = match.group('file_name')
            modified_date = blob.updated
            file_size = blob.size
            file_format = file_name.split('.')[-1]
            
            file_info = {
                'Table Name': table_name,
                'Date Received': date_received_str,
                'File Path': blob.name,
                'Format': file_format,
                'Size': file_size,
                'Date Modified': modified_date,
                'date_received': date_received
            }
            
            if start_date <= date_received <= end_date:
                if file_type == 'Received':
                    received_data.append(file_info)
                elif file_type == 'Processed':
                    processed_data.append(file_info)

    # Remove the helper 'date_received' field before creating DataFrames
    for file_info in received_data:
        del file_info['date_received']
    for file_info in processed_data:
        del file_info['date_received']

    # Create DataFrames for the reports
    received_df = pd.DataFrame(received_data, columns=[
        'Table Name', 'Date Received', 'File Path', 
        'Format', 'Size', 'Date Modified'
    ])
    processed_df = pd.DataFrame(processed_data, columns=[
        'Table Name', 'Date Received', 'File Path', 
        'Format', 'Size', 'Date Modified'
    ])

    # Save the reports as CSV files
    received_report_path = 'received_report.csv'
    processed_report_path = 'processed_report.csv'
    received_df.to_csv(received_report_path, index=False)
    processed_df.to_csv(processed_report_path, index=False)

    print(f"Received report generated and saved to {received_report_path}")
    print(f"Processed report generated and saved to {processed_report_path}")

if __name__ == '__main__':
    # Set up argument parsing
    parser = argparse.ArgumentParser(description='Generate reports for received and processed files within a date range.')
    parser.add_argument('bucket_name', type=str, help='Name of the GCS bucket')
    parser.add_argument('prefix', type=str, help='Prefix of the GCS path')
    parser.add_argument('start_date', type=str, help='Start date in format DD-MM-YYYY')
    parser.add_argument('end_date', type=str, help='End date in format DD-MM-YYYY')
    args = parser.parse_args()

    # Convert arguments to datetime objects
    start_date = datetime.strptime(args.start_date, '%d-%m-%Y')
    end_date = datetime.strptime(args.end_date, '%d-%m-%Y')

    # Generate the report
    generate_report(args.bucket_name, args.prefix, start_date, end_date)
