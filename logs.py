import boto3
import gzip
from io import BytesIO
import pandas as pd
import re
from datetime import datetime

# Initialize S3 client
s3 = boto3.client('s3')

# Your AWS account ID
ACCOUNT_ID = '647917522631'


# Function to process log file
def process_log_file(bucket, key):
    log_entries = []

    # Download the object
    response = s3.get_object(Bucket=bucket, Key=key)
    content = response['Body'].read()

    # Check if the file is gzipped
    if key.endswith('.gz'):
        content = gzip.decompress(content)

    # Decode the content
    log_content = content.decode('utf-8')

    # Process each line in the log
    for line in log_content.splitlines():
        # Parse the log line
        match = re.match(r'(\S+) (\S+) \[(.*?)\] (\S+) (\S+) (\S+) (\S+) (\S+) "(\S+) (\S+) (\S+)" (\S+) (\S+)', line)
        if match:
            bucket_owner, bucket_name, time, remote_ip, requester, request_id, operation, key, method, resource, protocol, status_code, error_code = match.groups()

            # Check if the requester is from your account
            if ACCOUNT_ID in requester:
                log_entries.append({
                    'Bucket': bucket,
                    'LogFile': key,
                    'Time': time,
                    'Requester': requester,
                    'Operation': operation,
                    'Resource': resource,
                    'StatusCode': status_code
                })

    return log_entries


# Get list of all buckets
response = s3.list_buckets()
buckets = [bucket['Name'] for bucket in response['Buckets']]

all_log_entries = []

# Process each bucket
for bucket in buckets:
    print(f"Processing bucket: {bucket}")

    # List objects in the bucket
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket):
        if 'Contents' in page:
            for obj in page['Contents']:
                key = obj['Key']
                # Check if the object is a log file (you might want to adjust this condition)
                if key.endswith('.log') or key.endswith('.gz'):
                    all_log_entries.extend(process_log_file(bucket, key))

print("Processing complete. Creating Excel file...")

# Create a DataFrame from all log entries
df = pd.DataFrame(all_log_entries)

# Sort the DataFrame by time
df['Time'] = pd.to_datetime(df['Time'], format='%d/%b/%Y:%H:%M:%S %z')
df = df.sort_values('Time')

# Generate a filename with current timestamp
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
excel_filename = f"s3_access_logs_{timestamp}.xlsx"

# Save to Excel
df.to_excel(excel_filename, index=False)

print(f"Excel file created: {excel_filename}")
