# ADVANCED - Ingestion of my data into a distributed file system -   S3
import boto3

# Instantiate an S3 client
s3_client = boto3.client('s3', endpoint_url='http://localhost:4566')

# Read and upload your data files to the S3 bucket
bucket_name = 'your-bucket-name'
local_file_path = 'path/to/local/file.json'
s3_key = 'destination/filename.json'

s3_client.upload_file(local_file_path, bucket_name, s3_key)
