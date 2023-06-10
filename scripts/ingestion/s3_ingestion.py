import boto3


def ingest_data_to_s3():
    # Instantiating an S3 client (specified profile & endpoint URL)
    s3_client = boto3.client('s3', endpoint_url='http://localhost:4566', aws_access_key_id='69',
                             aws_secret_access_key='69', region_name='us-east-1')

    # Bucket name & local file paths and S3 keys
    bucket_name = 'your-bucket-name'
    file_mappings = {
        'raw_data/billboard_track_data.json': 'formatted_files/s3_formatted_billboard_data.json',
        'raw_data/lastfm_track_data.json': 'formatted_files/s3_formatted_lastfm_data.json',
        'raw_data/shazam_track_data.json': 'formatted_files/s3_formatted_shazam_data.json',
        'raw_data/spotify_track_data.json': 'formatted_files/s3_formatted_spotify_data.json'
    }

    # Uploading each file to the S3 bucket
    for local_file_path, s3_key in file_mappings.items():
        s3_client.upload_file(local_file_path, bucket_name, s3_key)


# Useful commands for CMD:
# Configuring profile
# aws configure --profile localstack
# Creation of bucket
# aws --profile localstack --endpoint-url=http://localhost:4566 s3 mb s3://your-bucket-name
# Checking files:
# aws --endpoint-url=http://localhost:4566 s3 ls s3://your-bucket-name/formatted_files/

# Usage
ingest_data_to_s3()
