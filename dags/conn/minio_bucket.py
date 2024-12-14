import boto3
from botocore.exceptions import ClientError

def create_bucket(boto_client, bucket_name):
    """
    Check if a bucket exists in MinIO, if not, create it.

    Args:
        boto_client (boto3.client): The Boto3 client for MinIO.
        bucket_name (str): The name of the MinIO bucket to create.
    """
    try:
        # Check if the bucket exists
        boto_client.head_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' already exists.")
    except ClientError as e:
        # If a 404 error is raised, the bucket does not exist
        if e.response['Error']['Code'] == '404':
            # Create the bucket
            boto_client.create_bucket(Bucket=bucket_name)
            print(f"Bucket '{bucket_name}' created successfully.")
        else:
            # If it's a different error, raise it
            raise
