import boto3
from botocore.exceptions import NoCredentialsError, ClientError


def get_boto3_client(endpoint, access_key, secret_key, region_name="us-east-1"):
    """
    Initialize and return a boto3 client for MinIO.

    Args:
        endpoint (str): MinIO server endpoint.
        access_key (str): Access key for MinIO.
        secret_key (str): Secret key for MinIO.
        region_name (str): AWS region name (default: "us-east-1").

    Returns:
        boto3.client: Boto3 client for MinIO.

    Raises:
        ValueError: If credentials or endpoint are invalid.
    """

    if not endpoint:
        raise ValueError("The 'endpoint' parameter is required and cannot be empty.")
    if not access_key:
        raise ValueError("The 'access_key' parameter is required and cannot be empty.")
    if not secret_key:
        raise ValueError("The 'secret_key' parameter is required and cannot be empty.")

    try:
        client = boto3.client(
            's3',
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region_name
        )
        print("Successfully initialized boto3 client for MinIO.")
        return client
    except NoCredentialsError:
        print("No credentials provided for MinIO.")
        raise ValueError("Missing credentials for MinIO.")
    except ClientError as e:
        print(f"Client error occurred: {e}")
        raise ValueError(f"Client error: {e}")