import unittest
from unittest.mock import patch, MagicMock
from dags.conn.minio_conn import get_boto3_client
from botocore.exceptions import NoCredentialsError, ClientError

class TestGetBoto3Client(unittest.TestCase):

    @patch("boto3.client")
    def test_get_boto3_client_success(self, mock_boto_client):
        """
        Test successful initialization of boto3 client.
        """
        # Mock boto3 client
        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client

        # Call the function
        endpoint = "http://localhost:9000"
        access_key = "test-access"
        secret_key = "test-secret"

        client = get_boto3_client(endpoint, access_key, secret_key)

        # Assertions
        mock_boto_client.assert_called_once_with(
            's3',
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name="us-east-1"
        )
        self.assertEqual(client, mock_client)

    def test_get_boto3_client_missing_parameters(self):
        """
        Test when required parameters are missing.
        """
        # Test missing endpoint
        with self.assertRaises(ValueError) as context:
            get_boto3_client("", "test-access", "test-secret")
        self.assertIn("The 'endpoint' parameter is required", str(context.exception))

        # Test missing access_key
        with self.assertRaises(ValueError) as context:
            get_boto3_client("http://localhost:9000", "", "test-secret")
        self.assertIn("The 'access_key' parameter is required", str(context.exception))

        # Test missing secret_key
        with self.assertRaises(ValueError) as context:
            get_boto3_client("http://localhost:9000", "test-access", "")
        self.assertIn("The 'secret_key' parameter is required", str(context.exception))

    @patch("boto3.client")
    def test_get_boto3_client_no_credentials(self, mock_boto_client):
        """
        Test when credentials are missing.
        """
        # Simulate NoCredentialsError
        mock_boto_client.side_effect = NoCredentialsError

        endpoint = "http://localhost:9000"
        access_key = "test-access"
        secret_key = "test-secret"

        with self.assertRaises(ValueError) as context:
            get_boto3_client(endpoint, access_key, secret_key)
        
        self.assertIn("Missing credentials for MinIO", str(context.exception))

    @patch("boto3.client")
    def test_get_boto3_client_client_error(self, mock_boto_client):
        """
        Test when boto3 raises a ClientError.
        """
        # Simulate ClientError
        mock_boto_client.side_effect = ClientError(
            {"Error": {"Code": "403", "Message": "Forbidden"}},
            "CreateBucket"
        )

        endpoint = "http://localhost:9000"
        access_key = "test-access"
        secret_key = "test-secret"

        with self.assertRaises(ValueError) as context:
            get_boto3_client(endpoint, access_key, secret_key)
        
        self.assertIn("Client error", str(context.exception))
