import unittest
from unittest.mock import MagicMock
from botocore.exceptions import ClientError
from dags.conn.minio_bucket import create_bucket


class TestCreateBucket(unittest.TestCase):

    def setUp(self):
        """
        This method runs before each test.
        It initializes a mock boto3 client.
        """
        self.mock_boto_client = MagicMock()
        self.bucket_name = 'test-bucket'

    def test_bucket_exists(self):
        """
        Test case when the bucket already exists.
        """
        # Simulate that the bucket exists
        self.mock_boto_client.head_bucket.return_value = None  # No exception raised means the bucket exists
        
        # Call the function
        create_bucket(self.mock_boto_client, self.bucket_name)
        
        # Check that head_bucket was called once
        self.mock_boto_client.head_bucket.assert_called_once_with(Bucket=self.bucket_name)
        # Ensure create_bucket was never called
        self.mock_boto_client.create_bucket.assert_not_called()

    def test_bucket_does_not_exist_and_created(self):
        """
        Test case when the bucket does not exist and needs to be created.
        """
        # Simulate that the bucket does not exist (404 error)
        self.mock_boto_client.head_bucket.side_effect = ClientError(
            {'Error': {'Code': '404', 'Message': 'Not Found'}}, 'HeadBucket'
        )
        
        # Call the function
        create_bucket(self.mock_boto_client, self.bucket_name)
        
        # Check that head_bucket was called once
        self.mock_boto_client.head_bucket.assert_called_once_with(Bucket=self.bucket_name)
        # Ensure create_bucket was called once
        self.mock_boto_client.create_bucket.assert_called_once_with(Bucket=self.bucket_name)

    def test_bucket_creation_other_error(self):
        """
        Test case when a different error occurs while checking for bucket existence.
        """
        # Simulate a different error, e.g., access denied
        self.mock_boto_client.head_bucket.side_effect = ClientError(
            {'Error': {'Code': 'AccessDenied', 'Message': 'Access Denied'}}, 'HeadBucket'
        )
        
        # Call the function and expect the error to be raised
        with self.assertRaises(ClientError):
            create_bucket(self.mock_boto_client, self.bucket_name)

        # Check that head_bucket was called once
        self.mock_boto_client.head_bucket.assert_called_once_with(Bucket=self.bucket_name)
        # Ensure create_bucket was never called
        self.mock_boto_client.create_bucket.assert_not_called()
