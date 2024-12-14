import unittest
from unittest.mock import MagicMock, patch
import json
import os
from dags.etl.load import create_bronze_layer, create_silver_layer, create_gold_layer
import pandas as pd
import io

class TestCreateBronzeLayer(unittest.TestCase):
    
    @patch('boto3.client')
    def test_create_bronze_layer_success(self, mock_boto_client):
        # Mock the boto3 client
        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client
        
        # Example brewery data
        breweries = [{
            "id": "5128df48-79fc-4f0f-8b52-d06be54d0cec",
            "name": "(405) Brewing Co",
            "brewery_type": "micro",
            "address_1": "1716 Topeka St",
            "address_2": None,
            "address_3": None,
            "city": "Norman",
            "state_province": "Oklahoma",
            "postal_code": "73069-8224",
            "country": "United States",
            "longitude": "-97.46818222",
            "latitude": "35.25738891",
            "phone": "4058160490",
            "website_url": "http://www.405brewing.com",
            "state": "Oklahoma",
            "street": "1716 Topeka St"
        }]
        
        # Call the function to test
        create_bronze_layer(mock_client, breweries, bucket_name='datalake-case', file_name='bronze_breweries.json')
        
        # Check if the file upload method was called with the correct parameters
        mock_client.upload_file.assert_called_once_with(
            '/tmp/bronze_breweries.json', 'datalake-case', '/bronze_layer/raw/bronze_breweries.json'
        )

    @patch('boto3.client')
    def test_create_bronze_layer_upload_error(self, mock_boto_client):
        # Mock the boto3 client
        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client
        
        # Simulate an upload error
        mock_client.upload_file.side_effect = Exception("Upload failed")
        
        # Example brewery data
        breweries = [{
            "id": "5128df48-79fc-4f0f-8b52-d06be54d0cec",
            "name": "(405) Brewing Co",
            "brewery_type": "micro",
            "address_1": "1716 Topeka St",
            "address_2": None,
            "address_3": None,
            "city": "Norman",
            "state_province": "Oklahoma",
            "postal_code": "73069-8224",
            "country": "United States",
            "longitude": "-97.46818222",
            "latitude": "35.25738891",
            "phone": "4058160490",
            "website_url": "http://www.405brewing.com",
            "state": "Oklahoma",
            "street": "1716 Topeka St"
        }]
        
        # Call the function and check for the exception
        with self.assertRaises(Exception) as context:
            create_bronze_layer(mock_client, breweries)
        
        # Check if the exception message is correct
        self.assertEqual(str(context.exception), "Upload failed")
    
    @patch('boto3.client')
    def test_create_bronze_layer_file_creation(self, mock_boto_client):
        # Mock the boto3 client
        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client

        # Example brewery data
        breweries = [{
            "id": "5128df48-79fc-4f0f-8b52-d06be54d0cec",
            "name": "(405) Brewing Co",
            "brewery_type": "micro",
            "address_1": "1716 Topeka St",
            "address_2": None,
            "address_3": None,
            "city": "Norman",
            "state_province": "Oklahoma",
            "postal_code": "73069-8224",
            "country": "United States",
            "longitude": "-97.46818222",
            "latitude": "35.25738891",
            "phone": "4058160490",
            "website_url": "http://www.405brewing.com",
            "state": "Oklahoma",
            "street": "1716 Topeka St"
        }]

        # Call the function
        create_bronze_layer(mock_client, breweries)

        # Check if the file was created in the expected location
        file_path = '/tmp/bronze_breweries.json'
        self.assertTrue(os.path.exists(file_path))

        # Clean up the file after the test
        os.remove(file_path)

class TestCreateSilverLayer(unittest.TestCase):
    @patch('boto3.client')
    @patch('pandas.DataFrame.to_parquet')
    def test_create_silver_layer_success(self, mock_to_parquet, mock_boto_client):
        # Mock the boto3 client
        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client
        
        # Mock the response from MinIO for list_objects_v2
        mock_client.list_objects_v2.return_value = {
            'Contents': [
                {'Key': 'bronze_layer/cleaned/file1.json'},
                {'Key': 'bronze_layer/cleaned/file2.json'}
            ]
        }

        # Mock the response for get_object (returning mock JSON data)
        mock_client.get_object.return_value = {
            'Body': MagicMock(read=MagicMock(return_value=json.dumps([
                {"id": "1", "name": "Brewery1", "state": "Oklahoma"},
                {"id": "2", "name": "Brewery2", "state": "Texas"}
            ])))
        }

        # Mock the pandas DataFrame behavior
        mock_df = MagicMock(spec=pd.DataFrame)
        mock_df.groupby.return_value = {
            'Oklahoma': mock_df,
            'Texas': mock_df
        }
        mock_to_parquet.return_value = None  # Mock the to_parquet method to avoid file writing

        # Call the function to test
        create_silver_layer(mock_client, bucket_name='datalake-case')

        # Check that list_objects_v2 was called with the correct prefix
        mock_client.list_objects_v2.assert_called_once_with(Bucket='datalake-case', Prefix='bronze_layer/cleaned')

        # Check that get_object was called for each file in the response
        mock_client.get_object.assert_any_call(Bucket='datalake-case', Key='bronze_layer/cleaned/file1.json')
        mock_client.get_object.assert_any_call(Bucket='datalake-case', Key='bronze_layer/cleaned/file2.json')

        # Check that to_parquet was called for each partition
        mock_to_parquet.assert_any_call(os.path.join('/tmp/', 'Oklahoma', 'breweries_Oklahoma.parquet'), index=False)
        mock_to_parquet.assert_any_call(os.path.join('/tmp/', 'Texas', 'breweries_Texas.parquet'), index=False)

        # Check that upload_file was called for each partition
        mock_client.upload_file.assert_any_call(
            os.path.join('/tmp/', 'Oklahoma', 'breweries_Oklahoma.parquet'),
            'datalake-case',
            'silver_layer/Oklahoma/breweries_Oklahoma.parquet'
        )
        mock_client.upload_file.assert_any_call(
            os.path.join('/tmp/', 'Texas', 'breweries_Texas.parquet'),
            'datalake-case',
            'silver_layer/Texas/breweries_Texas.parquet'
        )

    @patch('boto3.client')
    def test_create_silver_layer_no_files(self, mock_boto_client):
        # Mock the boto3 client
        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client

        # Mock the response from MinIO for list_objects_v2 with no files
        mock_client.list_objects_v2.return_value = {'Contents': []}

        # Call the function and check if ValueError is raised
        with self.assertRaises(ValueError) as context:
            create_silver_layer(mock_client, bucket_name='datalake-case')
        
        self.assertEqual(str(context.exception), "No objects to concatenate")

    @patch('boto3.client')
    @patch('pandas.DataFrame.to_parquet')
    def test_create_silver_layer_missing_state_column(self, mock_to_parquet, mock_boto_client):
        # Mock the boto3 client
        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client
        
        # Mock the response from MinIO for list_objects_v2
        mock_client.list_objects_v2.return_value = {
            'Contents': [{'Key': 'bronze_layer/cleaned/file1.json'}]
        }

        # Mock the response for get_object (returning mock JSON data without 'state' column)
        mock_client.get_object.return_value = {
            'Body': MagicMock(read=MagicMock(return_value=json.dumps([
                {"id": "1", "name": "Brewery1"}  # Missing 'state' column
            ])))
        }

        # Call the function and check if ValueError is raised
        with self.assertRaises(ValueError) as context:
            create_silver_layer(mock_client, bucket_name='datalake-case')
        
        self.assertEqual(str(context.exception), "'state' column is missing in the file: bronze_layer/cleaned/file1.json")

class TestCreateGoldLayer(unittest.TestCase):

    @patch('boto3.client')
    def test_create_gold_layer_success(self, mock_boto_client):
        # Mock the S3 client
        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client

        # Mock the response of listing objects in the silver layer
        mock_client.list_objects_v2.return_value = {
            'Contents': [
                {'Key': 'silver_layer/file1.parquet'},
                {'Key': 'silver_layer/file2.parquet'},
            ]
        }

        # Mock the S3 object retrieval
        mock_file_obj_1 = MagicMock()
        mock_file_obj_1['Body'].read.return_value = self.mock_parquet_file()

        mock_file_obj_2 = MagicMock()
        mock_file_obj_2['Body'].read.return_value = self.mock_parquet_file()

        mock_client.get_object.side_effect = [mock_file_obj_1, mock_file_obj_2]

        # Mock os.makedirs to avoid actual directory creation
        with patch('os.makedirs') as mock_makedirs:
            mock_makedirs.return_value = None

            # Call the function under test
            create_gold_layer(mock_client)

            # Assert that the Parquet file was saved locally and uploaded
            mock_client.upload_file.assert_called_once_with(
                '/tmp/brewery_aggregated_by_type_and_location.parquet',
                'datalake-case',
                'golden_layer/brewery_aggregated_by_type_and_location.parquet'
            )

    @patch('boto3.client')
    def test_create_gold_layer_no_files_in_silver(self, mock_boto_client):
        # Mock the S3 client
        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client

        # Mock the response of listing objects in the silver layer with no contents
        mock_client.list_objects_v2.return_value = {}

        # Call the function under test and expect it to raise an exception
        with self.assertRaises(ValueError):
            create_gold_layer(mock_client)

    @patch('boto3.client')
    def test_create_gold_layer_missing_columns(self, mock_boto_client):
        # Mock the S3 client
        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client

        # Mock the response of listing objects in the silver layer
        mock_client.list_objects_v2.return_value = {
            'Contents': [
                {'Key': 'silver_layer/file1.parquet'}
            ]
        }

        # Mock the S3 object retrieval with a DataFrame that does not have the required columns
        mock_file_obj_1 = MagicMock()
        mock_file_obj_1['Body'].read.return_value = self.mock_parquet_file_missing_columns()

        mock_client.get_object.return_value = mock_file_obj_1

        # Call the function under test
        with patch('os.makedirs') as mock_makedirs:
            mock_makedirs.return_value = None

            # The function should skip processing and raise an exception due to missing columns
            with self.assertRaises(Exception):
                create_gold_layer(mock_client)

    def mock_parquet_file(self):
        # Creates a simple mock parquet file with valid brewery_type and state columns
        data = {
            'brewery_type': ['micro', 'brewpub', 'brewery'],
            'state': ['CA', 'TX', 'NY'],
            'name': ['Brewery1', 'Brewery2', 'Brewery3']
        }
        df = pd.DataFrame(data)
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        return buffer.read()

    def mock_parquet_file_missing_columns(self):
        # Creates a mock parquet file missing the 'brewery_type' and 'state' columns
        data = {'name': ['Brewery1', 'Brewery2', 'Brewery3']}
        df = pd.DataFrame(data)
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        return buffer.read()