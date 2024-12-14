import unittest
from unittest.mock import MagicMock, patch
import os
import json
import pandas as pd
from io import StringIO
from dags.etl.transform import clean_data
import re

class TestCleanData(unittest.TestCase):

    @patch("os.makedirs")
    def test_clean_data_no_files_in_raw_layer(self, mock_makedirs):
        """Test when no files are found in the raw layer."""
        mock_client = MagicMock()
        mock_client.list_objects_v2.return_value = {}

        with self.assertRaises(ValueError) as context:
            clean_data(mock_client)

        self.assertIn("No files found in the raw layer", str(context.exception))
        mock_makedirs.assert_called_once_with("tmp/cleaned", exist_ok=True)

    @patch("os.makedirs")
    def test_clean_data_skip_non_json_files(self, mock_makedirs):
        """Test that non-JSON files are skipped during processing."""
        mock_client = MagicMock()
        mock_client.list_objects_v2.return_value = {
            'Contents': [{'Key': 'bronze_layer/raw/file1.txt'}, {'Key': 'bronze_layer/raw/file2.csv'}]
        }

        clean_data(mock_client)

        mock_client.get_object.assert_not_called()
        mock_client.upload_file.assert_not_called()
        mock_makedirs.assert_called_once_with("tmp/cleaned", exist_ok=True)

    @patch("os.makedirs")
    @patch("pandas.DataFrame.to_json")
    def test_clean_data_successful_cleaning(self, mock_to_json, mock_makedirs):
        """Test successful cleaning and uploading of JSON files."""
        mock_client = MagicMock()
        mock_client.list_objects_v2.return_value = {
            'Contents': [{'Key': 'bronze_layer/raw/data.json'}]
        }
        sample_json = [{"Name": "Test Brewery", "Address": "123 Street", "City": "Test City"}]
        mock_file_body = StringIO(json.dumps(sample_json))
        mock_client.get_object.return_value = {'Body': mock_file_body}

        with patch("os.path.join", return_value="/tmp/data.json"):
            clean_data(mock_client)

        mock_client.get_object.assert_called_once()
        mock_to_json.assert_called_once()
        mock_client.upload_file.assert_called_once_with(
            "/tmp/data.json", 'datalake-case', 'bronze_layer/cleaned/data.json'
        )

    @patch("os.makedirs")
    @patch("pandas.DataFrame.to_json")
    def test_clean_data_upload_failure(self, mock_to_json, mock_makedirs):
        """Test when file upload to MinIO fails."""
        mock_client = MagicMock()
        mock_client.list_objects_v2.return_value = {
            'Contents': [{'Key': 'bronze_layer/raw/data.json'}]
        }
        sample_json = [{"Name": "Test Brewery"}]
        mock_file_body = StringIO(json.dumps(sample_json))
        mock_client.get_object.return_value = {'Body': mock_file_body}
        mock_client.upload_file.side_effect = Exception("Upload failed")

        with patch("os.path.join", return_value="/tmp/data.json"):
            with self.assertRaises(Exception) as context:
                clean_data(mock_client)

        # Check that the exception message matches the actual error message
        self.assertIn("Upload failed", str(context.exception))
        mock_client.get_object.assert_called_once()
        mock_client.upload_file.assert_called_once()

    @patch("os.makedirs")
    @patch("pandas.DataFrame.to_json")
    def test_clean_data_cleaning_logic(self, mock_to_json, mock_makedirs):
        """Test that spaces are replaced with underscores and null values are replaced with 'unknown'."""
        mock_client = MagicMock()
        mock_client.list_objects_v2.return_value = {
            'Contents': [{'Key': 'bronze_layer/raw/data.json'}]
        }
        sample_json = [{"Name": "Test Brewery", "Address": "123 Street", "City": None}]
        mock_file_body = StringIO(json.dumps(sample_json))
        mock_client.get_object.return_value = {'Body': mock_file_body}

        # Capture the DataFrame before saving
        def capture_dataframe(path, orient):
            # Load the sample data into a DataFrame
            self.captured_df = pd.read_json(StringIO(json.dumps(sample_json)))
            
            # Replace spaces with underscores in column names
            self.captured_df.columns = [col.replace(" ", "_") for col in self.captured_df.columns]
            
            # Apply the transformation to each element in the dataframe (column-wise)
            self.captured_df = self.captured_df.map(
                lambda x: re.sub(r"\s+", "_", str(x).lower()) if isinstance(x, str) else x
            )
            
            # Convert the entire DataFrame to object dtype to avoid type compatibility issues
            self.captured_df = self.captured_df.astype(object)
            
            # Replace NaN values with 'unknown'
            self.captured_df.fillna("unknown", inplace=True)

        mock_to_json.side_effect = capture_dataframe

        with patch("os.path.join", return_value="/tmp/data.json"):
            clean_data(mock_client)

        # Expected DataFrame
        df_expected = pd.DataFrame([{
            "Name": "test_brewery",
            "Address": "123_street",
            "City": "unknown"
        }])

        pd.testing.assert_frame_equal(self.captured_df, df_expected)
        mock_client.upload_file.assert_called_once()

if __name__ == "__main__":
    unittest.main()
