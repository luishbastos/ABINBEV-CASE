import unittest
from unittest.mock import patch, MagicMock
from dags.etl.extract import fetch_breweries
import requests

class TestFetchBreweries(unittest.TestCase):

    @patch("requests.get")
    def test_fetch_breweries_success(self, mock_get):
        """
        Test successful fetch of breweries data.
        """
        # Mock response data
        mock_response_data = [
            {"id": "1", "name": "Brewery One", "city": "City One"},
            {"id": "2", "name": "Brewery Two", "city": "City Two"}
        ]

        # Configure mock response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = mock_response_data
        mock_get.return_value = mock_response

        # Call the function
        per_page = 2
        result = fetch_breweries(per_page)

        # Assertions
        mock_get.assert_called_once_with(f"https://api.openbrewerydb.org/v1/breweries?per_page={per_page}")
        self.assertEqual(result, mock_response_data)

    @patch("requests.get")
    def test_fetch_breweries_http_error(self, mock_get):
        """
        Test fetch_breweries when an HTTP error occurs (e.g., 404).
        """
        # Configure mock to raise HTTPError
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("404 Client Error")
        mock_get.return_value = mock_response

        # Call the function and expect an exception
        with self.assertRaises(requests.exceptions.HTTPError):
            fetch_breweries()

        # Assertions
        mock_get.assert_called_once_with("https://api.openbrewerydb.org/v1/breweries?per_page=200")

    @patch("requests.get")
    def test_fetch_breweries_request_exception(self, mock_get):
        """
        Test fetch_breweries when a generic RequestException occurs.
        """
        # Configure mock to raise RequestException
        mock_get.side_effect = requests.exceptions.RequestException("Connection error")

        # Call the function and expect an exception
        with self.assertRaises(requests.exceptions.RequestException):
            fetch_breweries()

        # Assertions
        mock_get.assert_called_once_with("https://api.openbrewerydb.org/v1/breweries?per_page=200")

    @patch("requests.get")
    def test_fetch_breweries_default_per_page(self, mock_get):
        """
        Test fetch_breweries with the default per_page value.
        """
        # Mock response data
        mock_response_data = [{"id": "1", "name": "Default Brewery"}]

        # Configure mock response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = mock_response_data
        mock_get.return_value = mock_response

        # Call the function with default per_page
        result = fetch_breweries()

        # Assertions
        mock_get.assert_called_once_with("https://api.openbrewerydb.org/v1/breweries?per_page=200")
        self.assertEqual(result, mock_response_data)
