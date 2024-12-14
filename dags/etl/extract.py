import json
import requests

def fetch_breweries(per_page=200):
    """
    Fetch breweries data from the Open Brewery API.

    Args:
        per_page (int): Number of breweries per page. Default is 200.

    Returns:
        list: A list of brewery data.
    """
    url = f"https://api.openbrewerydb.org/v1/breweries?per_page={per_page}"

    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx and 5xx)
        breweries = response.json()
        return breweries
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        raise