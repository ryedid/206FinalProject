
import requests
# import my_key
from bs4 import BeautifulSoup
import json
import os

def get_city_bike_data():
    """
    Fetches data from the City Bike API.

    Parameters:
    - api_key (str): Your City Bike API key.

    Returns:
    - dict: Parsed JSON response from the API.
    """
    # Replace 'YOUR_API_KEY' with your actual API key
    base_url = "https://api.citybik.es/v2/"
    # endpoint = "networks"
    endpoint = "networks/velib"
    url = f"{base_url}{endpoint}"

    # Include your API key in the headers
    # headers = {"Authorization": f"Bearer {api_key}"}

    try:
        # Make a GET request to the API
        response = requests.get(url)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Parse the JSON response
            data = response.json()
            return data
        else:
            # Print an error message if the request was not successful
            print(f"Error: {response.status_code} - {response.text}")
            return None

    except requests.exceptions.RequestException as e:
        # Handle any exceptions that may occur during the request
        print(f"Error: {e}")
        return None

def load_json(data):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    filename = dir_path + '/' + "citybike.json"
    with open(filename, 'w') as json_file:
        json.dump(data, json_file, indent=2)
# Replace 'YOUR_API_KEY' with your actual City Bike API key
city_bike_data = get_city_bike_data()

# Print the retrieved data
if city_bike_data:
    print(city_bike_data)

load_json(city_bike_data)

