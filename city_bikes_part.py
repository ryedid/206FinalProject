
import requests
# import my_key
from bs4 import BeautifulSoup
import json
import os
import sqlite3

def get_city_bike_data():
    """
    Fetches data from the City Bike API.

    Parameters:
    - api_key (str): Your City Bike API key.

    Returns:
    - dict: Parsed JSON response from the API.
    """
    
    base_url = "https://api.citybik.es/v2/"
    # endpoint = "networks"
    endpoint = "networks"
    url = f"{base_url}{endpoint}"

    
    # headers = {"Authorization": f"Bearer {api_key}"}

    try:
        
        response = requests.get(url)

        if response.status_code == 200:
            data = response.json()
            return data
        else:
            # Print an error message if the request was not successful
            print(f"Error: {response.status_code} - {response.text}")
            return None

    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")
        return None

def load_json(data):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    filename = dir_path + '/' + "citybike.json"
    with open(filename, 'w') as json_file:
        json.dump(data, json_file, indent=2)




def set_up_database():
    """
    Sets up a SQLite database connection and cursor.

    Parameters
    -----------------------
    db_name: str
        The name of the SQLite database.

    Returns
    -----------------------
    Tuple (Cursor, Connection):
        A tuple containing the database cursor and connection objects.
    """
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path + "/" + "proj_base")
    cur = conn.cursor()
    return cur, conn
'''
def make_SQL(cur, conn): 
    cur.execute(
        "CREATE TABLE IF NOT EXISTS BikeCities (city TEXT, latitude TEXT, longitude TEXT)"
    )

    dir_path = os.path.dirname(os.path.realpath(__file__))
    filename = dir_path + '/' + "citybike.json"
    with open(filename, 'r') as json_file:
        data = json.load(json_file)
    real_data = data['networks']
    for item in real_data:
        if item['location']['country'] == "US":
            cur.execute(
            "INSERT OR IGNORE INTO BikeCities (city,latitude,longitude) VALUES (?,?,?)", (item['location']['city'], item['location']['latitude'],item['location']['longitude'])
        )
    conn.commit()
'''

def get_list_of_ids():
    ids = []

    dir_path = os.path.dirname(os.path.realpath(__file__))
    filename = dir_path + '/' + "citybike.json"
    with open(filename, 'r') as json_file:
        data = json.load(json_file)
        
    real_data = data.get('networks', [])  # Use .get to handle missing 'networks' key
    for item in real_data:
        location = item.get('location', {})
        country = location.get('country', '')
        if country == "US":
            ids.append(item.get('id', ''))

    return ids

def get_new_data(url):

  
    base_url = url

   
    # headers = {"Authorization": f"Bearer {api_key}"}

    try:
        response = requests.get(base_url)

        if response.status_code == 200:
            data = response.json()
            return data
        else:
            print(f"Error: {response.status_code} - {response.text}")
            return None

    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")
        return None

def make_SQL(cur, conn):
    cur.execute(
        "CREATE TABLE IF NOT EXISTS BikeCities (city INTEGER, latitude TEXT, longitude TEXT, free_bikes INTEGER, empty_slots INTEGER)"
    )

    ids = get_list_of_ids()
    num_inserted = 0
    city_num = 1
    for id in ids:
        base_url = "https://api.citybik.es/v2/"
        # endpoint = "networks"
        endpoint = "networks"
        url = f"{base_url}{endpoint}/{id}"
        curr_data = get_new_data(url)
        load_json(curr_data)
        real_data = curr_data.get('network', {})
        location = real_data.get('location', {})
        city = city_num
        latitude = location.get('latitude', '')
        longitude = location.get('longitude', '')
        stations = real_data.get('stations', [])
        amt_free_bikes = 0
        amt_empty_slots = 0
        for item in stations:
            free_bikes = item["free_bikes"]
            amt_free_bikes += free_bikes
            empty_slots = item["empty_slots"]
            amt_empty_slots += empty_slots

        if num_inserted < 25:
            cur.execute(
                "SELECT COUNT(*) FROM BikeCities WHERE city=? AND latitude=? AND longitude=? AND free_bikes=? AND empty_slots=?",
                (city, latitude, longitude, amt_free_bikes, amt_empty_slots)
            )
            if cur.fetchone()[0] == 0:
                # Item is not in the database, insert it
                cur.execute(
                    "INSERT INTO BikeCities (city,latitude,longitude,free_bikes,empty_slots) VALUES (?,?,?,?,?)",
                    (city, latitude, longitude, amt_free_bikes, amt_empty_slots)
                )
                city_num += 1
                if cur.rowcount > 0:
                    num_inserted += 1
            else:
                city_num+=1
        elif num_inserted >= 25:
            conn.commit()
            break
    conn.commit()

    

    # city_list = []


city_bike_data = get_city_bike_data()

# Print the retrieved data
#if city_bike_data:
    #print(city_bike_data)

load_json(city_bike_data)
cur, conn = set_up_database()
make_SQL(cur, conn)
