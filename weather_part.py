import requests
import json
import os
import sqlite3

def get_coordinates_from_database(database_path, cur, conn):
    # Connect to the SQLite database
    conn = sqlite3.connect(database_path)

    # Create a cursor object to execute SQL queries
    cur = conn.cursor()

    # Execute a query to retrieve all coordinates
    cur.execute("SELECT city, longitude, latitude FROM BikeCities")  # Replace 'your_table_name' with the actual table name

    # Fetch all rows from the result set
    rows = cur.fetchall()

    # Return the list of coordinates
    return rows

def get_links(coordinates):
    links = []
    for coords in coordinates:
        
        base_url = "https://api.weather.gov/"
        # endpoint = "networks"
        endpoint = "points"
        if coords[1] and coords[2]:
            url = f"{base_url}{endpoint}/{coords[2]},{coords[1]}"
        # print(url)


        try:
            # Make a GET request to the API
            response = requests.get(url)

            # Check if the request was successful (status code 200)
            if response.status_code == 200:
                # Parse the JSON response
                data = response.json()
                props = data.get('properties', {})
                link = props.get('forecastHourly', '')
                links.append(link)
            else:
                # Print an error message if the request was not successful
                print(f"Error: {response.status_code} - {response.text}")
                return None

        except requests.exceptions.RequestException as e:
            # Handle any exceptions that may occur during the request
            print(f"Error: {e}")
            return None
        
    return links

def get_new_data(url):


    base_url = url

    try:
        # Make a GET request to the API
        response = requests.get(base_url)

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
    filename = dir_path + '/' + "weather.json"
    with open(filename, 'w') as json_file:
        json.dump(data, json_file, indent=2)


def make_SQL(cur, conn, links):
    cur.execute(
        "CREATE TABLE IF NOT EXISTS WeatherCities (city TEXT, latitude TEXT, longitude TEXT, date TEXT, hour TEXT, temp INTEGER, precip INTEGER, humidity INTEGER, wind TEXT, short TEXT)"
    )

    city_and_coords = get_coordinates_from_database("proj_base", cur, conn)

    # city = 1
    # latitude = 2
    # longitude = 3

    city_index = -1
    num_inserted = 0

    #print(links)
    
    for link in links:
        #print(links)
        hourly_data = get_new_data(link)
        load_json(hourly_data)
        #print(data)
        # response = requests.get(hourly_data)
        # data = response.json()
        #print(data)
        if hourly_data:
            real_data = hourly_data.get('properties', {})
            real_real_data = real_data.get('periods', [])

        city_index += 1

        for item in real_real_data:
            # city+=1
            # latitude+=1
            # longitude+=1
            city = city_and_coords[city_index][0]
            latitude = city_and_coords[city_index][2]
            longitude = city_and_coords[city_index][1]
            date = item.get('startTime', '')
            date = date[0:10]
            hour = item.get('startTime', '')
            hour = hour[11:16]
            #print(hour)
            temp = item["temperature"]
            prob = item.get('probabilityOfPrecipitation', {})
            precip = prob.get('value', '')
            rel = item.get('relativeHumidity', {})
            humidity = rel.get('value', '')
            wind = item.get('windSpeed', '')
            short = item.get('shortForecast', '')
            if (hour == "08:00" or hour == "14:00" or hour == "20:00") and num_inserted < 25:
                # Check if the item is already in the database before attempting to insert
                cur.execute(
                    "SELECT COUNT(*) FROM WeatherCities WHERE city=? AND latitude=? AND longitude=? AND date=? AND hour=? AND temp=? AND precip=? AND humidity=? AND wind=? AND short=?",
                    (city, latitude, longitude, date, hour, temp, precip, humidity, wind, short)
                )
                if cur.fetchone()[0] == 0:
                    # Item is not in the database, insert it
                    cur.execute(
                        "INSERT INTO WeatherCities (city,latitude,longitude,date,hour,temp,precip,humidity,wind,short) VALUES (?,?,?,?,?,?,?,?,?,?)",
                        (city, latitude, longitude, date, hour, temp, precip, humidity, wind, short)
                    )
                    if cur.rowcount > 0:
                        num_inserted += 1
            elif num_inserted >= 25:
                conn.commit() 
                break
                #print(city, latitude, longitude, date, hour, temp, precip, humidity, wind, short)

    conn.commit()

database_path = "proj_base"

conn = sqlite3.connect(database_path)
# Create a cursor object to execute SQL queries
cur = conn.cursor()
# Replace with the actual path to your SQLite database
coordinates = get_coordinates_from_database(database_path, cur, conn)
# print(coordinates)
links = get_links(coordinates)
for i in range(32): #COMMENT THIS OUT!!!!!
    make_SQL(cur, conn, links)
