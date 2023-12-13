from bs4 import BeautifulSoup
import requests
import os
import re
import sqlite3

def create_city_index(cur, conn):
    unique_cities = [
        "Aspen, CO", "Austin, TX", "Chattanooga, TN",
        "Portland, OR", "Boulder, CO", "Fort Lauderdale, FL",
        "Milwaukee, WI", "Buffalo, NY", "Washington, DC",
        "Charlotte, NC", "Cincinnati, OH", "New York, NY",
        "Columbus, OH", "Denver, CO", "Chicago, IL",
        "El Paso, TX", "Fort Worth, TX", "Salt Lake City",
        "Omaha, NE", "", "Boston, MA", "Philadelphia, PA",
        "Indianapolis, IN", "Madison, WI", "Los Angeles, CA",
        "Houston, TX", "Minneapolis, MN", "San Antonio, TX",
        "Long Beach, CA", "Atlanta, GA", "Des Moines, IA",
        "Greenville, SC", "San Francisco Bay Area, CA",
        "Las Vegas, NV", "Oklahoma, OK", "Miami, FL",
        "Tucson, AZ", "Park City, UT", "Richmond, VA",
        "Honolulu", "Kailua-Kona"
    ]
    cur.execute('''CREATE TABLE IF NOT EXISTS CityIndex (
                    ID INTEGER PRIMARY KEY AUTOINCREMENT,
                    City TEXT
                )''')

    for city in unique_cities:
        cur.execute('''INSERT INTO CityIndex (City) VALUES (?)''', (city,))

    cur.execute('''UPDATE CityIndex SET city = NULLIF(city, '')''')
    cur.execute('''DELETE FROM CityIndex WHERE city IS NULL''')

    conn.commit()

def create_description_index(cur, conn):
    unique_descriptions = [
        "Mostly Cloudy", "Chance Snow Showers", "Partly Sunny",
        "Mostly Sunny", "Partly Cloudy", "Sunny", "Mostly Clear",
        "Chance Rain Showers", "Showers And Thunderstorms Likely",
        "Showers And Thunderstorms", "Chance Showers And Thunderstorms",
        "Slight Chance Rain Showers", "Slight Chance Light Rain",
        "Chance Light Rain", "Light Rain Likely", "Cloudy",
        "Slight Chance Light Snow", "Slight Chance Snow Showers",
        "Slight Chance Rain And Snow Showers", "Clear",
        "Chance Light Snow", "Rain Likely", "Chance Rain",
        "Areas Of Fog", "Patchy Fog"
    ]

    cur.execute('''CREATE TABLE IF NOT EXISTS DescriptionIndex (
                    ID INTEGER PRIMARY KEY AUTOINCREMENT,
                    Description TEXT
                )''')

    for desc in unique_descriptions:
        cur.execute('''INSERT INTO DescriptionIndex (Description) VALUES (?)''', (desc,))

    cur.execute('''UPDATE DescriptionIndex SET Description = NULLIF(Description, '')''')
    cur.execute('''DELETE FROM DescriptionIndex WHERE Description IS NULL''')

    conn.commit()

def update_weather(cur, conn):
    cur.execute('''ALTER TABLE WeatherCities ADD COLUMN short_id INTEGER''')
    cur.execute('''SELECT Description, ID FROM DescriptionIndex''')
    desc_index = dict(cur.fetchall())

    cur.execute('''SELECT short FROM WeatherCities''')
    descriptions = cur.fetchall()

    for description in descriptions:
        desc = description[0]
        desc_id = desc_index.get(desc)

        if desc_id is not None:
            cur.execute('''UPDATE WeatherCities SET short_id = ? WHERE short = ?''', (desc_id, desc))
    cur.execute('''ALTER TABLE BikeCities DROP COLUMN short''')
    conn.commit()

#Calculations
def calculate_bikes_bad(cur, conn):
    sql_command = """
        SELECT AVG(b.empty_slots), AVG(h.Feeling_Bad_About_Self), cid.City FROM BikeCities b 
        JOIN CityHealth h ON b.city = h.city
        JOIN CityIndex cid ON b.city = cid.ID
        GROUP BY b.city
    """
    try:
        cur.execute(sql_command)
        rows = cur.fetchall()
        return rows
    except Exception as e:
        conn.rollback()
        print(f"Error during calculation: {str(e)}")
        return None
    
def calculate_sunny(cur, conn):
    sql_command = """
        SELECT b.empty_slots, cid.City FROM BikeCities b 
        JOIN CityIndex cid ON b.city = cid.ID
        JOIN WeatherCities w ON b.city = w.city
        JOIN DescriptionIndex d ON d.ID = w.short_id
        WHERE d.Description = "Sunny"
        GROUP BY b.city
    """
    try:
        cur.execute(sql_command)
        rows = cur.fetchall()
        return rows
    except Exception as e:
        conn.rollback()
        print(f"Error during calculation: {str(e)}")
        return None
      

def calculate_bikes_genhealth(cur, conn):
    sql_command = """
        SELECT b.empty_slots, h.Gen_Health, cid.City FROM BikeCities b 
        JOIN CityIndex cid ON b.city = cid.ID        
        JOIN CityHealth h ON b.city = h.city
        GROUP BY b.city
    """
    try:
        cur.execute(sql_command)
        rows = cur.fetchall()
        return rows
    except Exception as e:
        conn.rollback()
        print(f"Error during calculation: {str(e)}")
        return None
    

def write_results_to_file(results, output_file_path="output.txt"):
    if results is not None:
        try:
            with open(output_file_path, 'w') as output_file:
                for row in results:
                    if len(row) >= 2:
                        output_file.write(f"{row[0]}\t{row[1]}\n")
                    else:
                        print("Invalid row format:", row)

        except Exception as e:
            print(f"Error while writing to file: {str(e)}")

def write_results_to_file_health(results, output_file_path="output.txt"):
    if results is not None:
        try:
            with open(output_file_path, 'w') as output_file:
                for row in results:
                    output_file.write(f"{row[0]}\t{row[1]}\t{row[2]}\n")
        except Exception as e:
            print(f"Error while writing to file: {str(e)}")






conn = sqlite3.connect("proj_base")
cur = conn.cursor()


create_city_index(cur, conn)
create_description_index(cur, conn)
update_weather(cur, conn)

#how many empty slots are sunny?
results = calculate_sunny(cur, conn)

# Write results to a file
write_results_to_file(results, "sunny.txt")


#how many empty slots are there in cities with the highest general health percentage?
results = calculate_bikes_genhealth(cur, conn)

# Write results to a file
write_results_to_file_health(results, "health.txt")

#how many empty slots are there in cities where people feel bad about themselves?
results = calculate_bikes_bad(cur, conn)

# Write results to a file
write_results_to_file(results, "bad.txt")
