from bs4 import BeautifulSoup
import requests
import os
import re
import sqlite3
# def normalize_city(cur, conn):
#     # Define the SQL command to update the city column
#     sql_command = """
#         UPDATE CityHealth
#         SET city = 
#             CASE 
#                 WHEN city LIKE 'San-Antonio%' THEN 'San Antonio, TX'
#                 WHEN city LIKE 'Des-Moines%' THEN 'Des Moines, IA'
#                 WHEN city LIKE 'Park-City%' THEN 'Park City, UT'
#                 WHEN city LIKE 'Long-Beach%' THEN 'Long Beach, CA'
#                 WHEN city LIKE 'Los-Angeles%' THEN 'Los Angeles, NV'
#                 WHEN city LIKE 'Fort-Lauderdale%' THEN 'Fort Lauderdale, FL'
#                 WHEN city LIKE 'El-Paso%' THEN 'El Paso, TX'
#                 WHEN city LIKE 'New-York%' THEN 'New York, NY'
#                 WHEN city LIKE 'Fort-Worth%' THEN 'Fort Worth, TX'
#                 WHEN city LIKE 'Las-Vegas%' THEN 'Las Vegas, NV'
#                 WHEN city LIKE 'Oklahoma-City%' THEN 'Oklahoma City, OK'
#                 WHEN city LIKE '%San-Francisco%' THEN 'San Francisco Bay Area, CA'
#                 WHEN city LIKE '%Salt-Lake-City-Utah%' THEN 'Salt Lake City'
#                 WHEN city LIKE 'Kailua' THEN 'Kailua-Kona'
#                 ELSE city
#             END;
#     """
#     try:
#         # Execute the SQL command
#         cur.execute(sql_command)
#         # Commit the changes to the database
#         conn.commit()
#         print("Normalization completed successfully!")
#     except Exception as e:
#         # Rollback changes if there's an error
#         conn.rollback()
#         print(f"Error during normalization: {str(e)}")

# def normalize_state(cur, conn):
#     # Define the SQL command to update the city column
#     sql_command = """
#         UPDATE CityHealth
#         SET city = 
#             CASE 
#                 WHEN city LIKE '%-Colorado' THEN REPLACE(city, '-Colorado', ', CO')  
#                 WHEN city LIKE 'Washington-District-of-Columbia' THEN 'Washington, DC'
#                 WHEN city LIKE '%-Texas' THEN REPLACE(city, '-Texas', ', TX')
#                 WHEN city LIKE '%-Tennessee' THEN REPLACE(city, '-Tennessee', ', TN')
#                 WHEN city LIKE '%-Oregon' THEN REPLACE(city, '-Oregon', ', OR')
#                 WHEN city LIKE '%-Florida' THEN REPLACE(city, '-Florida', ', FL')
#                 WHEN city LIKE '%-Wisconsin' THEN REPLACE(city, '-Wisconsin', ', WI')
#                 WHEN city LIKE '%-New-York' THEN REPLACE (city, '-New-York', ', NY')
#                 WHEN city LIKE '%-North-Carolina' THEN REPLACE(city, '-North-Carolina', ', NC')
#                 WHEN city LIKE '%-Ohio' THEN REPLACE(city, '-Ohio', ', OH')
#                 WHEN city LIKE '%-Illinois' THEN REPLACE(city, '-Illinois', ', IL')
#                 WHEN city LIKE '%-Nebraska' THEN REPLACE(city, '-Nebraska', ', NE')
#                 WHEN city LIKE '%-Massachusetts' THEN REPLACE(city, '-Massachusetts', ', MA')
#                 WHEN city LIKE '%-Pennsylvania' THEN REPLACE(city, '-Pennsylvania', ', PA')
#                 WHEN city LIKE '%-Indiana' THEN REPLACE(city, '-Indiana', ', IN')
#                 WHEN city LIKE '%-California' THEN REPLACE(city, '-California', ', CA')
#                 WHEN city LIKE '%-Minnesota' THEN REPLACE(city, '-Minnesota', ', MN')
#                 WHEN city LIKE '%-Virginia' THEN REPLACE(city, '-Virginia', ', VA')
#                 WHEN city LIKE '%-Georgia' THEN REPLACE(city, '-Georgia', ', GA')
#                 WHEN city LIKE '%-Iowa' THEN REPLACE(city, '-Iowa', ', IA')
#                 WHEN city LIKE '%-South-Carolina' THEN REPLACE(city, '-South-Carolina', ', SC')
#                 WHEN city LIKE '%-Nevada' THEN REPLACE(city, '-Nevada', ', NV')
#                 WHEN city LIKE '%-Oklahoma' THEN REPLACE(city, '-Oklahoma', ', OK')
#                 WHEN city LIKE '%-Arizona' THEN REPLACE(city, '-Arizona', ', AZ')
#                 WHEN city LIKE '%-Hawaii' THEN REPLACE(city, '-Hawaii', '')
#                 WHEN city LIKE 'Kailua' THEN 'Kailua-Kona'
#                 ELSE city
#             END;
#     """
#     try:
#         # Execute the SQL command
#         cur.execute(sql_command)
#         # Commit the changes to the database
#         conn.commit()
#         print("Normalization completed successfully!")
#     except Exception as e:
#         # Rollback changes if there's an error
#         conn.rollback()
#         print(f"Error during normalization: {str(e)}")


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


# def update_bike_cities(cur, conn):
#     cur.execute('''ALTER TABLE BikeCities ADD COLUMN city_id INTEGER''')
#     cur.execute('''SELECT City, ID FROM CityIndex''')
#     city_index = dict(cur.fetchall())

#     cur.execute('''SELECT city FROM BikeCities''')
#     cities = cur.fetchall()

#     for city in cities:
#         city_name = city[0]
#         city_id = city_index.get(city_name)

#         if city_id is not None:
#             cur.execute('''UPDATE BikeCities SET city_id = ? WHERE city = ?''', (city_id, city_name))
#     cur.execute('''ALTER TABLE BikeCities DROP COLUMN city''')
#     conn.commit()

# def update_city_health(cur, conn):
#     cur.execute('''ALTER TABLE CityHealth ADD COLUMN city_id INTEGER''')
#     cur.execute('''SELECT City, ID FROM CityIndex''')
#     city_index = dict(cur.fetchall())

#     cur.execute('''SELECT city FROM CityHealth''')
#     cities = cur.fetchall()

#     for city in cities:
#         city_name = city[0]
#         city_id = city_index.get(city_name)

#         if city_id is not None:
#             cur.execute('''UPDATE CityHealth SET city_id = ? WHERE city = ?''', (city_id, city_name))
#     cur.execute('''ALTER TABLE CityHealth DROP COLUMN city''')
#     conn.commit()

# def update_city_weather(cur, conn):
#     cur.execute('''ALTER TABLE WeatherCities ADD COLUMN city_id INTEGER''')
#     cur.execute('''SELECT City, ID FROM CityIndex''')
#     city_index = dict(cur.fetchall())

#     cur.execute('''SELECT city FROM WeatherCities''')
#     cities = cur.fetchall()

#     for city in cities:
#         city_name = city[0]
#         city_id = city_index.get(city_name)

#         if city_id is not None:
#             cur.execute('''UPDATE WeatherCities SET city_id = ? WHERE city = ?''', (city_id, city_name))
#     cur.execute('''ALTER TABLE WeatherCities DROP COLUMN city''')

#     conn.commit()

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
        # Execute the SQL command
        cur.execute(sql_command)

        # Fetch all rows from the result set
        rows = cur.fetchall()

        # Return the results
        return rows
    except Exception as e:
        # Rollback changes if there's an error
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
        # Execute the SQL command
        cur.execute(sql_command)

        # Fetch all rows from the result set
        rows = cur.fetchall()

        # Return the results
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
        # Execute the SQL command
        cur.execute(sql_command)

        # Fetch all rows from the result set
        rows = cur.fetchall()

        # Return the results
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
            # Save the results to a text file
            with open(output_file_path, 'w') as output_file:
                for row in results:
                    output_file.write(f"{row[0]}\t{row[1]}\t{row[2]}\n")
        except Exception as e:
            print(f"Error while writing to file: {str(e)}")






conn = sqlite3.connect("proj_base")

# Create a cursor object to execute SQL queries
cur = conn.cursor()

# normalize_city(cur, conn)
# normalize_state(cur, conn)
# normalize_city(cur, conn)
create_city_index(cur, conn)
# update_bike_cities(cur,conn)
# update_city_health(cur,conn)
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
