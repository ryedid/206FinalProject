from bs4 import BeautifulSoup
import requests
import os
import re
import sqlite3
def normalize_city(cur, conn):
    # Define the SQL command to update the city column
    sql_command = """
        UPDATE CityHealth
        SET city = 
            CASE 
                WHEN city LIKE 'San-Antonio%' THEN 'San Antonio, TX'
                WHEN city LIKE 'Des-Moines%' THEN 'Des Moines, IA'
                WHEN city LIKE 'Park-City%' THEN 'Park City, UT'
                WHEN city LIKE 'Long-Beach%' THEN 'Long Beach, CA'
                WHEN city LIKE 'Los-Angeles%' THEN 'Los Angeles, NV'
                WHEN city LIKE 'Fort-Lauderdale%' THEN 'Fort Lauderdale, FL'
                WHEN city LIKE 'El-Paso%' THEN 'El Paso, TX'
                WHEN city LIKE 'New-York%' THEN 'New York, NY'
                WHEN city LIKE 'Fort-Worth%' THEN 'Fort Worth, TX'
                WHEN city LIKE 'Las-Vegas%' THEN 'Las Vegas, NV'
                WHEN city LIKE 'Oklahoma-City%' THEN 'Oklahoma City, OK'
                WHEN city LIKE '%San-Francisco%' THEN 'San Francisco Bay Area, CA'
                WHEN city LIKE '%Salt-Lake-City-Utah%' THEN 'Salt Lake City'
                WHEN city LIKE 'Kailua' THEN 'Kailua-Kona'
                ELSE city
            END;
    """
    try:
        # Execute the SQL command
        cur.execute(sql_command)
        # Commit the changes to the database
        conn.commit()
        print("Normalization completed successfully!")
    except Exception as e:
        # Rollback changes if there's an error
        conn.rollback()
        print(f"Error during normalization: {str(e)}")

def normalize_state(cur, conn):
    # Define the SQL command to update the city column
    sql_command = """
        UPDATE CityHealth
        SET city = 
            CASE 
                WHEN city LIKE '%-Colorado' THEN REPLACE(city, '-Colorado', ', CO')  
                WHEN city LIKE 'Washington-District-of-Columbia' THEN 'Washington, DC'
                WHEN city LIKE '%-Texas' THEN REPLACE(city, '-Texas', ', TX')
                WHEN city LIKE '%-Tennessee' THEN REPLACE(city, '-Tennessee', ', TN')
                WHEN city LIKE '%-Oregon' THEN REPLACE(city, '-Oregon', ', OR')
                WHEN city LIKE '%-Florida' THEN REPLACE(city, '-Florida', ', FL')
                WHEN city LIKE '%-Wisconsin' THEN REPLACE(city, '-Wisconsin', ', WI')
                WHEN city LIKE '%-New-York' THEN REPLACE (city, '-New-York', ', NY')
                WHEN city LIKE '%-North-Carolina' THEN REPLACE(city, '-North-Carolina', ', NC')
                WHEN city LIKE '%-Ohio' THEN REPLACE(city, '-Ohio', ', OH')
                WHEN city LIKE '%-Illinois' THEN REPLACE(city, '-Illinois', ', IL')
                WHEN city LIKE '%-Nebraska' THEN REPLACE(city, '-Nebraska', ', NE')
                WHEN city LIKE '%-Massachusetts' THEN REPLACE(city, '-Massachusetts', ', MA')
                WHEN city LIKE '%-Pennsylvania' THEN REPLACE(city, '-Pennsylvania', ', PA')
                WHEN city LIKE '%-Indiana' THEN REPLACE(city, '-Indiana', ', IN')
                WHEN city LIKE '%-California' THEN REPLACE(city, '-California', ', CA')
                WHEN city LIKE '%-Minnesota' THEN REPLACE(city, '-Minnesota', ', MN')
                WHEN city LIKE '%-Virginia' THEN REPLACE(city, '-Virginia', ', VA')
                WHEN city LIKE '%-Georgia' THEN REPLACE(city, '-Georgia', ', GA')
                WHEN city LIKE '%-Iowa' THEN REPLACE(city, '-Iowa', ', IA')
                WHEN city LIKE '%-South-Carolina' THEN REPLACE(city, '-South-Carolina', ', SC')
                WHEN city LIKE '%-Nevada' THEN REPLACE(city, '-Nevada', ', NV')
                WHEN city LIKE '%-Oklahoma' THEN REPLACE(city, '-Oklahoma', ', OK')
                WHEN city LIKE '%-Arizona' THEN REPLACE(city, '-Arizona', ', AZ')
                WHEN city LIKE '%-Hawaii' THEN REPLACE(city, '-Hawaii', '')
                ELSE city
            END;
    """
    try:
        # Execute the SQL command
        cur.execute(sql_command)
        # Commit the changes to the database
        conn.commit()
        print("Normalization completed successfully!")
    except Exception as e:
        # Rollback changes if there's an error
        conn.rollback()
        print(f"Error during normalization: {str(e)}")

conn = sqlite3.connect("proj_base")
# Create a cursor object to execute SQL queries
cur = conn.cursor()
