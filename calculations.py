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
def calculate_bikes_bad(cur, conn):
    sql_command = """
        SELECT AVG(b.empty_slots), AVG(h.Feeling_Bad_About_Self), b.city FROM BikeCities b 
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
        # Rollback changes if there's an error
        conn.rollback()
        print(f"Error during calculation: {str(e)}")
        return None
    
def calculate_sunny(cur, conn):
    sql_command = """
        SELECT COUNT(b.empty_slots), b.city FROM BikeCities b 
        JOIN WeatherCities w ON b.city = w.city
        WHERE w.short = "Sunny"
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
      

def calculate_bikes_genhealth(cur, conn):
    sql_command = """
        SELECT b.empty_slots, h.Gen_Health, b.city FROM BikeCities b 
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
        # Rollback changes if there's an error
        conn.rollback()
        print(f"Error during calculation: {str(e)}")
        return None
    

def write_results_to_file(results, output_file_path="output.txt"):
    if results is not None:
        try:
            # Save the results to a text file
            # with open(output_file_path, 'w') as output_file:
            #     for row in results:
            #         output_file.write(f"{row[0]}\t{row[1]}\t{row[2]}\n")
            # Save the results to a text file
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

normalize_city(cur, conn)
normalize_state(cur, conn)
normalize_city(cur, conn)

#how many timeslots are sunny?
results = calculate_sunny(cur, conn)

# Write results to a file
write_results_to_file(results, "sunny.txt")


#how many empty slots are there in cities with the highest general health percentage?
results = calculate_bikes_genhealth(cur, conn)

# Write results to a file
write_results_to_file_health(results, "health.txt")

#how amny empty slots are there in cities where people feel bad about themselves?
results = calculate_bikes_bad(cur, conn)

# Write results to a file
write_results_to_file(results, "bad.txt")
