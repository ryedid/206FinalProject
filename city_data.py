from bs4 import BeautifulSoup
import requests
import os
import re
import sqlite3

list_of_us_cities = ["Aspen-Colorado", "Austin-Texas", "Chattanooga-Tennessee", "Portland-Oregon", "Boulder-Colorado", "Fort-Lauderdale-Florida", "Milwaukee-Wisconsin", "Buffalo-New-York", "Washington-District-of-Columbia", "Charlotte-North-Carolina", 
                     "Cincinnati-Ohio", "New-York-New-York", "Columbus-Ohio", "Denver-Colorado", "Chicago-Illinois", "El-Paso-Texas", "Fort-Worth-Texas", "Salt-Lake-City-Utah", "Omaha-Nebraska", "Houston-Texas", "Boston-Massachusetts", "Philadelphia-Pennsylvania", 
                     "Indianapolis-Indiana", "Madison-Wisconsin", "Los-Angeles-California", "Minneapolis-Minnesota", "San-Antonio-Texas", "Long-Beach-California", "Atlanta-Georgia", "Des-Moines-Iowa", "Greenville-South-Carolina", "San-Francisco-California", 
                     "Las-Vegas-Nevada", "Oklahoma-City-Oklahoma", "Miami-Florida", "Tucson-Arizona", "Park-City-Utah", "Richmond-Virginia", "Honolulu-Hawaii", "Kailua-Hawaii"]

def scrape_city(city):
    # html file for each specific city
   # city = input("Enter a city in the following format 'Aspen-Colorado': ")
   # returns a tuple: (General health condition, overweight people, feeling badly about themselves)

   base_url = "https://www.city-data.com/city/"
   new_url = base_url + city + ".html"

        
   try:
        response = requests.get(new_url)
        response.raise_for_status() 
        
   except requests.exceptions.RequestException as e:
        print(f"Error: {e}")
        return None
   
    
     
   #with open(response.text, 'r') as file:
        #html_content = file.read()
    
   feeling_bad_stat = ""
   overweight_stat = ""
   gen_health_stat = ""

   soup = BeautifulSoup(response.text, 'html.parser')
   healthTag = soup.find('section', id='health-nutrition')
   if healthTag == None:
       return (city,"N/A","N/A","N/A")
   # all health graphs
   healthGraphTags = healthTag.find_all('div', class_='hgraph')
   for tag in healthGraphTags:
        if tag.find('b').find('b').text == 'People feeling badly about themselves':
            tagstemp = tag.find_all('td')
            feeling_bad_stat = tagstemp[1].text
        elif tag.find('b').find('b').text == 'General health condition':
            tagstemp = tag.find_all('td')
            gen_health_stat = tagstemp[1].text
        elif tag.find('b').find('b').text == 'Overweight people':
            tagstemp = tag.find_all('td')
            overweight_stat = tagstemp[1].text
    
   #print(overweight_stat)
   return (city,gen_health_stat,overweight_stat,feeling_bad_stat)


def make_tup_list(city_list):
    return_lst = []
    for city in city_list:
        tup = scrape_city(city)
        return_lst.append(tup)

    return return_lst

def make_SQL(data_list, cur, conn):
    cur.execute(
        "CREATE TABLE IF NOT EXISTS CityHealth (city TEXT UNIQUE, Gen_Health TEXT, overweight TEXT, Feeling_Bad_About_Self TEXT)"
    )
    for city in data_list:
        city_name = city[0]
        gen_health = city[1]
        overweight = city[2]
        feel_bad = city[3]
        cur.execute("INSERT OR IGNORE INTO CityHealth (city, Gen_Health, overweight, Feeling_Bad_About_Self) VALUES (?,?,?,?)",
                    (city_name,gen_health,overweight,feel_bad))
    conn.commit()    



conn = sqlite3.connect("proj_base")
# Create a cursor object to execute SQL queries
cur = conn.cursor()
data = make_tup_list(list_of_us_cities)
make_SQL(data, cur, conn)
# scrape_city('Boston-Massachusetts')
# print(scrape_city('Boston-Massachusetts'))
