import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

def fetch_weather_data_from_db(database_path):
    conn = sqlite3.connect(database_path)
    query = "SELECT city, date, hour, temp FROM WeatherCities LIMIT 200"
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def plot_temperature_by_city(df):
    # Convert date and hour columns to a datetime format
    df['datetime'] = pd.to_datetime(df['date'] + ' ' + df['hour'])

    # Group by city and date, then plot temperature over time
    for city, data in df.groupby('city'):
        data.set_index('datetime')['temp'].plot(label=city, figsize=(10, 6))

    plt.title('Temperature by City Over Time')
    plt.xlabel('Date')
    plt.ylabel('Temperature (°F)')
    plt.legend()
    plt.show()

def fetch_weather_bike_data(database_path):
    conn = sqlite3.connect(database_path)
    # Select necessary columns from both tables and perform a join
    query = """
        SELECT wc.city, wc.precip, bc.free_bikes
        FROM WeatherCities wc
        JOIN BikeCities bc ON wc.city = bc.city
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df
def plot_bikes_precip_scatter(df):
    plt.scatter(df['precip'], df['free_bikes'])
    plt.title('Number of Free Bikes vs Precipitation Prediction')
    plt.xlabel('Precipitation Prediction (%)')
    plt.ylabel('Number of Free Bikes')
    plt.show()


weather_data = fetch_weather_data_from_db("proj_base")
# Plot temperature trends by city
plot_temperature_by_city(weather_data)
weather_bike_data = fetch_weather_bike_data("proj_base")
plot_bikes_precip_scatter(weather_bike_data)

