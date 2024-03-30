import os
import requests
import psycopg2
from datetime import datetime
import logging
import json  # Import the json module to read the json file
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Setup basic logging
logging.basicConfig(level=logging.INFO)

# Database connection parameters from environment variables
conn_params = {
    "dbname": os.getenv("DB_NAME", "postgres"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "xxx"),
    "host": os.getenv("DB_HOST", "xxx"),
    "port": os.getenv("DB_PORT", "xxx")
}

# Load locations from a JSON file
with open('locations.json', 'r') as f:
    locations = json.load(f)

def fetch_temperature(latitude, longitude):
    """Fetch current temperature for given coordinates."""
    try:
        logging.info("Fetching temperature from Open-Meteo...")
        response = requests.get(f"https://api.open-meteo.com/v1/forecast?latitude={latitude}&longitude={longitude}&current_weather=true")
        response.raise_for_status()  # Raise an exception for HTTP errors
        data = response.json()
        temperature = data['current_weather']['temperature']
        logging.info(f"Current temperature: {temperature}°C")
        return temperature
    except requests.RequestException as e:
        logging.error(f"Error fetching temperature data: {e}")

def insert_data(location, latitude, longitude, temperature):
    """Insert weather data into the database."""
    try:
        logging.info("Connecting to database...")
        with psycopg2.connect(**conn_params) as conn:
            with conn.cursor() as cursor:
                logging.info("Inserting data into database...")
                cursor.execute("INSERT INTO weather_data (location, latitude, longitude, temperature, timestamp) VALUES (%s, %s, %s, %s, %s);",
                               (location, latitude, longitude, temperature, datetime.now()))
                conn.commit()
                logging.info("Data inserted successfully.")
    except psycopg2.Error as e:
        logging.error(f"Database error: {e}")

def main():
    for loc in locations:
        temperature = fetch_temperature(loc["latitude"], loc["longitude"])
        if temperature is not None:
            insert_data(loc["name"], loc["latitude"], loc["longitude"], temperature)

if __name__ == "__main__":
    main()
