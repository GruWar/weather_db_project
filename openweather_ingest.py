from dotenv import load_dotenv
import psycopg2
import requests
import json
import os

load_dotenv()
apikey = os.getenv("OPENWEATHER_API_KEY")
cities = ['Prague,CZ', 'Brno,CZ', 'Ostrava,CZ']
conn_params = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD")
}

# Connect to DB and load data
try:
    conn = psycopg2.connect(**conn_params)
    print("Connected to DB")
    cur = conn.cursor()
except Exception as e:
    print(f"Error:",e)

for city in cities:
    try:
        # OpenWeather get data from API
        response = requests.get(f"https://api.openweathermap.org/data/2.5/weather?q={city}&units=metric&APPID={apikey}")
        if response.status_code == 200:
            payload_json = response.json()
            data = json.dumps(payload_json)
        else:
            print(response.text)
            continue
        cur.execute(
            "INSERT INTO bronze.openweather_raw (payload) VALUES (%s);",
            (data,)
        )
        print(f"City: {city} OK!")
    except Exception as e:
        print(f"Error {city}:",e)
try:
    conn.commit()
except Exception as e:
    print(f"Error:",e)
cur.close()
conn.close()


