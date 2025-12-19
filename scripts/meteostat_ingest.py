from dotenv import load_dotenv
from datetime import datetime, timedelta
import psycopg2
import requests
import json
import os

load_dotenv()

# --- CONFIG ---
API_KEY = os.getenv("METEOSTAT_API_KEY")

HEADERS = {
    "x-rapidapi-key": API_KEY,
    "x-rapidapi-host": "meteostat.p.rapidapi.com"
}

START_DATE = (datetime.today() - timedelta(days=365)).strftime("%Y-%m-%d")
END_DATE = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")

DB_PARAMS = {
    "host": os.getenv("WEATHER_DB_HOST"),
    "port": os.getenv("WEATHER_DB_PORT"),
    "database": os.getenv("WEATHER_DB_NAME"),
    "user": os.getenv("WEATHER_DB_USER"),
    "password": os.getenv("WEATHER_DB_PASSWORD"),
}

# --- DB CONNECT ---
conn = psycopg2.connect(**DB_PARAMS)
cur = conn.cursor()
print("Connected to DB")

# --- LOAD CITIES FROM DB ---
cur.execute("""
    SELECT
        city_id,
        meteostat_id
    FROM silver.dim_city
    WHERE meteostat_id IS NOT NULL
""")

cities = cur.fetchall()

# --- INGEST LOOP ---
for city_id, station_id in cities:
    try:
        response = requests.get(f"https://meteostat.p.rapidapi.com/stations/daily?station={station_id}&start={START_DATE}&end={END_DATE}", headers=HEADERS)
        if response.status_code == 200:
            payload_json = response.json()
            data = json.dumps(payload_json)
        else:
            print(response.text)
            continue
        cur.execute(
            "INSERT INTO bronze.meteostat_raw (station_id,payload) VALUES (%s, %s);",
            (station_id, data,)
        )

        print(f"[OK] City {city_id} / Station {station_id}")

    except Exception as e:
        print(f"[ERROR] Station {station_id}: {e}")

# --- COMMIT & CLOSE ---
conn.commit()
cur.close()
conn.close()
print("Done.")
