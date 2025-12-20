import psycopg2
import requests
import json
from airflow.hooks.base import BaseHook
from airflow.models import Variable
import logging

log = logging.getLogger(__name__)

def main():
    log.info("Starting openweather ingest")
    # --- CONFIG ---
    API_KEY = Variable.get("OPENWEATHER_API_KEY")
    conn = BaseHook.get_connection("weather_db")
    # DB connect parametters
    conn_params = {
        "host": conn.host,
        "port": conn.port,
        "database": conn.schema,
        "user": conn.login,
        "password": conn.password
    }

    # Connect to DB and load data
    try:
        conn = psycopg2.connect(**conn_params)
        print("Connected to DB")
        cur = conn.cursor()
    except Exception as e:
        print(f"[ERROR]:",e)

    # --- LOAD CITIES FROM DB ---
    cur.execute("""
        SELECT
            city_query
        FROM silver.dim_city
        WHERE city_query IS NOT NULL
    """)

    cities = cur.fetchall()

    for (city_query,) in cities:
        try:
            # OpenWeather get data from API
            response = requests.get(f"https://api.openweathermap.org/data/2.5/weather?q={city_query}&units=metric&APPID={API_KEY}")
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
            print(f"City: {city_query} OK!")
        except Exception as e:
            print(f"[ERROR] {city_query}:",e)
    try:
        conn.commit()
    except Exception as e:
        print(f"[ERROR]:",e)
    cur.close()
    conn.close()
    log.info("openweather ingest finished successfully")


