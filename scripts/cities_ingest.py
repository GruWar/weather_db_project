
#=====================================================
# Cities ingestion from JSON file
#=====================================================
# Imports
from airflow.hooks.base import BaseHook
import psycopg2
import json
import logging

log = logging.getLogger(__name__)

def main():
    log.info("Starting cities ingest")

    # load json
    try:
        with open("/opt/airflow/datasets/cities.json", "r", encoding="utf-8") as f:
            data = json.load(f)
            cities = data["cities"]
    except Exception as e:
        raise RuntimeError(f"Failed to load cities.json: {e}")

    conn = BaseHook.get_connection("weather_db")
    # DB connect parametters
    conn_params = {
        "host": conn.host,
        "port": conn.port,
        "database": conn.schema,
        "user": conn.login,
        "password": conn.password
    }

    # Connect to DB
    try:
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()
        print("Connected to DB")
    except Exception as e:
        raise RuntimeError(f"DB connection failed: {e}")

    # Insert cities
    for city in cities:
        try:
            cur.execute(
                """
                INSERT INTO silver.dim_city (
                    city_name,
                    city_query,
                    country_code,
                    meteostat_id,
                    lat,
                    lon
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (city_name, country_code) DO NOTHING;
                """,
                (
                    city["city_name"],
                    city["sources"]["openweather"]["city_query"],
                    city["country_code"],
                    city["sources"]["meteostat"]["station_id"],
                    city["lat"],
                    city["lon"]
                )
            )
            print(f"Inserted: {city['city_name']}, {city['country_code']}")
        except Exception as e:
            print(f"[ERROR] inserting {city['city_name']}: {e}")

    # Commit & close
    conn.commit()
    cur.close()
    conn.close()
    print("Disconnected from DB")
    print("City ingest finished.")
    log.info("Cities ingest finished successfully")
