from dotenv import load_dotenv
import psycopg2
import os
import json

load_dotenv()

conn_params = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD")
}
# load json
try:
    with open("cities.json", "r", encoding="utf-8") as f:
        data = json.load(f)
        cities = data["cities"]
except Exception as e:
    raise RuntimeError(f"Failed to load cities.json: {e}")

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
                country_code,
                meteostat_id,
                lat,
                lon
            )
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (city_name, country_code) DO NOTHING;
            """,
            (
                city["city_name"],
                city["country_code"],
                city["sources"]["meteostat"]["station_id"],
                city["lat"],
                city["lon"]
            )
        )
        print(f"Inserted: {city['city_name']}, {city['country_code']}")
    except Exception as e:
        print(f"Error inserting {city['city_name']}: {e}")

# Commit & close
conn.commit()
cur.close()
conn.close()

print("City ingest finished.")