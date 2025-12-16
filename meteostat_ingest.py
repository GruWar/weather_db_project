from dotenv import load_dotenv
from datetime import datetime, timedelta
import psycopg2
import requests
import json
import os

load_dotenv()
apikey = os.getenv("METEOSTAT_API_KEY")
stations = {
    "Prague,CZ":"11518",
    "Brno,CZ":"11723",
    "Ostrava,CZ":"11782",
}
start_date = datetime.today() - timedelta(days=365)
start_date = start_date.strftime('%Y-%m-%d')
end_date = datetime.today() - timedelta(days=1)
end_date = end_date.strftime('%Y-%m-%d')
conn_params = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD")
}
headers = {
	"x-rapidapi-key": apikey,
	"x-rapidapi-host": "meteostat.p.rapidapi.com"
}
# Connect to DB and load data
try:
    conn = psycopg2.connect(**conn_params)
    print("Connected to DB")
    cur = conn.cursor()
except Exception as e:
    print(f"Error:",e)

for station, station_id in stations.items():
    try:
        # Meteostat get data from API
        response = requests.get(f"https://meteostat.p.rapidapi.com/stations/daily?station={station_id}&start={start_date}&end={end_date}", headers=headers)
        if response.status_code == 200:
            payload_json = response.json()
            data = json.dumps(payload_json)
        else:
            print(response.text)
            continue
        cur.execute(
            "INSERT INTO bronze.meteostat_raw (payload) VALUES (%s);",
            (data,)
        )
        print(f"Station: {station} OK!")
    except Exception as e:
        print(f"Error {station}:",e)
try:
    conn.commit()
except Exception as e:
    print(f"Error:",e)
cur.close()
conn.close()