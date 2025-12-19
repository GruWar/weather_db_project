from dotenv import load_dotenv
import psycopg2
import os
load_dotenv()

# Připojovací parametry
conn_params = {
    "host": os.getenv("WEATHER_DB_HOST"),
    "port": os.getenv("WEATHER_DB_PORT"),
    "database": os.getenv("WEATHER_DB_NAME"),
    "user": os.getenv("WEATHER_DB_USER"),
    "password": os.getenv("WEATHER_DB_PASSWORD")
}

try:
    # vytvoření spojení
    conn = psycopg2.connect(**conn_params)
    print("Připojení k PostgreSQL funguje!")
    
    # volitelně: test dotaz
    cur = conn.cursor()
    cur.execute("SELECT version();")
    version = cur.fetchone()
    print("PostgreSQL verze:", version[0])
    
    cur.close()
    conn.close()

except psycopg2.Error as e:
    print("Chyba při připojení:", e)