import psycopg2

# Připojovací parametry
conn_params = {
    "host": "localhost",
    "port": 5432,
    "database": "weather_db",
    "user": "postgres",
    "password": "password"
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