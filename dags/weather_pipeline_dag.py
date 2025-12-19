from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
import sys

sys.path.append("/opt/airflow")

def run_cities():
    from scripts.cities_ingest import main
    main()

def run_openweather():
    from scripts.openweather_ingest import main
    main()

def run_meteostat():
    from scripts.meteostat_ingest import main
    main()

with DAG(
    dag_id="weather_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["weather", "portfolio"],
) as dag:

    ingest_cities = PythonOperator(
        task_id="ingest_cities",
        python_callable=run_cities
    )

    ingest_openweather = PythonOperator(
        task_id="ingest_openweather",
        python_callable=run_openweather
    )

    ingest_meteostat = PythonOperator(
        task_id="ingest_meteostat",
        python_callable=run_meteostat
    )

    silver_openweather = PostgresOperator(
        task_id="silver_openweather",
        postgres_conn_id="weather_db",
        sql="sql/openweather_bronze_to_silver.sql"
    )

    silver_meteostat = PostgresOperator(
        task_id="silver_meteostat",
        postgres_conn_id="weather_db",
        sql="sql/meteostat_bronze_to_silver.sql"
    )

    gold_daily = PostgresOperator(
        task_id="gold_daily",
        postgres_conn_id="weather_db",
        sql="sql/fact_weather_daily.sql"
    )

    gold_monthly = PostgresOperator(
        task_id="gold_monthly",
        postgres_conn_id="weather_db",
        sql="sql/fact_weather_monthly.sql"
    )

    (
        ingest_cities
        >> ingest_openweather
        >> ingest_meteostat
        >> silver_openweather
        >> silver_meteostat
        >> gold_daily
        >> gold_monthly
    )
