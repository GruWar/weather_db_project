from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
import sys

sys.path.append("/opt/airflow")

def run_cities():
    from scripts.cities_ingest import main
    main()

def run_meteostat():
    from scripts.meteostat_ingest import main
    main()

with DAG(
    dag_id="meteostat_historical_backfill",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["meteostat", "historical", "backfill"],
    template_searchpath="/opt/airflow/sql"
) as dag:

    ingest_cities = PythonOperator(
        task_id="ingest_cities",
        python_callable=run_cities
    )

    ingest_meteostat = PythonOperator(
        task_id="ingest_meteostat",
        python_callable=run_meteostat
    )

    silver_meteostat = PostgresOperator(
        task_id="silver_meteostat",
        postgres_conn_id="weather_db",
        sql="meteostat_bronze_to_silver.sql"
    )

    (
        ingest_cities
        >> ingest_meteostat
        >> silver_meteostat
    )
