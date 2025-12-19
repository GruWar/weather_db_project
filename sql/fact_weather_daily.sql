-- delete old date
DELETE FROM gold.fact_weather_daily
WHERE date = CURRENT_DATE;

INSERT INTO gold.fact_weather_daily (
    city_id,
    date,
    min_temp_c,
    max_temp_c,
    avg_temp_c,
    precipitation_mm 
)
SELECT
    city_id,
    date,
    min_temp_c,
    max_temp_c,
    ROUND((min_temp_c + max_temp_c) / 2, 2) AS avg_temp_c,
    precipitation_mm
FROM silver.fact_weather_daily
WHERE date = CURRENT_DATE;

select * from gold.fact_weather_daily