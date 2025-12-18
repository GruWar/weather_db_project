-- delete old date
DELETE FROM gold.fact_weather_monthly
WHERE date = date_trunc('month', CURRENT_DATE)::date;

-- insert
INSERT INTO gold.fact_weather_monthly (
    city_id,
    date,
    avg_min_temp_c,
    avg_max_temp_c,
    avg_temp_c,
    total_precipitation_mm
)
SELECT
    city_id,
    date_trunc('month', CURRENT_DATE)::date AS date,
    ROUND(AVG(min_temp_c), 2)  AS avg_min_temp_c,
    ROUND(AVG(max_temp_c), 2)  AS avg_max_temp_c,
    ROUND(AVG((min_temp_c + max_temp_c) / 2), 2) AS avg_temp_c,
    ROUND(SUM(precipitation_mm), 2) AS total_precipitation_mm
FROM silver.fact_weather_daily
WHERE date >= date_trunc('month', CURRENT_DATE)::date
  AND date <  (date_trunc('month', CURRENT_DATE) + INTERVAL '1 month')::date
GROUP BY city_id;