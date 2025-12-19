INSERT INTO silver.fact_weather_daily (
    city_id,
    date,
    avg_temp_c,
    min_temp_c,
    max_temp_c,
    precipitation_mm
)
SELECT
    c.city_id,
    (day ->> 'date')::date AS date,
    (day ->> 'tavg')::numeric AS avg_temp_c,
    (day ->> 'tmin')::numeric AS min_temp_c,
    (day ->> 'tmax')::numeric AS max_temp_c,
    (day ->> 'prcp')::numeric AS precipitation_mm
FROM bronze.meteostat_raw AS r
JOIN silver.dim_city AS c
    ON c.meteostat_id = r.station_id
CROSS JOIN LATERAL
    jsonb_array_elements(r.payload -> 'data') AS day
ON CONFLICT (city_id, date) DO NOTHING;