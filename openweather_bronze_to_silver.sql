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
    (to_timestamp((payload ->> 'dt')::bigint)
     + ((payload ->> 'timezone')::int * interval '1 second')
    )::date AS date,
    NULL AS avg_temp_c,
    (r.payload -> 'main' ->> 'temp_min')::numeric AS min_temp_c,
    (r.payload -> 'main' ->> 'temp_max')::numeric AS max_temp_c,
    COALESCE((r.payload -> 'rain' ->> '1h'), (r.payload -> 'rain' ->> '3h'), NULL)::numeric AS precipitation_mm
FROM bronze.openweather_raw AS r
INNER JOIN silver.dim_city AS c
    ON c.city_name = (r.payload ->> 'name')
ON CONFLICT (city_id, date) DO NOTHING;