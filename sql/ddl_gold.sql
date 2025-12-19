CREATE TABLE IF NOT EXISTS gold.fact_weather_daily (
    city_id BIGINT NOT NULL,
    date DATE,
    min_temp_c NUMERIC(5,2),
    max_temp_c NUMERIC(5,2),
    avg_temp_c NUMERIC(5,2),
    precipitation_mm NUMERIC(6,2),

    PRIMARY KEY (city_id, date),
    CONSTRAINT fk_city
        FOREIGN KEY (city_id)
        REFERENCES silver.dim_city (city_id)
);

CREATE TABLE IF NOT EXISTS gold.fact_weather_monthly (
    city_id BIGINT NOT NULL,
    date DATE,
    avg_min_temp_c NUMERIC(5,2),
    avg_max_temp_c NUMERIC(5,2),
    avg_temp_c NUMERIC(5,2),
    total_precipitation_mm NUMERIC(6,2),

    PRIMARY KEY (city_id, date),
    CONSTRAINT fk_city
        FOREIGN KEY (city_id)
        REFERENCES silver.dim_city (city_id)
);