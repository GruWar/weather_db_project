-- dim cities
CREATE TABLE IF NOT EXISTS silver.dim_city (
    city_id BIGSERIAL PRIMARY KEY,
    city_name VARCHAR(70) NOT NULL,
    country_code VARCHAR(10) NOT NULL,
    lat NUMERIC(9,6),
    lon NUMERIC(9,6),
    meteostat_id INT,
    UNIQUE (city_name, country_code)
);

-- weather fact
CREATE TABLE IF NOT EXISTS silver.fact_weather_daily (
    city_id BIGINT NOT NULL,
    date DATE NOT NULL,

    avg_temp_c NUMERIC(5,2),
    min_temp_c NUMERIC(5,2),
    max_temp_c NUMERIC(5,2),
    precipitation_mm NUMERIC(6,2),

    PRIMARY KEY (city_id, date),
    CONSTRAINT fk_city
        FOREIGN KEY (city_id)
        REFERENCES silver.dim_city (city_id)
);