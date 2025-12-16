-- dim cities
CREATE TABLE IF NOT EXISTS silver.dim_city(
    city_id BIGSERIAL PRIMARY KEY,
    city_name VARCHAR(70) NOT NULL,
    country_code VARCHAR(10) NOT NULL,
    lat NUMERIC(9,6),
    lon NUMERIC(9,6),
    meteostat_id INT,
    UNIQUE (city_name, country_code)
);