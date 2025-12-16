-- open Weather
CREATE TABLE IF NOT EXISTS bronze.openweather_raw (
    raw_id BIGSERIAL PRIMARY KEY,
    load_date TIMESTAMP DEFAULT NOW(),
    payload JSONB
);

-- Meteostat
CREATE TABLE IF NOT EXISTS bronze.meteostat_raw (
    raw_id BIGSERIAL PRIMARY KEY,
    load_date TIMESTAMP DEFAULT NOW(),
    payload JSONB
);
