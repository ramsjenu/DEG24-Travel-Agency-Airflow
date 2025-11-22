
-- Got this from the transformed_data_to_redshift script
begin;
CREATE TABLE IF NOT EXISTS countries_data (
    country_name TEXT NOT NULL,
    independent BOOLEAN,
    unMember BOOLEAN,
    startOfWeek VARCHAR(225),
    official_country_name TEXT,
    common_native_names TEXT,
    currency_code VARCHAR(225),
    currency_name TEXT,
    currency_symbol VARCHAR(225),
    country_code VARCHAR(2000) UNIQUE NOT NULL,
    capital TEXT,
    region TEXT,
    subregion TEXT,
    languages VARCHAR (1000),
    area FLOAT,
    population BIGINT,
    continents TEXT
);
end;
