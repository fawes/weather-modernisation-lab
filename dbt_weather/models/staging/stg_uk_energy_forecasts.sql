WITH source_data AS (
    SELECT * FROM {{ source('raw_weather', 'uk_energy_forecasts_latest') }}
)
SELECT
    CAST(time AS TIMESTAMP)       AS forecast_timestamp_utc,
    asset_id,
    name                          AS asset_name,
    asset_type,
    ROUND(CAST(latitude AS DOUBLE), 2)     AS latitude,
    ROUND(CAST(longitude AS DOUBLE), 2)    AS longitude,
    ROUND(CAST(temp_c AS DOUBLE), 2)       AS temp_c,
    ROUND(CAST(pressure_hpa AS DOUBLE), 1) AS pressure_hpa,
    ROUND(CAST(hdd_hourly AS DOUBLE), 4)   AS hdd_hourly,
    forecast_run
FROM source_data
