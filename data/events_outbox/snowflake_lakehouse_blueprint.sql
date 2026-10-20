-- ============================================================================
-- SNOWFLAKE + ICEBERG + KAFKA STREAMING BLUEPRINT (UK ENERGY TRADING PLATFORM)
-- Maps directly to our local DuckDB + Delta Lake + dbt + Kafka lab!
-- ============================================================================

-- 1. External Storage Volume pointing to Cloud Object Storage (S3 / Azure ADLS / GCS)
CREATE OR REPLACE EXTERNAL VOLUME uk_weather_lakehouse_vol
  STORAGE_LOCATIONS = (
    (
      NAME = 'uk-weather-s3-london'
      STORAGE_PROVIDER = 'S3'
      STORAGE_BASE_URL = 's3://uk-energy-weather-lakehouse/iceberg/'
      STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/SnowflakeIcebergRole'
    )
  );

-- 2. Apache Iceberg Table over our Extracted UK Energy Forecasts (Zero-Copy!)
CREATE OR REPLACE ICEBERG TABLE raw_weather.uk_energy_forecasts_iceberg (
    forecast_timestamp_utc TIMESTAMP_NTZ,
    asset_id STRING,
    asset_name STRING,
    asset_type STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    temp_c DOUBLE,
    pressure_hpa DOUBLE,
    hdd_hourly DOUBLE,
    forecast_run STRING
)
  CATALOG = 'SNOWFLAKE'
  EXTERNAL_VOLUME = 'uk_weather_lakehouse_vol'
  BASE_LOCATION = 'uk_energy_forecasts/';

-- 3. Snowflake Dynamic Table (Declarative Streaming ELT - Equivalent to our dbt Mart!)
CREATE OR REPLACE DYNAMIC TABLE marts.mart_uk_trading_desk_alerts
  TARGET_LAG = '5 minutes'
  WAREHOUSE = 'TRADING_ANALYTICS_WH'
AS
  SELECT
      asset_id,
      asset_name,
      asset_type,
      MIN(temp_c)       AS min_temp_c,
      SUM(hdd_hourly)   AS total_hdd,
      MIN(pressure_hpa) AS min_pressure_hpa,
      CASE
          WHEN MIN(pressure_hpa) < 1000.0 THEN 'CRITICAL_STORM_RISK'
          ELSE 'NORMAL_OPERATIONS'
      END AS risk_level
  FROM raw_weather.uk_energy_forecasts_iceberg
  GROUP BY asset_id, asset_name, asset_type;
