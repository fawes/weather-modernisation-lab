"""
Day 3 (Part B): Scaffolds and executes a production-grade dbt Core project (dbt-duckdb)
Implements Medallion Architecture:
  - Source: raw_weather (uk_energy_forecasts_latest & v0)
  - Staging Model: stg_uk_energy_forecasts (View)
  - Mart Model: mart_uk_trading_desk_alerts (Table comparing V1 vs V0 with storm alerts)
  - Data Quality Tests: Schema tests (not_null, accepted_values) + Custom Physical Weather Bounds test
"""

from pathlib import Path
import subprocess
import sys
import duckdb

def build_and_run_dbt():
    dbt_dir = Path("dbt_weather")
    models_staging = dbt_dir / "models" / "staging"
    models_marts = dbt_dir / "models" / "marts"
    tests_dir = dbt_dir / "tests"

    for d in [models_staging, models_marts, tests_dir]:
        d.mkdir(parents=True, exist_ok=True)

    # 1. profiles.yml (Connects dbt to our local DuckDB Lakehouse file)
    # In AWS/GCP/Azure, this profile simply points to BigQuery, Snowflake, Athena, or Databricks!
    (dbt_dir / "profiles.yml").write_text("""
dbt_weather:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: '../data/lakehouse/weather_lakehouse.duckdb'
      threads: 4
""".lstrip())

    # 2. dbt_project.yml
    (dbt_dir / "dbt_project.yml").write_text("""
name: 'dbt_weather'
version: '1.0.0'
config-version: 2
profile: 'dbt_weather'

model-paths: ["models"]
test-paths: ["tests"]

models:
  dbt_weather:
    staging:
      +materialized: view
    marts:
      +materialized: table
""".lstrip())

    # 3. Staging Sources & Schema Tests (models/staging/schema.yml)
    (models_staging / "schema.yml").write_text("""
version: 2

sources:
  - name: raw_weather
    schema: raw_weather
    tables:
      - name: uk_energy_forecasts_latest
      - name: uk_energy_forecasts_v0

models:
  - name: stg_uk_energy_forecasts
    description: "Cleaned & standardized hourly UK energy forecasts from latest ECMWF run"
    columns:
      - name: asset_id
        tests:
          - not_null
      - name: asset_type
        tests:
          - not_null
          - accepted_values:
              arguments:
                values: ['Offshore_Wind', 'Trading_Hub', 'Solar_Park']
      - name: temp_c
        tests:
          - not_null
      - name: pressure_hpa
        tests:
          - not_null
  - name: mart_uk_trading_desk_alerts
    description: "Gold Mart table for BP/Energy Trading Desk comparing 12z vs 00z runs"
    columns:
      - name: asset_id
        tests:
          - unique
          - not_null
      - name: risk_level
        tests:
          - accepted_values:
              arguments:
                values: ['CRITICAL_STORM_RISK', 'ELEVATED_HEATING_DEMAND', 'NORMAL_OPERATIONS']
""".lstrip())

    # 4. Staging SQL Model (models/staging/stg_uk_energy_forecasts.sql)
    (models_staging / "stg_uk_energy_forecasts.sql").write_text("""
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
""".lstrip())

    # 5. Gold Mart SQL Model (models/marts/mart_uk_trading_desk_alerts.sql)
    # Uses dbt's {{ ref() }} macro to automatically build the dependency lineage!
    (models_marts / "mart_uk_trading_desk_alerts.sql").write_text("""
WITH latest_run AS (
    SELECT * FROM {{ ref('stg_uk_energy_forecasts') }}
),
morning_baseline AS (
    SELECT * FROM {{ source('raw_weather', 'uk_energy_forecasts_v0') }}
),
latest_agg AS (
    SELECT
        asset_id,
        asset_name,
        asset_type,
        ROUND(MIN(temp_c), 2)       AS min_temp_c,
        ROUND(AVG(temp_c), 2)       AS avg_temp_c,
        ROUND(SUM(hdd_hourly), 2)   AS total_hdd_12z,
        ROUND(MIN(pressure_hpa), 1) AS min_pressure_hpa
    FROM latest_run
    GROUP BY asset_id, asset_name, asset_type
),
baseline_agg AS (
    SELECT
        asset_id,
        ROUND(SUM(hdd_hourly), 2)   AS total_hdd_00z
    FROM morning_baseline
    GROUP BY asset_id
)
SELECT
    l.asset_id,
    l.asset_name,
    l.asset_type,
    l.min_temp_c,
    l.avg_temp_c,
    b.total_hdd_00z,
    l.total_hdd_12z,
    ROUND(l.total_hdd_12z - b.total_hdd_00z, 2) AS hdd_revision_delta,
    l.min_pressure_hpa,
    CASE
        WHEN l.min_pressure_hpa < 1000.0 THEN 'CRITICAL_STORM_RISK'
        WHEN l.total_hdd_12z > 1.0       THEN 'ELEVATED_HEATING_DEMAND'
        ELSE 'NORMAL_OPERATIONS'
    END AS risk_level
FROM latest_agg l
JOIN baseline_agg b ON l.asset_id = b.asset_id
ORDER BY l.asset_id
""".lstrip())

    # 6. Custom Singular Data Quality Test (tests/assert_physical_weather_bounds.sql)
    # In dbt, a test PASSES if this query returns 0 offending rows!
    # Checks that no corrupted sensor/model value violates Earth's UK atmospheric physics.
    (tests_dir / "assert_physical_weather_bounds.sql").write_text("""
-- Fails if temperature is outside [-50°C, +55°C] or pressure is outside [870 hPa, 1085 hPa]
SELECT
    asset_id,
    forecast_timestamp_utc,
    temp_c,
    pressure_hpa
FROM {{ ref('stg_uk_energy_forecasts') }}
WHERE temp_c < -50.0
   OR temp_c > 55.0
   OR pressure_hpa < 870.0
   OR pressure_hpa > 1085.0
""".lstrip())

    print("[SUCCESS] Scaffolded dbt project inside: dbt_weather/")

    # 7. Execute 'dbt run' and 'dbt test' using our .venv dbt binary
    dbt_bin = str(Path(sys.executable).parent / "dbt")

    print("\n--- EXECUTING 'dbt run' (Materializing Staging Views & Mart Tables) ---")
    subprocess.run([dbt_bin, "run", "--profiles-dir", "."], cwd=dbt_dir, check=True)

    print("\n--- EXECUTING 'dbt test' (Running 8 Automated Data Quality Assertions) ---")
    subprocess.run([dbt_bin, "test", "--profiles-dir", "."], cwd=dbt_dir, check=True)

    # 8. Query the final Gold Mart table produced by dbt
    print("\n--- FINAL GOLD MART TABLE: mart_uk_trading_desk_alerts ---")
    con = duckdb.connect("data/lakehouse/weather_lakehouse.duckdb")
    con.sql("SELECT * FROM main.mart_uk_trading_desk_alerts").show()
    con.close()

if __name__ == "__main__":
    build_and_run_dbt()