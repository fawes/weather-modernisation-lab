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
