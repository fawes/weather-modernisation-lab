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
