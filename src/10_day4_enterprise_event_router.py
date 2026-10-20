"""
Day 4 (Part B): Enterprise Integration Engine (Apache Camel / Kafka EIPs + Snowflake Blueprint)
Implements:
  1. Event-Driven Splitter & Content-Based Router (CloudEvents v1.0 format)
  2. Routes North Sea 5m+ Wave Breaches -> 'sap.pm.offshore_work_order_halt' topic
  3. Routes 12z HDD Gas Demand Spikes   -> 'salesforce.crm.energy_trading_alert' topic
  4. Generates Snowflake Iceberg & Dynamic Table SQL DDL Blueprint + Day 4 Architecture Visual
"""

from pathlib import Path
import json
import uuid
from datetime import datetime, timezone
import duckdb
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch


def run_enterprise_integration_router():
    outbox_dir = Path("data/events_outbox")
    outbox_dir.mkdir(parents=True, exist_ok=True)
    out_dir = Path("data/output")
    out_dir.mkdir(parents=True, exist_ok=True)

    print("\n--- 1. RUNNING ENTERPRISE INTEGRATION ROUTER (CAMEL / KAFKA EIP PATTERN) ---")

    # Source 1: Read North Sea Wave Breaches (>= 5.0m) from Day 3.5 Parquet
    wave_parquet = Path("data/lakehouse/north_sea_wave_alerts.parquet")
    wave_breaches = duckdb.sql(f"""
        SELECT
            asset_id,
            name AS asset_name,
            ROUND(MAX(swh_m), 2)                  AS peak_swh_m,
            ROUND(MAX(wind_ms), 1)                AS peak_wind_ms,
            COUNT(*) FILTER (WHERE swh_m >= 5.0)  AS breach_duration_hours,
            CAST(MIN(time) FILTER (WHERE swh_m >= 5.0) AS VARCHAR) AS breach_start_utc
        FROM read_parquet('{wave_parquet}')
        GROUP BY asset_id, name
        HAVING MAX(swh_m) >= 5.0
        ORDER BY peak_swh_m DESC
    """).df()

    # Source 2: Read UK Energy Trading Alerts from Day 3 dbt Gold Mart
    con = duckdb.connect("data/lakehouse/weather_lakehouse.duckdb", read_only=True)
    trading_alerts = con.sql("""
        SELECT * FROM main.mart_uk_trading_desk_alerts
        WHERE risk_level IN ('CRITICAL_STORM_RISK', 'ELEVATED_HEATING_DEMAND')
    """).df()
    con.close()

    sap_events = []
    salesforce_events = []

    # EIP Pattern: Message Translator (to CloudEvents v1.0) + Content-Based Router
    # Route A: Marine SWH >= 5.0m -> SAP Plant Maintenance (Halts Offshore Rig/Heli Work Orders)
    for _, row in wave_breaches.iterrows():
        event = {
            "specversion": "1.0",
            "id": str(uuid.uuid4()),
            "source": "urn:weather-platform:north-sea-wave-engine",
            "type": "com.energy.sap.offshore.maintenance.halt",
            "time": datetime.now(timezone.utc).isoformat(),
            "datacontenttype": "application/json",
            "kafka_topic": "sap.pm.offshore_work_order_halt",
            "kafka_partition_key": row["asset_id"],
            "data": {
                "sap_plant_code": row["asset_id"],
                "facility_name": row["asset_name"],
                "hazard_type": "SIGNIFICANT_WAVE_HEIGHT_BREACH_5M",
                "peak_wave_height_m": float(row["peak_swh_m"]),
                "peak_wind_speed_ms": float(row["peak_wind_ms"]),
                "breach_hours": int(row["breach_duration_hours"]),
                "suspend_from_utc": row["breach_start_utc"],
                "sap_action": "HOLD_HELICOPTER_AND_VESSEL_DISPATCH",
            },
        }
        sap_events.append(event)

    # Route B: 12z ECMWF Cold Storm Revision -> Salesforce CRM / Energy Trading Desk
    for _, row in trading_alerts.iterrows():
        event = {
            "specversion": "1.0",
            "id": str(uuid.uuid4()),
            "source": "urn:weather-platform:dbt-gold-mart",
            "type": "com.energy.salesforce.trading.hdd_revision_alert",
            "time": datetime.now(timezone.utc).isoformat(),
            "datacontenttype": "application/json",
            "kafka_topic": "salesforce.crm.energy_trading_alert",
            "kafka_partition_key": row["asset_id"],
            "data": {
                "asset_id": row["asset_id"],
                "asset_name": row["asset_name"],
                "asset_type": row["asset_type"],
                "risk_classification": row["risk_level"],
                "hdd_00z_baseline": float(row["total_hdd_00z"]),
                "hdd_12z_revised": float(row["total_hdd_12z"]),
                "hdd_surge_delta": float(row["hdd_revision_delta"]),
                "min_pressure_hpa": float(row["min_pressure_hpa"]),
                "trading_recommendation": "LONG_UK_NBP_GAS_DEMAND" if row["hdd_revision_delta"] > 0.3 else "MONITOR",
            },
        }
        salesforce_events.append(event)

    # Write routed event streams to local Kafka Outbox JSONL files
    sap_outbox = outbox_dir / "kafka_topic_sap_offshore_halt.json"
    sfdc_outbox = outbox_dir / "kafka_topic_salesforce_trading_alerts.json"
    sap_outbox.write_text(json.dumps(sap_events, indent=2))
    sfdc_outbox.write_text(json.dumps(salesforce_events, indent=2))

    print(f"[ROUTE 1 -> SAP ERP] Published {len(sap_events)} Offshore Halt Events to: {sap_outbox}")
    for e in sap_events:
        d = e["data"]
        print(f"  • [SAP HOLD] {d['sap_plant_code']} ({d['facility_name']}): Peak SWH={d['peak_wave_height_m']}m ({d['breach_hours']}h >= 5m)")

    print(f"\n[ROUTE 2 -> SALESFORCE / TRADING] Published {len(salesforce_events)} Trading Events to: {sfdc_outbox}")
    for e in salesforce_events:
        d = e["data"]
        print(f"  • [SFDC ALERT] {d['asset_id']}: HDD jumped +{d['hdd_surge_delta']} -> Action: {d['trading_recommendation']}")

    # Generate Snowflake Production Blueprint SQL file (for UK Job Market Interviews!)
    snowflake_sql_path = outbox_dir / "snowflake_lakehouse_blueprint.sql"
    snowflake_sql_path.write_text("""
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
""".lstrip())
    print(f"\n[SNOWFLAKE BLUEPRINT] Saved interview-ready Snowflake Iceberg DDL to: {snowflake_sql_path}")

    # Generate Visual Diagram of Day 4 Architecture
    create_day4_visual(sap_events, salesforce_events, out_dir / "day4_integration_and_api_visual.png")


def create_day4_visual(sap_events: list, sfdc_events: list, out_img: Path):
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15.5, 6.8), dpi=150)
    fig.suptitle(
        "Day 4 Architecture: STAC Catalog, FastAPI OGC EDR & Event-Driven Enterprise Integration (Kafka/Camel)",
        fontsize=13.5, fontweight="bold", y=0.98
    )

    # Left Panel: End-to-End API & Event Routing Architecture
    ax1.set_title("1. Replacing Static Apache Tomcat with OGC APIs & Camel/Kafka EIPs", fontsize=11, fontweight="bold")
    ax1.axis("off")

    boxes = [
        (0.04, 0.73, 0.92, 0.19, "#d9ead3", "#274e13",
         "1. Cloud Storage & Lakehouse Layer (Zarr + Delta/Iceberg + dbt Marts)",
         "• era5_uk.zarr & north_sea_wave.zarr  |  • DuckDB / Snowflake / BigQuery Gold Marts"),
        (0.04, 0.48, 0.44, 0.19, "#cfe2f3", "#0b5394",
         "2A. Synchronous Pull: STAC + FastAPI",
         "• GET /stac/collections (Discovery)\n• GET /collections/{id}/position?coords=POINT()\n• On-the-fly Zarr slicing (No static PNGs!)"),
        (0.52, 0.48, 0.44, 0.19, "#fff2cc", "#b45f06",
         "2B. Asynchronous Push: Camel / Kafka Router",
         "• Content-Based Router + CloudEvents v1.0\n• Evaluates SWH >= 5.0m & 12z HDD Deltas\n• Decouples Lakehouse from SAP & Salesforce"),
        (0.04, 0.12, 0.44, 0.27, "#f4cccc", "#990000",
         f"3A. Kafka Topic: sap.pm.offshore_halt ({len(sap_events)} Events)",
         "Target: SAP ERP Plant Maintenance\n"
         "• NS-PLAT-02 (Forties): 7.3m SWH -> HOLD OPS\n"
         "• NS-PLAT-01 (Clair Ridge): 6.5m -> HOLD OPS\n"
         "• NS-WIND-01 (Moray East): 6.3m -> HOLD OPS"),
        (0.52, 0.12, 0.44, 0.27, "#d9d2e9", "#351c75",
         f"3B. Kafka Topic: salesforce.trading ({len(sfdc_events)} Events)",
         "Target: Salesforce CRM / Gas Trading Desk\n"
         "• 12z Storm Revision (+0.85 to +1.55 HDD)\n"
         "• Signal: LONG_UK_NBP_GAS_DEMAND\n"
         "• Pushed to Trader Risk Dashboards"),
    ]

    for x, y, w, h, fc, ec, title, body in boxes:
        ax1.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.02", facecolor=fc, edgecolor=ec, linewidth=1.5))
        ax1.text(x + 0.02, y + h - 0.05, title, fontsize=9.2, fontweight="bold", color=ec)
        ax1.text(x + 0.02, y + 0.03, body, fontsize=8, family="monospace")

    # Right Panel: Plot Routed SAP Offshore Wave Breaches vs. 5.0m Limit
    ax2.set_title("2. Routed SAP Offshore Halt Events (SWH >= 5.0m Breach Severity)", fontsize=11, fontweight="bold")
    plants = [e["data"]["sap_plant_code"] for e in sap_events]
    peaks = [e["data"]["peak_wave_height_m"] for e in sap_events]
    hours = [e["data"]["breach_hours"] for e in sap_events]

    bars = ax2.bar(plants, peaks, color="#cc0000", alpha=0.85, width=0.45, label="Peak Significant Wave Height (m)")
    ax2.axhline(5.0, color="black", linestyle="--", linewidth=2, label="5.0m SAP Helicopter/Vessel Halt Threshold")
    ax2.set_ylabel("Peak Wave Height - SWH (meters)")
    ax2.set_ylim(0, 9.0)
    ax2.grid(axis="y", linestyle="--", alpha=0.4)
    ax2.legend(loc="upper right", fontsize=8.5)

    for bar, hr in zip(bars, hours):
        h = bar.get_height()
        ax2.text(
            bar.get_x() + bar.get_width() / 2, h + 0.2,
            f"{h:.2f}m\n({hr}h breach)",
            ha="center", fontsize=8.5, fontweight="bold"
        )

    plt.tight_layout(rect=[0, 0, 1, 0.94])
    plt.savefig(out_img)
    plt.close(fig)
    print(f"[SUCCESS] Saved Day 4 Architecture Visual to: {out_img.resolve()}")


if __name__ == "__main__":
    run_enterprise_integration_router()