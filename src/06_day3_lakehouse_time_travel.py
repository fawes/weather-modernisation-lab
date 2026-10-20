"""
Day 3 (Part A): Open Table Formats (Delta Lake / Iceberg Architecture)
Demonstrates ACID Transactions, Copy-on-Write Updates, and Forecast Time Travel
"""

from pathlib import Path
import json
import shutil
import pandas as pd
import duckdb
from deltalake import write_deltalake, DeltaTable
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch

def run_lakehouse_time_travel():
    raw_parquet_dir = Path("data/lakehouse/uk_energy_forecasts")
    delta_table_path = Path("data/lakehouse/uk_energy_delta")
    out_dir = Path("data/output")
    out_dir.mkdir(parents=True, exist_ok=True)

    if delta_table_path.exists():
        shutil.rmtree(delta_table_path)

    # =========================================================================
    # STEP 1: Load Day 2 Parquet & Write Version 0 (00z Morning ECMWF Run)
    # =========================================================================
    print("\n--- 1. CREATING ACID DELTA LAKE TABLE: VERSION 0 (00z FORECAST RUN) ---")
    df_v0 = duckdb.sql(
        f"SELECT * FROM read_parquet('{raw_parquet_dir}/**/*.parquet', hive_partitioning=true)"
    ).df()
    df_v0["forecast_run"] = "00z_MORNING_RUN"

    # Write Version 0 to Delta Lake
    write_deltalake(delta_table_path, df_v0, mode="overwrite")
    dt = DeltaTable(delta_table_path)
    print(f"[SUCCESS] Created Delta Table at: {delta_table_path} (Current Version: {dt.version()})")

    # =========================================================================
    # STEP 2: Simulate Mid-Day 12z ECMWF Forecast Revision (North Sea Storm!)
    # =========================================================================
    # In energy trading, a 12z forecast revision showing a sudden pressure drop
    # (storm front) and colder temperatures shifts gas demand (HDD) and wind output.
    print("\n--- 2. APPLYING 12z MID-DAY FORECAST REVISION (VERSION 1) ---")
    df_v1 = df_v0.copy()
    df_v1["forecast_run"] = "12z_REVISED_RUN"

    # Simulate a cold North Sea storm front hitting Offshore Wind & Hubs from 12:00 UTC onwards
    afternoon_mask = pd.to_datetime(df_v1["time"]).dt.hour >= 12
    df_v1.loc[afternoon_mask, "temp_c"] = (df_v1.loc[afternoon_mask, "temp_c"] - 3.2).round(2)
    df_v1.loc[afternoon_mask, "pressure_hpa"] = (df_v1.loc[afternoon_mask, "pressure_hpa"] - 14.5).round(1)
    # Recalculate Heating Degree Days (HDD) after the 3.2°C temperature drop
    df_v1["hdd_hourly"] = ((15.5 - df_v1["temp_c"]).clip(lower=0.0) / 24.0).round(4)

    # Overwrite the table atomically. Under the hood, Delta Lake DOES NOT delete Version 0's
    # Parquet files! It uses Copy-on-Write to add a new Parquet file and writes 00001.json.
    write_deltalake(delta_table_path, df_v1, mode="overwrite")
    dt = DeltaTable(delta_table_path)
    print(f"[SUCCESS] Committed Revised Forecast! (New Table Version: {dt.version()})")

    # =========================================================================
    # STEP 3: Inspect the Transaction Log (_delta_log) & Physical Files
    # =========================================================================
    print("\n--- 3. INSPECTING THE TRANSACTION LOG (_delta_log) ON DISK ---")
    for p in sorted(delta_table_path.rglob("*")):
        if p.is_file():
            print(f"  ├── {str(p.relative_to(delta_table_path)):<48} ({p.stat().st_size:,} bytes)")

    print("\nAudit History from Delta Transaction Log:")
    for entry in dt.history():
        print(f"  • Version {entry.get('version')}: operation={entry.get('operation')} | timestamp={entry.get('timestamp')}")

    # =========================================================================
    # STEP 4: Time-Travel Analytics (Comparing Version 0 vs. Version 1 in SQL)
    # =========================================================================
    # Energy traders always ask: "What changed between the 00z run and the 12z run?"
    # With Delta/Iceberg Time Travel, we load Version 0 and Version 1 side-by-side!
    print("\n--- 4. TIME-TRAVEL COMPARISON: 00z (VERSION 0) VS. 12z (VERSION 1) ---")
    v0_arrow = DeltaTable(delta_table_path, version=0).to_pyarrow_table()
    v1_arrow = DeltaTable(delta_table_path, version=1).to_pyarrow_table()

    comparison_sql = """
    WITH v0_agg AS (
        SELECT
            asset_id,
            name,
            asset_type,
            ROUND(AVG(temp_c), 2)       AS v0_avg_temp_c,
            ROUND(SUM(hdd_hourly), 2)   AS v0_daily_hdd,
            ROUND(MIN(pressure_hpa), 1) AS v0_min_press_hpa
        FROM v0_arrow
        GROUP BY asset_id, name, asset_type
    ),
    v1_agg AS (
        SELECT
            asset_id,
            ROUND(AVG(temp_c), 2)       AS v1_avg_temp_c,
            ROUND(SUM(hdd_hourly), 2)   AS v1_daily_hdd,
            ROUND(MIN(pressure_hpa), 1) AS v1_min_press_hpa
        FROM v1_arrow
        GROUP BY asset_id
    )
    SELECT
        v0.asset_id,
        v0.name,
        v0.v0_avg_temp_c,
        v1.v1_avg_temp_c,
        ROUND(v1.v1_avg_temp_c - v0.v0_avg_temp_c, 2) AS temp_delta_c,
        v0.v0_daily_hdd,
        v1.v1_daily_hdd,
        ROUND(v1.v1_daily_hdd - v0.v0_daily_hdd, 2)   AS extra_gas_hdd,
        v1.v1_min_press_hpa
    FROM v0_agg v0
    JOIN v1_agg v1 ON v0.asset_id = v1.asset_id
    ORDER BY v0.asset_id;
    """

    comp_df = duckdb.sql(comparison_sql).df()
    duckdb.sql(comparison_sql).show()

    # Save our latest Delta snapshot into a local DuckDB database file so dbt can build on it next
    db_path = Path("data/lakehouse/weather_lakehouse.duckdb")
    if db_path.exists():
        db_path.unlink()
    con = duckdb.connect(str(db_path))
    con.execute("CREATE SCHEMA IF NOT EXISTS raw_weather;")
    con.execute("CREATE TABLE raw_weather.uk_energy_forecasts_v0 AS SELECT * FROM v0_arrow;")
    con.execute("CREATE TABLE raw_weather.uk_energy_forecasts_latest AS SELECT * FROM v1_arrow;")
    con.close()
    print(f"[SUCCESS] Registered V0 & V1 Lakehouse tables in: {db_path}")

    # Generate Visual Diagram
    create_day3_visual(comp_df, out_dir / "day3_lakehouse_timetravel_visual.png")

def create_day3_visual(comp_df: pd.DataFrame, out_img: Path):
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6.5), dpi=150)
    fig.suptitle("Day 3 Architecture: Open Table Formats (Delta/Iceberg) & Forecast Time-Travel", fontsize=14, fontweight="bold", y=0.98)

    # Panel 1: Copy-on-Write & Transaction Log Architecture
    ax1.set_title("1. How Delta Lake / Iceberg Enables Time Travel on Object Storage", fontsize=11, fontweight="bold")
    ax1.axis("off")

    ax1.add_patch(FancyBboxPatch((0.04, 0.65), 0.92, 0.25, boxstyle="round,pad=0.02", facecolor="#fff2cc", edgecolor="#d6b656"))
    ax1.text(0.07, 0.82, "Transaction Log (_delta_log/ or Iceberg Metadata)", fontsize=10, fontweight="bold")
    ax1.text(0.07, 0.69, "• 000000.json (Version 0 - 00z Run)  --> Points to File A (part-00000.parquet)\n"
                         "• 000001.json (Version 1 - 12z Run)  --> Points to File B (part-00001.parquet)", fontsize=8.5, family="monospace")

    ax1.add_patch(FancyBboxPatch((0.04, 0.32), 0.43, 0.25, boxstyle="round,pad=0.02", facecolor="#cfe2f3", edgecolor="#0b5394"))
    ax1.text(0.07, 0.49, "File A: part-00000.parquet", fontsize=9.5, fontweight="bold")
    ax1.text(0.07, 0.36, "00z Morning Forecast\nImmutable! Kept on S3/GCS\nfor Time-Travel & Audit.", fontsize=8.5)

    ax1.add_patch(FancyBboxPatch((0.53, 0.32), 0.43, 0.25, boxstyle="round,pad=0.02", facecolor="#f4cccc", edgecolor="#990000"))
    ax1.text(0.56, 0.49, "File B: part-00001.parquet", fontsize=9.5, fontweight="bold")
    ax1.text(0.56, 0.36, "12z Storm Revision\nWritten via Copy-on-Write.\nZero dirty reads for traders!", fontsize=8.5)

    ax1.add_patch(FancyBboxPatch((0.04, 0.05), 0.92, 0.20, boxstyle="round,pad=0.02", facecolor="#d9ead3", edgecolor="#38761d"))
    ax1.text(0.07, 0.17, "dbt Core Transformation & DataOps Layer (Stage -> Test -> Mart)", fontsize=10, fontweight="bold", color="#274e13")
    ax1.text(0.07, 0.08, "Validates physical bounds (-50°C to +55°C, 870 to 1085 hPa) & builds Trading Desk Marts.", fontsize=8.5)

    # Panel 2: Plot Time-Travel Shift in Heating Degree Days (HDD)
    ax2.set_title("2. Time-Travel Impact: Daily Heating Degree Days (00z V0 vs. 12z V1)", fontsize=11, fontweight="bold")
    x = range(len(comp_df))
    w = 0.35
    b1 = ax2.bar([i - w/2 for i in x], comp_df["v0_daily_hdd"], width=w, label="Version 0 (00z Morning Run)", color="#6fa8dc")
    b2 = ax2.bar([i + w/2 for i in x], comp_df["v1_daily_hdd"], width=w, label="Version 1 (12z Cold Storm Update)", color="#e06666")

    ax2.set_xticks(list(x))
    ax2.set_xticklabels(comp_df["asset_id"], fontweight="bold")
    ax2.set_ylabel("Total Daily Heating Degree Days (HDD)")
    ax2.grid(axis="y", linestyle="--", alpha=0.4)
    ax2.legend()

    for bar in list(b1) + list(b2):
        h = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2, h + 0.03, f"{h:.2f}", ha="center", fontsize=8.5, fontweight="bold")

    plt.tight_layout(rect=[0, 0, 1, 0.94])
    plt.savefig(out_img)
    plt.close(fig)
    print(f"[SUCCESS] Saved Day 3 Time-Travel visual to: {out_img.resolve()}")

if __name__ == "__main__":
    run_lakehouse_time_travel()