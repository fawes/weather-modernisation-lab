"""
Day 2 Lab: NetCDF -> Chunked Zarr Store -> Partitioned Parquet Lakehouse -> DuckDB Serverless SQL
Target Audience: Data & Integration Architect (Weather & Energy Trading Domain)
"""

from pathlib import Path
import json
import shutil
import xarray as xr
import pandas as pd
import duckdb
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch

def run_day2_pipeline():
    nc_path = Path("data/raw/era5_uk_sample.nc")
    zarr_path = Path("data/zarr/era5_uk.zarr")
    lakehouse_dir = Path("data/lakehouse/uk_energy_forecasts")
    out_dir = Path("data/output")

    # Clean previous runs so our lab is idempotent
    if zarr_path.exists():
        shutil.rmtree(zarr_path)
    if lakehouse_dir.exists():
        shutil.rmtree(lakehouse_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # =========================================================================
    # STEP 1: Convert Monolithic NetCDF to Chunked Zarr Store
    # =========================================================================
    print("\n--- 1. CONVERTING MONOLITHIC NETCDF TO CHUNKED ZARR ---")
    ds = xr.open_dataset(nc_path, engine="netcdf4")

    # ARCHITECTURAL DECISION: Chunking Strategy
    # Our cube has shape (time: 24, latitude: 45, longitude: 49).
    # We chunk along time into 6-hour blocks: (6, 45, 49).
    # This creates 24 / 6 = 4 independent chunk files per variable!
    ds_chunked = ds.chunk({"time": 6, "latitude": 45, "longitude": 49})

    # Write to Zarr with consolidated=True (creates a single top-level .zmetadata JSON file
    # so cloud readers on S3/GCS/ADLS only need 1 HTTP GET to know the entire schema).
    ds_chunked.to_zarr(zarr_path, mode="w", consolidated=True, zarr_format=2)
    ds.close()
    print(f"[SUCCESS] Wrote chunked Zarr store to: {zarr_path}")

    # =========================================================================
    # STEP 2: Inspect the Physical Zarr Files on Disk (No Black Boxes!)
    # =========================================================================
    print("\n--- 2. INSPECTING INSIDE THE .zarr FOLDER ON DISK ---")
    all_zarr_files = sorted([str(p.relative_to(zarr_path)) for p in zarr_path.rglob("*") if p.is_file()])
    print("Files created inside era5_uk.zarr/:")
    for f in all_zarr_files:
        size_bytes = (zarr_path / f).stat().st_size
        print(f"  ├── {f:<22} ({size_bytes:,} bytes)")

    # Peek inside the plain-text .zarray JSON metadata for 't2m'
    t2m_zarray = json.loads((zarr_path / "t2m" / ".zarray").read_text())
    print("\nPlain-text JSON inside era5_uk.zarr/t2m/.zarray:")
    print(f"  • Full Cube Shape: {t2m_zarray['shape']}")
    print(f"  • Chunk Shape:     {t2m_zarray['chunks']}  <-- 4 chunks of (6 hours x 45 lats x 49 lons)")
    print(f"  • Compressor:      {t2m_zarray['compressor']}")
    print(f"  • Data Type:       {t2m_zarray['dtype']}")

    # =========================================================================
    # STEP 3: Lazy Open from Zarr & Vectorized Multi-Asset Portfolio Extraction
    # =========================================================================
    # Imagine BP Energy Traders monitoring 5 critical UK energy infrastructure assets.
    # Instead of looping 5 times, Xarray lets us slice all 5 locations in ONE vectorized operation!
    print("\n--- 3. EXTRACTING UK ENERGY PORTFOLIO FROM ZARR ---")
    ds_zarr = xr.open_zarr(zarr_path, consolidated=True)

    portfolio = [
        {"asset_id": "UK-WIND-01", "name": "Dogger Bank Offshore Wind", "asset_type": "Offshore_Wind", "lat": 54.75, "lon": 1.90},
        {"asset_id": "UK-WIND-02", "name": "Moray East Offshore Wind",  "asset_type": "Offshore_Wind", "lat": 58.10, "lon": -2.80},
        {"asset_id": "UK-HUB-01",  "name": "London NBP Gas Demand Hub", "asset_type": "Trading_Hub",   "lat": 51.51, "lon": -0.13},
        {"asset_id": "UK-HUB-02",  "name": "Birmingham Midlands Hub",   "asset_type": "Trading_Hub",   "lat": 52.49, "lon": -1.89},
        {"asset_id": "UK-SOL-01",  "name": "Cleve Hill Solar Park",     "asset_type": "Solar_Park",    "lat": 51.34, "lon": 0.90},
    ]
    meta_df = pd.DataFrame(portfolio)

    # Create Xarray DataArrays for vectorized pointwise indexing
    target_lats = xr.DataArray(meta_df["lat"].values, dims="asset")
    target_lons = xr.DataArray(meta_df["lon"].values, dims="asset")

    # Vectorized nearest-neighbor extraction across all 5 assets simultaneously
    extracted_ds = ds_zarr.sel(latitude=target_lats, longitude=target_lons, method="nearest")

    # Convert to tabular Pandas DataFrame & enrich with asset metadata (Content Enricher EIP)
    raw_df = extracted_ds.to_dataframe().reset_index()
    enriched_df = raw_df.merge(
        meta_df,
        left_on="asset",
        right_index=True
    )

    # Calculate Energy Trading Metrics:
    # 1. Temperature in Celsius (t2m_c)
    # 2. Pressure in hPa (msl_hpa)
    # 3. Hourly Heating Degree Days (HDD) contribution: max(0, 15.5°C - temp_c) / 24
    #    (In UK energy trading, 15.5°C is the standard threshold where domestic gas heating turns on)
    enriched_df["temp_c"] = (enriched_df["t2m"] - 273.15).round(2)
    enriched_df["pressure_hpa"] = (enriched_df["msl"] / 100.0).round(1)
    enriched_df["hdd_hourly"] = ((15.5 - enriched_df["temp_c"]).clip(lower=0.0) / 24.0).round(4)

    final_cols = [
        "time", "asset_id", "name", "asset_type",
        "latitude", "longitude", "temp_c", "pressure_hpa", "hdd_hourly"
    ]
    lakehouse_df = enriched_df[final_cols]

    # Write as Hive-Partitioned Parquet Dataset (Partitioned by 'asset_type')
    # On AWS/GCP/Azure, this creates folders like: s3://bucket/uk_energy_forecasts/asset_type=Offshore_Wind/*.parquet
    lakehouse_df.to_parquet(
        lakehouse_dir,
        engine="pyarrow",
        compression="snappy",
        partition_cols=["asset_type"],
        index=False
    )
    print(f"[SUCCESS] Wrote Hive-Partitioned Parquet Lakehouse to: {lakehouse_dir}")
    for p in sorted(lakehouse_dir.rglob("*.parquet")):
        print(f"  ├── {p.relative_to(lakehouse_dir)}")

    # =========================================================================
    # STEP 4: Serverless SQL Analytics with DuckDB (Replacing Legacy Postgres)
    # =========================================================================
    # Notice: No PostgreSQL server, no CREATE TABLE, no COPY command!
    # DuckDB reads the Hive-partitioned Parquet directory directly using SQL,
    # just like Amazon Athena, GCP BigQuery (BigLake), or Azure Synapse Serverless.
    print("\n--- 4. SERVERLESS SQL ANALYTICS OVER PARQUET LAKEHOUSE (DUCKDB) ---")

    sql_query = f"""
    SELECT
        asset_type,
        asset_id,
        name,
        ROUND(MIN(temp_c), 2)          AS min_temp_c,
        ROUND(MAX(temp_c), 2)          AS max_temp_c,
        ROUND(AVG(temp_c), 2)          AS avg_temp_c,
        ROUND(SUM(hdd_hourly), 2)      AS total_daily_hdd,
        ROUND(MIN(pressure_hpa), 1)    AS min_pressure_hpa,
        CASE
            WHEN MIN(pressure_hpa) < 1010.0 THEN 'HIGH_WIND_ALERT'
            ELSE 'NORMAL'
        END                            AS trading_signal
    FROM read_parquet('{lakehouse_dir}/**/*.parquet', hive_partitioning = true)
    GROUP BY asset_type, asset_id, name
    ORDER BY asset_type, asset_id;
    """

    summary_df = duckdb.sql(sql_query).df()
    print("\nEnergy Trading Daily Weather Summary (Queried directly from Parquet via SQL):")
    duckdb.sql(sql_query).show()

    # =========================================================================
    # STEP 5: Generate Visual Architecture Diagram of Your Zarr & Lakehouse
    # =========================================================================
    create_day2_visual(zarr_path, summary_df, out_dir / "day2_zarr_and_lakehouse_visual.png")

def create_day2_visual(zarr_path: Path, summary_df: pd.DataFrame, out_img: Path):
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6.5), dpi=150)
    fig.suptitle("Day 2 Architecture: Zarr Chunk Structure & DuckDB Energy Trading Analytics", fontsize=14, fontweight="bold", y=0.98)

    # Panel 1: Zarr Physical Structure on Disk
    ax1.set_title("1. Inside 'era5_uk.zarr': Decoupled JSON Metadata + 4 Time Chunks", fontsize=11, fontweight="bold")
    ax1.axis("off")

    ax1.add_patch(FancyBboxPatch((0.04, 0.72), 0.92, 0.20, boxstyle="round,pad=0.02", facecolor="#fff2cc", edgecolor="#d6b656"))
    ax1.text(0.07, 0.84, "era5_uk.zarr/.zmetadata (Consolidated JSON Header)", fontsize=10, fontweight="bold")
    ax1.text(0.07, 0.76, "Read in 1 single HTTP GET! Tells Xarray exact grid coordinates, units & chunk layout.", fontsize=8.5)

    chunks = [
        ("t2m/0.0.0", "Hours 00:00 - 05:00\nShape: (6, 45, 49)", "#cfe2f3"),
        ("t2m/1.0.0", "Hours 06:00 - 11:00\nShape: (6, 45, 49)", "#cfe2f3"),
        ("t2m/2.0.0", "Hours 12:00 - 17:00\nShape: (6, 45, 49)", "#9fc5e8"),
        ("t2m/3.0.0", "Hours 18:00 - 23:00\nShape: (6, 45, 49)", "#cfe2f3"),
    ]
    for i, (cname, desc, col) in enumerate(chunks):
        x0 = 0.04 + i * 0.235
        ax1.add_patch(FancyBboxPatch((x0, 0.36), 0.215, 0.28, boxstyle="round,pad=0.02", facecolor=col, edgecolor="#0b5394"))
        ax1.text(x0 + 0.02, 0.55, cname, fontsize=9.5, fontweight="bold", family="monospace")
        ax1.text(x0 + 0.02, 0.41, desc, fontsize=8)

    ax1.add_patch(FancyBboxPatch((0.04, 0.06), 0.92, 0.22, boxstyle="round,pad=0.02", facecolor="#d9ead3", edgecolor="#38761d"))
    ax1.text(0.07, 0.20, "Partitioned Parquet Lakehouse (data/lakehouse/uk_energy_forecasts/)", fontsize=10, fontweight="bold", color="#274e13")
    ax1.text(0.07, 0.09, "├── asset_type=Offshore_Wind/part.parquet\n"
                         "├── asset_type=Solar_Park/part.parquet\n"
                         "└── asset_type=Trading_Hub/part.parquet   <-- Queried via DuckDB / Athena / BigQuery!", fontsize=8.5, family="monospace")

    # Panel 2: Plot Daily Min/Avg/Max Temp & Heating Degree Days per Asset
    ax2.set_title("2. UK Energy Portfolio Weather Summary (Queried via DuckDB SQL)", fontsize=11, fontweight="bold")
    x = range(len(summary_df))
    w = 0.28
    ax2.bar([i - w for i in x], summary_df["min_temp_c"], width=w, label="Min Temp (°C)", color="#3c78d8")
    ax2.bar(list(x), summary_df["avg_temp_c"], width=w, label="Avg Temp (°C)", color="#f1c232")
    ax2.bar([i + w for i in x], summary_df["max_temp_c"], width=w, label="Max Temp (°C)", color="#cc0000")

    ax2.axhline(15.5, color="black", linestyle="--", linewidth=1, label="15.5°C UK Heating Threshold (HDD)")
    ax2.set_xticks(list(x))
    ax2.set_xticklabels([f"{r['asset_id']}\n({r['asset_type']})" for _, r in summary_df.iterrows()], fontsize=8, fontweight="bold")
    ax2.set_ylabel("2m Temperature (°C)")
    ax2.grid(axis="y", linestyle="--", alpha=0.4)
    ax2.legend(loc="upper left", fontsize=8.5)

    plt.tight_layout(rect=[0, 0, 1, 0.94])
    plt.savefig(out_img)
    plt.close(fig)
    print(f"[SUCCESS] Saved Day 2 architectural diagram to: {out_img.resolve()}")

if __name__ == "__main__":
    run_day2_pipeline()