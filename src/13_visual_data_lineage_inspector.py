"""
Visual Internals & Data Lineage Inspector:
Traces how data physically moves and transforms from Source -> Intermediaries -> Destinations
across all 11 scripts, and generates a comprehensive visual flowchart in data/output/.
"""

from pathlib import Path
import json
import xarray as xr
import duckdb
from deltalake import DeltaTable
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch


def inspect_and_visualize_lineage():
    out_dir = Path("data/output")
    out_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 88)
    print("LIVE DATA LINEAGE TRACE: FOLLOWING 'UK-WIND-01' (DOGGER BANK) ACROSS ALL 6 STAGES")
    print("=" * 88)

    # 1. STAGE 1: Raw NetCDF4 Source
    ds_nc = xr.open_dataset("data/raw/era5_uk_sample.nc", engine="netcdf4")
    pt_nc = ds_nc.sel(latitude=54.75, longitude=1.90, method="nearest").isel(time=15)
    t_kelvin = float(pt_nc["t2m"].values)
    p_pascal = float(pt_nc["msl"].values)
    print(f"\n[HOP 1: RAW SOURCE] File: data/raw/era5_uk_sample.nc (Monolithic NetCDF4)")
    print(f"  • Cube Dimensions : {dict(ds_nc.sizes)} (Total cells = {ds_nc['t2m'].size:,})")
    print(f"  • Dogger Bank @15z: t2m = {t_kelvin:.2f} K | msl = {p_pascal:.1f} Pa (Raw SI Units)")
    ds_nc.close()

    # 2. STAGE 2A: Chunked Zarr Intermediary
    ds_zarr = xr.open_zarr("data/zarr/era5_uk.zarr", consolidated=True)
    print(f"\n[HOP 2A: INTERMEDIARY CUBE] Folder: data/zarr/era5_uk.zarr (Chunked Zarr v2)")
    print(f"  • Metadata Header : data/zarr/era5_uk.zarr/.zmetadata (Plain-text JSON)")
    print(f"  • Physical Chunks : 4 files per variable (0.0.0, 1.0.0, 2.0.0, 3.0.0) of shape {ds_zarr['t2m'].chunks}")

    # 3. STAGE 2B: Hive-Partitioned Parquet Intermediary
    parquet_row = duckdb.sql("""
        SELECT time, asset_id, asset_type, temp_c, pressure_hpa, hdd_hourly
        FROM read_parquet('data/lakehouse/uk_energy_forecasts/**/*.parquet', hive_partitioning=true)
        WHERE asset_id = 'UK-WIND-01' AND CAST(time AS VARCHAR) LIKE '%15:00:00%'
    """).df().to_dict(orient="records")[0]
    print(f"\n[HOP 2B: INTERMEDIARY PARQUET] Folder: data/lakehouse/uk_energy_forecasts/")
    print(f"  • Transformation  : Vectorized Xarray .sel() -> K to °C, Pa to hPa, HDD math")
    print(f"  • Dogger Bank @15z: {parquet_row}")

    # 4. STAGE 3A: ACID Delta Lake Time-Travel Intermediary
    dt_v0 = DeltaTable("data/lakehouse/uk_energy_delta", version=0).to_pyarrow_table()
    dt_v1 = DeltaTable("data/lakehouse/uk_energy_delta", version=1).to_pyarrow_table()
    delta_comp = duckdb.sql("""
        SELECT v0.asset_id,
               v0.temp_c AS v0_00z_temp_c, v1.temp_c AS v1_12z_temp_c,
               v0.pressure_hpa AS v0_press_hpa, v1.pressure_hpa AS v1_press_hpa,
               v0.hdd_hourly AS v0_hdd, v1.hdd_hourly AS v1_hdd
        FROM dt_v0 v0 JOIN dt_v1 v1 USING (asset_id, time)
        WHERE v0.asset_id = 'UK-WIND-01' AND CAST(v0.time AS VARCHAR) LIKE '%15:00:00%'
    """).df().to_dict(orient="records")[0]
    print(f"\n[HOP 3A: INTERMEDIARY DELTA LAKE] Folder: data/lakehouse/uk_energy_delta/_delta_log/")
    print(f"  • Version 0 -> 1  : Copy-on-Write commit (00000.json -> 00001.json)")
    print(f"  • Dogger Bank @15z: {delta_comp}")

    # 5. STAGE 3B: dbt Core Staging View -> Gold Mart Table in DuckDB
    con = duckdb.connect("data/lakehouse/weather_lakehouse.duckdb", read_only=True)
    mart_row = con.sql("""
        SELECT * FROM main.mart_uk_trading_desk_alerts WHERE asset_id = 'UK-WIND-01'
    """).df().to_dict(orient="records")[0]
    con.close()
    print(f"\n[HOP 3B: INTERMEDIARY DBT GOLD MART] DB: data/lakehouse/weather_lakehouse.duckdb")
    print(f"  • dbt Lineage     : raw_weather.* -> stg_uk_energy_forecasts (View) -> mart_uk_trading_desk_alerts (Table)")
    print(f"  • Aggregated Row  : {mart_row}")

    # 6. STAGE 4: Final Destinations (STAC, FastAPI OGC EDR, Kafka Outbox JSONs)
    sap_msgs = json.loads(Path("data/events_outbox/kafka_topic_sap_offshore_halt.json").read_text())
    sfdc_msgs = json.loads(Path("data/events_outbox/kafka_topic_salesforce_trading_alerts.json").read_text())
    uk_wind_event = [m for m in sfdc_msgs if m["kafka_partition_key"] == "UK-WIND-01"][0]
    print(f"\n[HOP 4: FINAL DESTINATIONS] APIs & Enterprise Kafka Event Streams")
    print(f"  • Dest A (STAC)   : data/stac/catalog.json (Indexes era5-uk & north-sea-wave)")
    print(f"  • Dest B (FastAPI): GET /collections/era5-uk/position & GET /api/v1/trading-alerts")
    print(f"  • Dest C (SAP PM) : {len(sap_msgs)} CloudEvents in kafka_topic_sap_offshore_halt.json (>=5m waves)")
    print(f"  • Dest D (SFDC)   : Dogger Bank Kafka Payload -> {uk_wind_event['data']}")
    print("=" * 88)

    # Render High-Resolution Visual Architecture & Data-Movement Map
    render_master_lineage_diagram(t_kelvin, p_pascal, parquet_row, delta_comp, mart_row, uk_wind_event, sap_msgs[0], out_dir / "master_data_movement_lineage.png")


def draw_stage_box(ax, x, y, w, h, bg_color, border_color, badge_text, title, subtitle, payload_text):
    # Main card
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.012", facecolor=bg_color, edgecolor=border_color, linewidth=2))
    # Header bar
    ax.add_patch(FancyBboxPatch((x, y + h - 0.045), w, 0.045, boxstyle="round,pad=0.005", facecolor=border_color, edgecolor=border_color, linewidth=1))
    ax.text(x + 0.012, y + h - 0.030, f"{badge_text}  |  {title}", fontsize=9.5, fontweight="bold", color="white")
    ax.text(x + 0.012, y + h - 0.072, subtitle, fontsize=8.2, fontweight="bold", color="#222222")
    ax.text(x + 0.012, y + 0.014, payload_text, fontsize=7.8, family="monospace", color="#111111")


def draw_arrow(ax, x1, y1, x2, y2, label=""):
    ax.annotate(
        "", xy=(x2, y2), xytext=(x1, y1),
        arrowprops=dict(arrowstyle="-|>,head_width=0.4,head_length=0.6", lw=2.0, color="#333333")
    )
    if label:
        mx, my = (x1 + x2) / 2, (y1 + y2) / 2
        ax.text(mx + 0.01, my, label, fontsize=8, fontweight="bold", color="#0b5394",
                bbox=dict(boxstyle="round,pad=0.15", facecolor="white", edgecolor="#0b5394", alpha=0.9))


def render_master_lineage_diagram(t_k, p_pa, pq_row, d_comp, m_row, sfdc_ev, sap_ev, out_img: Path):
    fig, ax = plt.subplots(figsize=(18, 12), dpi=160)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")

    fig.suptitle(
        "MASTER ARCHITECTURE & DATA MOVEMENT MAP: SOURCE -> INTERMEDIARIES -> DESTINATIONS\n"
        "(Tracing Actual Live Data Payloads Across Scripts 01 to 11 in Your WSL2 Lab)",
        fontsize=14, fontweight="bold", y=0.98
    )

    # ROW 1: STAGE 1 (RAW SOURCE) & STAGE 3.5 (MARINE SOURCE)
    draw_stage_box(
        ax, 0.03, 0.79, 0.44, 0.16, "#fce5cd", "#b45f06",
        "SOURCE 1 (Script 01)", "Monolithic NetCDF4 Grid (data/raw/era5_uk_sample.nc)",
        "Engine: netCDF4 / HDF5  |  Shape: (24h x 45 lat x 49 lon = 52,920 cells)",
        f"• Physical State : 3D Binary Arrays on POSIX Disk (SI Units)\n"
        f"• Sample Cell    : Lat=54.75N, Lon=1.75E @ 15:00 UTC\n"
        f"• Raw Values     : t2m = {t_k:.2f} Kelvin  |  msl = {p_pa:.1f} Pascals"
    )

    draw_stage_box(
        ax, 0.53, 0.79, 0.44, 0.16, "#d0e0e3", "#134f5c",
        "SOURCE 2 (Script 08)", "North Sea Marine Wave Cube (data/zarr/north_sea_wave.zarr)",
        "Engine: Xarray + Zarr + Cartopy  |  Variables: swh (Wave Height), u10, v10, msl",
        f"• Physical State : Chunked Zarr Cube + north_sea_wave_alerts.parquet\n"
        f"• Hazard Logic   : Flags Offshore Rigs where Significant Wave Height >= 5.0m\n"
        f"• Peak Breach    : {sap_ev['data']['sap_plant_code']} ({sap_ev['data']['facility_name']}) = {sap_ev['data']['peak_wave_height_m']}m SWH"
    )

    # ROW 2: INTERMEDIARY 1 (ZARR CUBE) & INTERMEDIARY 2 (HIVE PARQUET)
    draw_stage_box(
        ax, 0.03, 0.56, 0.44, 0.16, "#fff2cc", "#bf9000",
        "INTERMEDIARY 1 (Script 05 - Part A)", "Cloud-Optimized Zarr Cube (data/zarr/era5_uk.zarr/)",
        "Engine: Xarray + Dask .chunk(time=6) -> .to_zarr(consolidated=True, zarr_format=2)",
        "• Physical State : Plain-text .zmetadata JSON + 4 binary chunks per variable\n"
        "• Chunk Files    : t2m/0.0.0 (00-05z), 1.0.0 (06-11z), 2.0.0 (12-17z), 3.0.0 (18-23z)\n"
        "• Why It Exists  : Allows HTTP clients to slice 1 chunk without reading the whole file!"
    )

    draw_stage_box(
        ax, 0.53, 0.56, 0.44, 0.16, "#cfe2f3", "#0b5394",
        "INTERMEDIARY 2 (Script 05 - Part B)", "Hive-Partitioned Parquet (data/lakehouse/uk_energy_forecasts/)",
        "Engine: Xarray Vectorized .sel() -> Pandas Content Enricher -> PyArrow Parquet",
        f"• Physical State : Columnar binary folders (asset_type=Offshore_Wind/part.parquet)\n"
        f"• Extracted Row  : asset_id='{pq_row['asset_id']}' @ 15:00 UTC\n"
        f"• Transformed    : temp_c = {pq_row['temp_c']}°C | pressure = {pq_row['pressure_hpa']} hPa | hdd = {pq_row['hdd_hourly']}"
    )

    # ROW 3: INTERMEDIARY 3 (ACID DELTA LAKE) & INTERMEDIARY 4 (DBT GOLD MART IN DUCKDB)
    draw_stage_box(
        ax, 0.03, 0.32, 0.44, 0.17, "#ead1dc", "#741b47",
        "INTERMEDIARY 3 (Script 06)", "ACID Delta Lake Table (data/lakehouse/uk_energy_delta/)",
        "Engine: Rust deltalake (Copy-on-Write Versioning) + DuckDB Time-Travel",
        f"• _delta_log/00000.json (V0 00z Run) : temp={d_comp['v0_00z_temp_c']}°C, press={d_comp['v0_press_hpa']} hPa\n"
        f"• _delta_log/00001.json (V1 12z Run) : temp={d_comp['v1_12z_temp_c']}°C, press={d_comp['v1_press_hpa']} hPa\n"
        f"• Registered into DuckDB             : raw_weather.uk_energy_forecasts_v0 & _latest"
    )

    draw_stage_box(
        ax, 0.53, 0.32, 0.44, 0.17, "#d9ead3", "#274e13",
        "INTERMEDIARY 4 (Script 07 -> dbt Core)", "DuckDB Gold Mart (weather_lakehouse.duckdb)",
        "Engine: dbt-duckdb (stg_uk_energy_forecasts VIEW -> 8 DataOps Tests -> Gold MART Table)",
        f"• Circuit Breaker: 8/8 dbt tests passed (Bounds: -50..+55°C, 870..1085 hPa)\n"
        f"• Gold Mart Table: main.mart_uk_trading_desk_alerts (1 summary row per asset)\n"
        f"• UK-WIND-01 Row : HDD 00z={m_row['total_hdd_00z']} -> 12z={m_row['total_hdd_12z']} (+{m_row['hdd_revision_delta']}) | {m_row['risk_level']}"
    )

    # ROW 4: FINAL DESTINATIONS (SYNCHRONOUS OGC API + ASYNCHRONOUS KAFKA TOPICS)
    draw_stage_box(
        ax, 0.03, 0.05, 0.30, 0.19, "#e6cff2", "#4a154b",
        "DESTINATION 1 (Script 09)", "STAC + FastAPI OGC EDR Server",
        "Containerized in Docker behind Layer-7 ALB",
        "• GET /stac/collections\n"
        "  (Reads data/stac/catalog.json)\n"
        "• GET /collections/{id}/position\n"
        "  (Slices Zarr cubes live in RAM!)\n"
        "• GET /api/v1/trading-alerts\n"
        "  (Queries dbt Gold Mart in DuckDB)"
    )

    draw_stage_box(
        ax, 0.35, 0.05, 0.30, 0.19, "#f4cccc", "#990000",
        "DESTINATION 2 (Script 10)", "Kafka Topic: SAP Offshore Halt",
        "File: kafka_topic_sap_offshore_halt.json",
        f"• CloudEvents v1.0 Envelope\n"
        f"• Topic : sap.pm.offshore_work_order_halt\n"
        f"• Key   : '{sap_ev['kafka_partition_key']}' (Strict order)\n"
        f"• Wave  : {sap_ev['data']['peak_wave_height_m']}m ({sap_ev['data']['breach_hours']}h >= 5.0m)\n"
        f"• Action: {sap_ev['data']['sap_action']}"
    )

    draw_stage_box(
        ax, 0.67, 0.05, 0.30, 0.19, "#d9d2e9", "#351c75",
        "DESTINATION 3 (Script 10)", "Kafka Topic: Salesforce Trading",
        "File: kafka_topic_salesforce_trading_alerts.json",
        f"• CloudEvents v1.0 Envelope\n"
        f"• Topic : salesforce.crm.energy_trading_alert\n"
        f"• Key   : '{sfdc_ev['kafka_partition_key']}'\n"
        f"• Delta : +{sfdc_ev['data']['hdd_surge_delta']} Extra Gas HDD\n"
        f"• Signal: {sfdc_ev['data']['trading_recommendation']}"
    )

    # Draw Connectors Showing Exact Data Movement
    draw_arrow(ax, 0.25, 0.79, 0.25, 0.72, "xr.to_zarr(chunk=6)")
    draw_arrow(ax, 0.47, 0.64, 0.53, 0.64, "Vectorized .sel(5 assets)")
    draw_arrow(ax, 0.75, 0.56, 0.25, 0.49, "write_deltalake(V0 & V1)")
    draw_arrow(ax, 0.47, 0.40, 0.53, 0.40, "dbt run + dbt test")
    draw_arrow(ax, 0.18, 0.56, 0.18, 0.24, "Live Zarr Slice")
    draw_arrow(ax, 0.65, 0.32, 0.25, 0.24, "DuckDB SQL")
    draw_arrow(ax, 0.75, 0.79, 0.50, 0.24, "Wave >= 5.0m Filter")
    draw_arrow(ax, 0.78, 0.32, 0.82, 0.24, "HDD Delta > 0.3 Filter")

    plt.tight_layout(rect=[0, 0, 1, 0.94])
    plt.savefig(out_img)
    plt.close(fig)
    print(f"\n[VISUAL GENERATED] Open this file in VS Code to see the full Source -> Intermediary -> Destination map:")
    print(f"  --> {out_img.resolve()}")


if __name__ == "__main__":
    inspect_and_visualize_lineage()