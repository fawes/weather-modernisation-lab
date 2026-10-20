"""
Day 3.5 Bonus Lab: North Sea Marine & Wave Hazard Intelligence Pipeline
Replicates & Modernizes the BP North Sea Wave Alert Pipeline (Significant Wave Height >= 5.0m)
Stack: Xarray, Zarr, Dask, DuckDB, Matplotlib, Cartopy
"""

from pathlib import Path
import shutil
import numpy as np
import pandas as pd
import xarray as xr
import duckdb
import matplotlib.pyplot as plt

# Optional Cartopy import for real UK & North Sea coastlines
try:
    import cartopy.crs as ccrs
    import cartopy.feature as cfeature
    HAS_CARTOPY = True
except ImportError:
    HAS_CARTOPY = False


def build_north_sea_wave_zarr(zarr_path: Path):
    """
    Simulates an ECMWF Marine/Wave (WAM/ERA5) dataset over the UK & North Sea:
      - Bounding Box: 50°N to 62°N, 6°W (-6.0) to 8°E (8.0) at 0.25° resolution
      - Variables:
          swh : Significant height of combined wind waves and swell (m)
          u10 : 10m U-wind component (m/s)
          v10 : 10m V-wind component (m/s)
          msl : Mean sea level pressure (hPa)
    """
    if zarr_path.exists():
        shutil.rmtree(zarr_path)

    lats = np.arange(62.0, 49.75, -0.25, dtype=np.float32)  # 49 lat points
    lons = np.arange(-6.0, 8.25, 0.25, dtype=np.float32)    # 57 lon points
    times = pd.date_range("2026-10-20 00:00", periods=24, freq="1h")

    t_idx = np.arange(24)[:, None, None]
    lat_g = lats[None, :, None]
    lon_g = lons[None, None, :]

    # Storm center moves East-Southeast across the Northern/Central North Sea, peaking at 15:00 UTC
    storm_lat = 59.5 - 0.12 * t_idx
    storm_lon = -1.5 + 0.22 * t_idx
    intensity = np.sin(np.pi * t_idx / 24.0) ** 1.2  # Peaks mid-afternoon (12z-16z)

    dist_sq = ((lat_g - storm_lat) / 2.8) ** 2 + ((lon_g - storm_lon) / 4.2) ** 2

    # 1. Significant Wave Height (swh in meters): Background 1.8m, Storm core peaks at ~7.8m (> 5.0m threshold!)
    swh = 1.6 + (6.4 * intensity) * np.exp(-dist_sq)
    # Dampen waves slightly near Southern English Channel
    swh = swh - 0.4 * np.exp(-((lat_g - 51.0) ** 2) / 3.0)
    rng = np.random.default_rng(seed=101)
    swh = np.clip(swh + rng.normal(0, 0.12, size=swh.shape), 0.5, 12.0).astype(np.float32)

    # 2. Cyclonic (Counter-Clockwise) 10m Wind Vectors (u10, v10 in m/s) around Low Pressure Center
    dx = (lon_g - storm_lon) * np.cos(np.deg2rad(lat_g))
    dy = lat_g - storm_lat
    wind_mag = 8.0 + (22.0 * intensity) * np.exp(-dist_sq * 0.85)
    angle = np.arctan2(dy, dx) + (np.pi / 2.0)  # Counter-clockwise circulation
    u10 = (wind_mag * np.cos(angle)).astype(np.float32)
    v10 = (wind_mag * np.sin(angle)).astype(np.float32)

    # 3. Mean Sea Level Pressure (hPa): Deep 972 hPa low in the storm center
    msl = (1014.0 - (42.0 * intensity) * np.exp(-dist_sq * 0.9)).astype(np.float32)

    ds = xr.Dataset(
        data_vars={
            "swh": (["time", "latitude", "longitude"], swh, {"units": "m", "long_name": "Significant height of combined wind waves and swell"}),
            "u10": (["time", "latitude", "longitude"], u10, {"units": "m s**-1", "long_name": "10 metre U wind component"}),
            "v10": (["time", "latitude", "longitude"], v10, {"units": "m s**-1", "long_name": "10 metre V wind component"}),
            "msl": (["time", "latitude", "longitude"], msl, {"units": "hPa", "long_name": "Mean sea level pressure"}),
        },
        coords={
            "time": times,
            "latitude": (["latitude"], lats, {"units": "degrees_north"}),
            "longitude": (["longitude"], lons, {"units": "degrees_east"}),
        },
        attrs={
            "Conventions": "CF-1.7",
            "title": "UK & North Sea Marine Wave & Wind Forecast (ECMWF WAM / ERA5 Structure)",
            "alert_threshold_swh_m": 5.0,
        },
    )

    ds.chunk({"time": 6, "latitude": 49, "longitude": 57}).to_zarr(
        zarr_path, mode="w", consolidated=True, zarr_format=2
    )
    print(f"[SUCCESS] Created North Sea Wave & Wind Zarr Cube at: {zarr_path}")


def run_north_sea_wave_pipeline():
    zarr_path = Path("data/zarr/north_sea_wave.zarr")
    out_dir = Path("data/output")
    lakehouse_dir = Path("data/lakehouse")
    out_dir.mkdir(parents=True, exist_ok=True)
    lakehouse_dir.mkdir(parents=True, exist_ok=True)

    # 1. Build the North Sea Zarr Store
    build_north_sea_wave_zarr(zarr_path)

    # 2. Open Lazily via Xarray & Compute Vectorized Wind Speed (m/s and knots)
    ds = xr.open_zarr(zarr_path, consolidated=True)
    ds["wind_speed_ms"] = np.sqrt(ds["u10"] ** 2 + ds["v10"] ** 2)
    ds["wind_speed_ms"].attrs = {"units": "m/s", "long_name": "10m Wind Speed"}

    # 3. Define Key North Sea Offshore Energy Assets (Platforms, Wind Farms & Heliport)
    offshore_assets = [
        {"asset_id": "NS-PLAT-01", "name": "Clair Ridge Platform (West of Shetland)", "lat": 60.70, "lon": -2.50},
        {"asset_id": "NS-PLAT-02", "name": "Forties Charlie Platform (Central NS)",   "lat": 57.72, "lon": 0.88},
        {"asset_id": "NS-PLAT-03", "name": "Ekofisk Complex (Norwegian Sector)",      "lat": 56.54, "lon": 3.21},
        {"asset_id": "NS-WIND-01", "name": "Moray East Offshore Wind",                "lat": 58.15, "lon": -2.80},
        {"asset_id": "NS-WIND-02", "name": "Dogger Bank Offshore Wind",               "lat": 54.75, "lon": 1.90},
        {"asset_id": "NS-HELI-01", "name": "Aberdeen Offshore Heliport",              "lat": 57.20, "lon": -2.20},
    ]
    assets_df = pd.DataFrame(offshore_assets)

    # 4. Vectorized Point Extraction Across All 6 North Sea Assets
    target_lats = xr.DataArray(assets_df["lat"].values, dims="asset")
    target_lons = xr.DataArray(assets_df["lon"].values, dims="asset")
    extracted = ds.sel(latitude=target_lats, longitude=target_lons, method="nearest").to_dataframe().reset_index()

    merged_df = extracted.merge(assets_df, left_on="asset", right_index=True)
    merged_df["swh_m"] = merged_df["swh"].round(2)
    merged_df["wind_ms"] = merged_df["wind_speed_ms"].round(1)
    merged_df["msl_hpa"] = merged_df["msl"].round(1)

    # Save to Parquet & Run DuckDB Alert Query for the 5.0m Wave Height Threshold
    alerts_parquet = lakehouse_dir / "north_sea_wave_alerts.parquet"
    merged_df.to_parquet(alerts_parquet, index=False)

    alert_sql = f"""
    SELECT
        asset_id,
        name,
        ROUND(MAX(swh_m), 2)                                    AS peak_wave_m,
        ROUND(MAX(wind_ms), 1)                                  AS peak_wind_ms,
        ROUND(MIN(msl_hpa), 1)                                  AS min_pressure_hpa,
        COUNT(*) FILTER (WHERE swh_m >= 5.0)                    AS hours_above_5m,
        MIN(time) FILTER (WHERE swh_m >= 5.0)                   AS alert_start_utc,
        CASE
            WHEN MAX(swh_m) >= 5.0 THEN 'RED_ALERT: SUSPEND_HELI_AND_VESSEL_OPS'
            WHEN MAX(swh_m) >= 3.5 THEN 'AMBER_WARNING: STANDBY_LIMITS'
            ELSE 'GREEN: SAFE_TO_OPERATE'
        END                                                     AS marine_ops_status
    FROM read_parquet('{alerts_parquet}')
    GROUP BY asset_id, name
    ORDER BY peak_wave_m DESC;
    """
    print("\n--- NORTH SEA MARINE WAVE ALERT SUMMARY (THRESHOLD: SWH >= 5.0m) ---")
    duckdb.sql(alert_sql).show()
    summary_df = duckdb.sql(alert_sql).df()

    # 5. Render the 2-Panel North Sea Wave & Wind Hazard Map at Peak Storm Hour (15:00 UTC)
    target_time = "2026-10-20T15:00:00"
    snap = ds.sel(time=target_time)
    snap_assets = merged_df[merged_df["time"] == pd.Timestamp(target_time)]

    fig = plt.figure(figsize=(16, 7.5), dpi=150)
    fig.suptitle(
        f"North Sea Marine Weather Intelligence: Significant Wave Height (SWH) & 5.0m Alert Boundary\n"
        f"Valid: {target_time} UTC | Data Source: Consolidated Zarr Cube -> Xarray + DuckDB Alert Engine",
        fontsize=13, fontweight="bold", y=0.98
    )

    # Left Panel: Spatial Wave Height Map + 5.0m Danger Contour + Wind Vectors
    if HAS_CARTOPY:
        ax1 = fig.add_subplot(1, 2, 1, projection=ccrs.PlateCarree())
        ax1.set_extent([-6.0, 8.0, 50.0, 62.0], crs=ccrs.PlateCarree())
        ax1.add_feature(cfeature.LAND, facecolor="#e6e6e6", zorder=2)
        ax1.add_feature(cfeature.COASTLINE, linewidth=1.1, edgecolor="black", zorder=3)
        ax1.add_feature(cfeature.BORDERS, linestyle=":", alpha=0.6, zorder=3)
        gl = ax1.gridlines(draw_labels=True, linestyle="--", alpha=0.4)
        gl.top_labels = False
        gl.right_labels = False
    else:
        ax1 = fig.add_subplot(1, 2, 1)
        ax1.set_xlabel("Longitude (°E)")
        ax1.set_ylabel("Latitude (°N)")
        ax1.grid(True, linestyle="--", alpha=0.4)

    # Base heatmap: Significant Wave Height (m)
    pcm = ax1.pcolormesh(
        snap.longitude, snap.latitude, snap["swh"],
        cmap="turbo", vmin=1.0, vmax=8.0, shading="auto", zorder=1
    )
    cbar = plt.colorbar(pcm, ax=ax1, shrink=0.82, pad=0.03)
    cbar.set_label("Significant Wave Height - SWH (meters)", fontweight="bold")

    # Highlight the 5.0m Alert Threshold Boundary with a bold white/black contour + hatching
    cs_alert = ax1.contour(
        snap.longitude, snap.latitude, snap["swh"],
        levels=[5.0], colors=["white"], linewidths=2.5, zorder=4
    )
    ax1.contour(
        snap.longitude, snap.latitude, snap["swh"],
        levels=[5.0], colors=["#b20000"], linewidths=1.5, linestyles="--", zorder=4
    )
    ax1.clabel(cs_alert, fmt="5.0m ALERT ZONE", fontsize=9)

    # Overlay subsampled 10m Wind Vectors (Quiver arrows every 3rd grid cell so map stays clean)
    step = 3
    q = ax1.quiver(
        snap.longitude[::step], snap.latitude[::step],
        snap["u10"][::step, ::step], snap["v10"][::step, ::step],
        color="white", alpha=0.75, scale=350, width=0.0028, zorder=4
    )

    # Plot the 6 Offshore Assets (Red Triangle if SWH >= 5.0m, Green Circle if < 5.0m)
    for _, row in snap_assets.iterrows():
        is_breach = row["swh_m"] >= 5.0
        marker = "^" if is_breach else "o"
        color = "#ff1a1a" if is_breach else "#00e640"
        ax1.plot(
            row["lon"], row["lat"], marker=marker, color=color,
            markersize=10, markeredgecolor="black", markeredgewidth=1.2, zorder=5
        )
        ax1.text(
            row["lon"] + 0.22, row["lat"] + 0.12,
            f"{row['asset_id']}\n({row['swh_m']:.1f}m)",
            fontsize=8, fontweight="bold", color="white",
            bbox=dict(boxstyle="round,pad=0.18", facecolor="black", alpha=0.72),
            zorder=6
        )

    ax1.set_title("1. North Sea Wave Field, Wind Vectors & 5.0m Breach Zone", fontsize=11, fontweight="bold")

    # Right Panel: 24-Hour Wave Height Time Series per Asset vs. 5.0m Operational Limit
    ax2 = fig.add_subplot(1, 2, 2)
    ax2.set_title("2. 24-Hour Significant Wave Height Forecast by Offshore Asset", fontsize=11, fontweight="bold")

    for asset_id, grp in merged_df.groupby("asset_id"):
        peak_val = grp["swh_m"].max()
        lw = 2.6 if peak_val >= 5.0 else 1.5
        ls = "-" if peak_val >= 5.0 else "--"
        ax2.plot(
            pd.to_datetime(grp["time"]).dt.strftime("%H:%M"),
            grp["swh_m"],
            label=f"{asset_id}: {grp['name'].iloc[0][:24]} (Peak: {peak_val:.1f}m)",
            linewidth=lw, linestyle=ls
        )

    # Draw the 5.0m Red Alert Threshold Line & Shaded Danger Zone
    ax2.axhline(5.0, color="#cc0000", linewidth=2.2, linestyle="-", label="5.0m Helicopter/Vessel Shutdown Limit")
    ax2.axhspan(5.0, 8.5, color="#ffcccc", alpha=0.35, label="Marine Ops Suspended Zone (>= 5.0m)")

    ax2.set_xlabel("Forecast Hour (UTC)")
    ax2.set_ylabel("Significant Wave Height - SWH (m)")
    ax2.set_ylim(1.0, 8.2)
    ax2.set_xticks(range(0, 24, 3))
    ax2.grid(True, linestyle="--", alpha=0.4)
    ax2.legend(loc="upper left", fontsize=8.2)

    out_map = out_dir / "north_sea_wave_5m_alert_map.png"
    plt.tight_layout(rect=[0, 0, 1, 0.93])
    plt.savefig(out_map)
    plt.close(fig)
    print(f"[SUCCESS] Saved North Sea 5m Wave Hazard Map to: {out_map.resolve()}")


if __name__ == "__main__":
    run_north_sea_wave_pipeline()