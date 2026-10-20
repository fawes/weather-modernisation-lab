"""
Day 1 Lab: NetCDF -> Xarray -> UK Weather Field Plotting & Point Extraction
Python 3.12 | Stack: xarray, netCDF4, numpy, pandas, matplotlib
"""

from pathlib import Path
import numpy as np
import pandas as pd
import xarray as xr
import matplotlib.pyplot as plt

def main():
    nc_path = Path("data/raw/era5_uk_sample.nc")
    out_dir = Path("data/output")
    out_dir.mkdir(parents=True, exist_ok=True)

    # =========================================================================
    # STEP 1: Open the NetCDF Dataset & Inspect Metadata
    # =========================================================================
    # Legacy BP way: nc = netCDF4.Dataset(nc_path, 'r') -> manual variable loops
    # Modern Xarray way: xr.open_dataset() reads metadata & coordinates automatically
    print("\n--- 1. OPENING ERA5 NETCDF DATASET WITH XARRAY ---")
    ds = xr.open_dataset(nc_path, engine="netcdf4")
    print(ds)

    print("\n--- BREAKING DOWN THE 4 CORE XARRAY COMPONENTS ---")
    print(f"1. Dimensions (Axis sizes):     {dict(ds.sizes)}")
    print(f"2. Coordinates (Axis labels):   {list(ds.coords.keys())}")
    print(f"3. Data Variables (Payloads):   {list(ds.data_vars.keys())}")
    print(f"4. Global Metadata Attributes:  {ds.attrs.get('source')}")
    print(f"   t2m Variable Attributes:     {ds['t2m'].attrs}")

    # =========================================================================
    # STEP 2: Vectorized Unit Conversion (Kelvin -> Celsius)
    # =========================================================================
    # ERA5 stores temperature in Kelvin. Xarray supports NumPy operations directly
    # while preserving all coordinate labels!
    print("\n--- 2. CONVERTING KELVIN TO CELSIUS ---")
    ds["t2m_c"] = ds["t2m"] - 273.15
    ds["t2m_c"].attrs = {
        "units": "°C",
        "long_name": "2 metre temperature (°C)",
    }

    # =========================================================================
    # STEP 3: Label-Based Slicing (.sel) vs Integer Indexing
    # =========================================================================
    # In legacy NumPy, you had to look up which array index corresponded to 15:00 UTC.
    # In Xarray, .sel() lets you query by human-readable timestamps and lat/lon!
    target_time = "2026-07-15T15:00:00"
    print(f"\n--- 3. SLICING UK TEMPERATURE AT {target_time} ---")

    uk_afternoon_temp = ds["t2m_c"].sel(time=target_time)
    print(f"Sliced DataArray shape: {uk_afternoon_temp.shape} (Dimensions: {uk_afternoon_temp.dims})")

    # Spatial Bounding Box Slice (e.g., Southern UK: 54°N down to 50°N, 6°W to 2°E)
    # Important ERA5 quirk: Because ERA5 latitude runs North->South (60 down to 49),
    # our latitude slice goes from 54.0 to 50.0.
    southern_uk = uk_afternoon_temp.sel(
        latitude=slice(54.0, 50.0),
        longitude=slice(-6.0, 2.0)
    )
    print(f"Southern UK regional mean temp at 15:00 UTC: {float(southern_uk.mean()):.2f} °C")

    # =========================================================================
    # STEP 4: Plotting the UK Weather Field (Xarray + Matplotlib)
    # =========================================================================
    # On the BP platform, you had to extract 2D NumPy grids (lat, lon, z) and manually
    # configure Matplotlib pcolormesh/contourf axes and colourbar labels.
    # Xarray's built-in .plot() automatically reads coordinate axes and 'units' metadata!
    print("\n--- 4. GENERATING UK 2M TEMPERATURE & PRESSURE MAP ---")
    fig, ax = plt.subplots(figsize=(9, 8), dpi=150)

    # Plot filled temperature contours/grid using Xarray's native Matplotlib wrapper
    uk_afternoon_temp.plot.pcolormesh(
        ax=ax,
        cmap="RdYlBu_r",
        cbar_kwargs={"label": "2m Temperature (°C)", "shrink": 0.85},
    )

    # Overlay Mean Sea Level Pressure (converted from Pa to hPa/mb) as contour lines
    msl_hpa = (ds["msl"].sel(time=target_time) / 100.0)
    cs = msl_hpa.plot.contour(
        ax=ax,
        levels=10,
        colors="black",
        linewidths=0.8,
        alpha=0.6,
    )
    ax.clabel(cs, inline=True, fontsize=8, fmt="%.0f hPa")

    # Mark key UK energy/business locations (London & Aberdeen)
    cities = {
        "London": (51.5074, -0.1278),
        "Birmingham": (52.4862, -1.8904),
        "Aberdeen": (57.1497, -2.0943),
    }
    for city, (lat, lon) in cities.items():
        ax.plot(lon, lat, marker="o", color="black", markersize=5)
        ax.text(lon + 0.2, lat + 0.1, city, fontsize=9, fontweight="bold", color="black")

    ax.set_title(f"ERA5 UK 2m Temperature (°C) & MSLP (hPa)\nValid: {target_time} UTC", fontsize=12, fontweight="bold")
    ax.set_xlabel("Longitude (°E)")
    ax.set_ylabel("Latitude (°N)")
    ax.grid(True, linestyle="--", alpha=0.4)

    map_output_path = out_dir / "uk_era5_t2m_map.png"
    plt.tight_layout()
    plt.savefig(map_output_path)
    plt.close(fig)
    print(f"[SUCCESS] Saved UK temperature map to: {map_output_path.resolve()}")

    # =========================================================================
    # STEP 5: Xarray -> Pandas Bridge (Extracting a City Forecast CSV)
    # =========================================================================
    # At BP, Airflow DAGs extracted city/regional forecasts into CSVs and PostgreSQL.
    # With Xarray, .sel(..., method="nearest") snaps to the closest 0.25° grid cell
    # and .to_dataframe() bridges cleanly into Pandas!
    print("\n--- 5. EXTRACTING LONDON 24-HOUR TIME SERIES TO PANDAS/CSV ---")
    london_ds = ds[["t2m_c", "msl"]].sel(
        latitude=51.5074,
        longitude=-0.1278,
        method="nearest"
    )

    # Convert the 1D time-series Xarray Dataset into a clean Pandas DataFrame
    london_df = london_ds.to_dataframe().reset_index()
    london_df["msl_hpa"] = (london_df["msl"] / 100.0).round(1)
    london_df["t2m_c"] = london_df["t2m_c"].round(2)

    csv_output_path = out_dir / "london_24h_era5_series.csv"
    london_df[["time", "latitude", "longitude", "t2m_c", "msl_hpa"]].to_csv(csv_output_path, index=False)

    print("Sample of extracted Pandas DataFrame (First 5 hours):")
    print(london_df[["time", "latitude", "longitude", "t2m_c", "msl_hpa"]].head())
    print(f"[SUCCESS] Saved London point time-series CSV to: {csv_output_path.resolve()}")

    # Close dataset file handle cleanly
    ds.close()

if __name__ == "__main__":
    main()