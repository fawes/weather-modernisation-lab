"""
Day 1 Helper: Generates a CF-compliant ERA5-structured NetCDF4 file for the UK region.
Grid: 0.25° resolution (matching real ECMWF ERA5 reanalysis)
Bounding Box: 60°N to 49°N, 10°W (-10.0) to 2°E (2.0)
Variables: t2m (2m temperature in Kelvin), msl (Mean sea level pressure in Pa)
"""

from pathlib import Path
import numpy as np
import pandas as pd
import xarray as xr

def create_era5_uk_sample():
    raw_dir = Path("data/raw")
    raw_dir.mkdir(parents=True, exist_ok=True)
    output_file = raw_dir / "era5_uk_sample.nc"

    # 1. Define standard ERA5 0.25-degree coordinates for the UK
    # Note: ERA5 stores latitude from North to South (60.0 down to 49.0)
    lats = np.arange(60.0, 48.75, -0.25, dtype=np.float32)   # 45 grid points
    lons = np.arange(-10.0, 2.25, 0.25, dtype=np.float32)    # 49 grid points
    times = pd.date_range("2026-07-15 00:00", periods=24, freq="1h")

    # Create 3D coordinate meshes for realistic synthetic weather fields
    time_idx = np.arange(24)[:, None, None]
    lat_grid = lats[None, :, None]
    lon_grid = lons[None, None, :]

    # 2. Simulate realistic UK 2m Temperature in Kelvin (t2m)
    # Base ~288K (15°C) + warmer in South + diurnal cycle peaking at 15:00 UTC + SE England warm core
    base_temp_k = 285.15 + (60.0 - lat_grid) * 0.65
    diurnal_wave = 4.5 * np.sin((time_idx - 9) * np.pi / 12.0)
    uk_land_warmth = 3.5 * np.exp(-((lat_grid - 52.0)**2 / 8.0 + (lon_grid + 1.0)**2 / 6.0))
    rng = np.random.default_rng(seed=42)
    noise = rng.normal(0, 0.3, size=(len(times), len(lats), len(lons)))

    t2m_data = (base_temp_k + diurnal_wave + uk_land_warmth + noise).astype(np.float32)

    # 3. Simulate Mean Sea Level Pressure in Pascals (msl)
    msl_data = (101500.0 - (lat_grid - 50.0) * 120.0 + 300.0 * np.cos(lon_grid * 0.3)).astype(np.float32)
    msl_data = np.broadcast_to(msl_data, t2m_data.shape).copy()

    # 4. Construct the Xarray Dataset with authentic ECMWF ERA5 metadata
    ds = xr.Dataset(
        data_vars={
            "t2m": (
                ["time", "latitude", "longitude"],
                t2m_data,
                {
                    "units": "K",
                    "long_name": "2 metre temperature",
                    "standard_name": "air_temperature",
                },
            ),
            "msl": (
                ["time", "latitude", "longitude"],
                msl_data,
                {
                    "units": "Pa",
                    "long_name": "Mean sea level pressure",
                    "standard_name": "air_pressure_at_mean_sea_level",
                },
            ),
        },
        coords={
            "time": times,
            "latitude": (
                ["latitude"],
                lats,
                {"units": "degrees_north", "long_name": "latitude"},
            ),
            "longitude": (
                ["longitude"],
                lons,
                {"units": "degrees_east", "long_name": "longitude"},
            ),
        },
        attrs={
            "Conventions": "CF-1.7",
            "institution": "European Centre for Medium-Range Weather Forecasts (Simulated Lab)",
            "source": "ERA5 Reanalysis (0.25x0.25 degree UK subset)",
            "history": "Created for Day 1 Weather Platform Modernisation Lab",
        },
    )

    # 5. Write to disk using the netCDF4 engine
    ds.to_netcdf(output_file, engine="netcdf4")
    print(f"[SUCCESS] Created ERA5 UK NetCDF file at: {output_file.resolve()}")

if __name__ == "__main__":
    create_era5_uk_sample()