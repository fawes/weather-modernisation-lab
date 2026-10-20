import xarray as xr

ds = xr.open_dataset("data/raw/era5_uk_sample.nc", engine="netcdf4")

print("=== 1. THE COORDINATE RULERS (1D Arrays) ===")
print(f"Time Ruler ({len(ds.time)} steps):      {ds.time.values[:3]} ... (Hourly)")
print(f"Latitude Ruler ({len(ds.latitude)} pts):  {ds.latitude.values[:5]} ... (Step: -0.25°)")
print(f"Longitude Ruler ({len(ds.longitude)} pts): {ds.longitude.values[:5]} ... (Step: +0.25°)")

print("\n=== 2. THE 3D TEMPERATURE CUBE (t2m) ===")
print(f"Cube Shape (Time, Lat, Lon): {ds['t2m'].shape}")
print(f"Total Grid Cells in Cube:    {ds['t2m'].size:,} (24 hours x 45 lats x 49 lons)")

print("\n=== 3. HORIZONTAL CUT (1 Hour, All UK Lat/Lons -> 2D Map) ===")
horizontal_cut = ds["t2m"].sel(time="2026-07-15T15:00:00")
print(f"Shape after picking 1 time:  {horizontal_cut.shape} -> {horizontal_cut.dims}")

print("\n=== 4. VERTICAL DRILL (All 24 Hours, 1 Single Lat/Lon -> 1D Time Series) ===")
vertical_drill = ds["t2m"].sel(latitude=52.5, longitude=-1.89, method="nearest")
print(f"Snapped to Birmingham Grid Point: Lat={float(vertical_drill.latitude)}°N, Lon={float(vertical_drill.longitude)}°E")
print(f"Shape after picking 1 point: {vertical_drill.shape} -> {vertical_drill.dims}")