"""
Day 4 (Part A): STAC Catalog Generation & FastAPI OGC API - Environmental Data Retrieval (EDR)
Replaces Legacy BP Apache Tomcat static websites with dynamic Zarr slicing & dbt Mart REST APIs.
"""

from pathlib import Path
import json
import re
import xarray as xr
import duckdb
from fastapi import FastAPI, HTTPException, Query
from fastapi.testclient import TestClient

# =============================================================================
# 1. BUILD STAC (SpatioTemporal Asset Catalog) METADATA FOR OUR ZARR CUBES
# =============================================================================
def generate_stac_catalog() -> Path:
    stac_dir = Path("data/stac")
    stac_dir.mkdir(parents=True, exist_ok=True)

    datasets = {
        "era5-uk": {
            "path": Path("data/zarr/era5_uk.zarr"),
            "title": "ECMWF ERA5 UK Surface Meteorology Cube (t2m, msl)",
            "description": "Chunked Zarr store covering UK bounding box at 0.25 degree resolution.",
        },
        "north-sea-wave": {
            "path": Path("data/zarr/north_sea_wave.zarr"),
            "title": "North Sea Marine Wave & Wind Hazard Cube (swh, u10, v10, msl)",
            "description": "Chunked Zarr store for North Sea offshore operations (5.0m SWH threshold).",
        },
    }

    catalog = {
        "type": "Catalog",
        "stac_version": "1.0.0",
        "id": "uk-weather-intelligence-stac",
        "title": "Modernized Weather Intelligence Platform STAC Catalog",
        "description": "Cloud-native SpatioTemporal Asset Catalog indexing Zarr weather cubes.",
        "links": [{"rel": "self", "href": "./catalog.json"}],
    }

    for item_id, meta in datasets.items():
        ds = xr.open_zarr(meta["path"], consolidated=True)
        min_lon, max_lon = float(ds.longitude.min()), float(ds.longitude.max())
        min_lat, max_lat = float(ds.latitude.min()), float(ds.latitude.max())
        start_time = str(ds.time.values[0])[:19] + "Z"
        end_time = str(ds.time.values[-1])[:19] + "Z"
        variables = {
            var: {
                "dimensions": list(ds[var].dims),
                "units": ds[var].attrs.get("units", "unknown"),
                "long_name": ds[var].attrs.get("long_name", var),
            }
            for var in ds.data_vars
        }
        ds.close()

        stac_item = {
            "stac_version": "1.0.0",
            "stac_extensions": ["https://stac-extensions.github.io/datacube/v2.2.0/schema.json"],
            "type": "Feature",
            "id": item_id,
            "bbox": [min_lon, min_lat, max_lon, max_lat],
            "geometry": {
                "type": "Polygon",
                "coordinates": [[
                    [min_lon, min_lat], [max_lon, min_lat],
                    [max_lon, max_lat], [min_lon, max_lat],
                    [min_lon, min_lat],
                ]],
            },
            "properties": {
                "title": meta["title"],
                "description": meta["description"],
                "start_datetime": start_time,
                "end_datetime": end_time,
                "cube:variables": variables,
            },
            "assets": {
                "zarr-store": {
                    "href": str(meta["path"].resolve()),
                    "type": "application/vnd+zarr",
                    "roles": ["data"],
                }
            },
        }

        item_file = stac_dir / f"{item_id}.json"
        item_file.write_text(json.dumps(stac_item, indent=2))
        catalog["links"].append({"rel": "item", "href": f"./{item_id}.json", "title": meta["title"]})

    catalog_file = stac_dir / "catalog.json"
    catalog_file.write_text(json.dumps(catalog, indent=2))
    print(f"[SUCCESS] Generated STAC Catalog & 2 Datacube Items in: {stac_dir.resolve()}")
    return catalog_file


# =============================================================================
# 2. DEFINE FASTAPI OGC API - EDR & LAKEHOUSE SERVING ENDPOINTS
# =============================================================================
app = FastAPI(
    title="Weather Intelligence Platform - OGC EDR & Trading API",
    version="1.0.0",
    description="Replaces legacy Apache Tomcat static files with dynamic Zarr & Lakehouse slicing.",
)

WKT_POINT_REGEX = re.compile(r"POINT\s*\(\s*([+-]?\d+\.?\d*)\s+([+-]?\d+\.?\d*)\s*\)", re.IGNORECASE)


@app.get("/stac/collections")
def get_stac_collections():
    """Returns our STAC catalog items so clients know what weather cubes exist."""
    stac_dir = Path("data/stac")
    items = []
    for f in sorted(stac_dir.glob("*.json")):
        if f.name != "catalog.json":
            items.append(json.loads(f.read_text()))
    return {"collections": items}


@app.get("/collections/{collection_id}/position")
def ogc_edr_position_query(
    collection_id: str,
    coords: str = Query(..., description="WKT Point, e.g., POINT(-2.09 57.15) for Aberdeen"),
    parameter_name: str = Query(None, description="Comma-separated variables, e.g., t2m,msl or swh"),
):
    """
    Standard OGC API - Environmental Data Retrieval (EDR) 'position' query!
    Slices the Zarr cube on the fly for ANY (lon, lat) coordinate without pre-generating CSVs.
    """
    match = WKT_POINT_REGEX.match(coords.strip())
    if not match:
        raise HTTPException(status_code=400, detail="Invalid WKT format. Expected 'POINT(lon lat)'.")
    target_lon, target_lat = float(match.group(1)), float(match.group(2))

    zarr_map = {
        "era5-uk": Path("data/zarr/era5_uk.zarr"),
        "north-sea-wave": Path("data/zarr/north_sea_wave.zarr"),
    }
    if collection_id not in zarr_map:
        raise HTTPException(status_code=404, detail=f"Collection '{collection_id}' not found.")

    ds = xr.open_zarr(zarr_map[collection_id], consolidated=True)
    point_slice = ds.sel(latitude=target_lat, longitude=target_lon, method="nearest")

    requested_vars = [v.strip() for v in parameter_name.split(",")] if parameter_name else list(ds.data_vars)
    series_data = {}
    for var in requested_vars:
        if var in point_slice:
            vals = point_slice[var].values.tolist()
            # Convert Kelvin to Celsius on the fly if t2m
            if var == "t2m":
                vals = [round(v - 273.15, 2) for v in vals]
                unit = "degC"
            else:
                vals = [round(v, 2) for v in vals]
                unit = point_slice[var].attrs.get("units", "")
            series_data[var] = {"units": unit, "values": vals}

    response = {
        "type": "CoverageJSON",
        "collection": collection_id,
        "query_coords_wkt": coords,
        "snapped_grid_point": {
            "latitude": round(float(point_slice.latitude), 2),
            "longitude": round(float(point_slice.longitude), 2),
        },
        "domain": {
            "axes": {
                "t": {"values": [str(t)[:19] + "Z" for t in point_slice.time.values]}
            }
        },
        "ranges": series_data,
    }
    ds.close()
    return response


@app.get("/api/v1/trading-alerts")
def get_trading_desk_alerts():
    """Queries the dbt Gold Mart table (mart_uk_trading_desk_alerts) directly via DuckDB."""
    con = duckdb.connect("data/lakehouse/weather_lakehouse.duckdb", read_only=True)
    records = con.sql("SELECT * FROM main.mart_uk_trading_desk_alerts").df().to_dict(orient="records")
    con.close()
    return {"source": "dbt_gold_mart", "count": len(records), "alerts": records}


# =============================================================================
# 3. SELF-TEST THE API ENDPOINTS USING FASTAPI TESTCLIENT
# =============================================================================
def verify_day4_apis():
    print("\n--- 1. GENERATING STAC CATALOG FOR ZARR CUBES ---")
    generate_stac_catalog()

    client = TestClient(app)

    print("\n--- 2. TESTING OGC API - EDR POSITION ENDPOINT (DYNAMIC ZARR SLICE) ---")
    # Query Forties Charlie Platform in Central North Sea: POINT(0.88 57.72)
    res_wave = client.get(
        "/collections/north-sea-wave/position",
        params={"coords": "POINT(0.88 57.72)", "parameter_name": "swh,msl"}
    )
    wave_json = res_wave.json()
    print(f"HTTP Status: {res_wave.status_code} | Collection: {wave_json['collection']}")
    print(f"Requested WKT: {wave_json['query_coords_wkt']} -> Snapped Grid: {wave_json['snapped_grid_point']}")
    print(f"First 6 Hours SWH (m):  {wave_json['ranges']['swh']['values'][:6]}")
    print(f"Peak Afternoon SWH (m): {max(wave_json['ranges']['swh']['values'])}m (at 15:00 UTC)")

    print("\n--- 3. TESTING DBT GOLD MART TRADING ALERTS API ---")
    res_alerts = client.get("/api/v1/trading-alerts")
    alerts_json = res_alerts.json()
    print(f"HTTP Status: {res_alerts.status_code} | Returned {alerts_json['count']} asset risk summaries:")
    for a in alerts_json["alerts"]:
        print(f"  • {a['asset_id']} ({a['asset_name']}): HDD_12z={a['total_hdd_12z']} (Delta: +{a['hdd_revision_delta']}) | Risk={a['risk_level']}")


if __name__ == "__main__":
    verify_day4_apis()