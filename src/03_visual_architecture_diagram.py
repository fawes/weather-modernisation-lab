"""
Generates a 4-panel visual diagram explaining:
1. Coarse (1.0°) vs ERA5 (0.25°) Spatial Resolution
2. 3D Weather Data Cube (Time x Lat x Lon) & Slicing Directions
3. How GRIB stores data (Sequential 2D Messages)
4. How Xarray/Zarr stores data (Unified 3D Data Cube)
"""

from pathlib import Path
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle, FancyBboxPatch

def create_visual_guide():
    out_dir = Path("data/output")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "grid_and_grib_vs_xarray_visual.png"

    fig = plt.figure(figsize=(16, 12), dpi=150)
    fig.suptitle(
        "Weather Data Engineering Visual Guide: Grids, Resolutions & GRIB vs. Xarray",
        fontsize=16, fontweight="bold", y=0.97
    )

    # =========================================================================
    # PANEL 1: Spatial Resolution (Coarse 1.0° vs ERA5 0.25° over Central UK)
    # =========================================================================
    ax1 = fig.add_subplot(2, 2, 1)
    ax1.set_title("1. Spatial Resolution: 1.0° Grid vs. ERA5 0.25° Grid (Midlands/UK)", fontsize=12, fontweight="bold")

    # Fine 0.25° ERA5 grid points
    lons_fine, lats_fine = np.meshgrid(np.arange(-3.0, 0.25, 0.25), np.arange(51.0, 54.25, 0.25))
    ax1.scatter(lons_fine, lats_fine, color="#1f77b4", s=25, alpha=0.6, label="ERA5 0.25° Grid (~28km)")

    # Coarse 1.0° grid points
    lons_coarse, lats_coarse = np.meshgrid(np.arange(-3.0, 1.0, 1.0), np.arange(51.0, 55.0, 1.0))
    ax1.scatter(lons_coarse, lats_coarse, color="#d62728", s=110, marker="s", label="Coarse 1.0° Grid (~111km)")

    # Plot Birmingham & London
    ax1.plot(-1.8904, 52.4862, marker="*", color="gold", markersize=16, markeredgecolor="black", label="Birmingham")
    ax1.text(-1.82, 52.53, "Birmingham\n(52.49°N, -1.89°W)", fontsize=9, fontweight="bold")

    ax1.set_xlabel("Longitude (°E)")
    ax1.set_ylabel("Latitude (°N)")
    ax1.set_xlim(-3.2, 0.3)
    ax1.set_ylim(50.8, 54.2)
    ax1.grid(True, linestyle="--", alpha=0.4)
    ax1.legend(loc="lower left", fontsize=9)

    # =========================================================================
    # PANEL 2: 3D Data Cube (Time x Latitude x Longitude)
    # =========================================================================
    ax2 = fig.add_subplot(2, 2, 2, projection="3d")
    ax2.set_title("2. The 3D Xarray Cube: Horizontal Map Slice vs. Vertical Time Drill", fontsize=12, fontweight="bold")

    # Create a 5x5x5 conceptual grid (Lon x Lat x Time)
    x, y, z = np.meshgrid(np.arange(5), np.arange(5), np.arange(5))
    ax2.scatter(x, y, z, color="lightgray", s=20, alpha=0.4)

    # Highlight Horizontal Slice (1 single timestamp z=2 -> 2D UK Map)
    xh, yh = np.meshgrid(np.arange(5), np.arange(5))
    zh = np.full_like(xh, 2)
    ax2.plot_surface(xh, yh, zh, color="#ff7f0e", alpha=0.45)
    ax2.scatter(xh, yh, zh, color="#ff7f0e", s=40, label="Horizontal Cut (1 Hour = 2D Map)")

    # Highlight Vertical Drill (1 single Lat/Lon (2,2) across all Times -> 1D City Series)
    zv = np.arange(5)
    xv = np.full_like(zv, 2)
    yv = np.full_like(zv, 2)
    ax2.plot(xv, yv, zv, color="#2ca02c", linewidth=4, label="Vertical Drill (1 City = 1D Time Series)")
    ax2.scatter(xv, yv, zv, color="#2ca02c", s=80)

    ax2.set_xlabel("Longitude (West -> East)")
    ax2.set_ylabel("Latitude (South -> North)")
    ax2.set_zlabel("Time (Hours 00:00 -> 23:00)")
    ax2.legend(loc="upper left", fontsize=8)

    # =========================================================================
    # PANEL 3: How GRIB Stores Data (Sequential 2D Messages on Disk)
    # =========================================================================
    ax3 = fig.add_subplot(2, 2, 3)
    ax3.set_title("3. Legacy GRIB File: Flat Sequential Stream of 2D 'Postcards'", fontsize=12, fontweight="bold")
    ax3.axis("off")

    messages = [
        ("Msg 1: Temp (t2m) | 00:00 UTC", "[2D Grid Flattened: 12.1, 12.4, 13.0 ...]", "#aec7e8"),
        ("Msg 2: Pres (msl) | 00:00 UTC", "[2D Grid Flattened: 1012, 1013, 1014 ...]", "#ffbb78"),
        ("Msg 3: Temp (t2m) | 01:00 UTC", "[2D Grid Flattened: 11.8, 12.0, 12.6 ...]", "#aec7e8"),
        ("Msg 4: Pres (msl) | 01:00 UTC", "[2D Grid Flattened: 1011, 1012, 1013 ...]", "#ffbb78"),
        ("Msg 5: Temp (t2m) | 02:00 UTC", "[2D Grid Flattened: 11.5, 11.7, 12.2 ...]", "#aec7e8"),
    ]

    for i, (header, payload, color) in enumerate(messages):
        y_pos = 0.80 - i * 0.16
        box = FancyBboxPatch((0.05, y_pos), 0.90, 0.12, boxstyle="round,pad=0.02", facecolor=color, edgecolor="black")
        ax3.add_patch(box)
        ax3.text(0.08, y_pos + 0.06, f"{header}   -->   {payload}", fontsize=9.5, fontweight="bold", va="center")

    ax3.text(
        0.05, 0.02,
        "Bottleneck: Variables & hours are interleaved! To get a 24h temperature forecast for London,\n"
        "ecCodes/WGRIB must scan the file sequentially, skipping pressure messages and decoding one hour at a time.",
        fontsize=9, color="#b22222", fontweight="bold"
    )

    # =========================================================================
    # PANEL 4: How Xarray (NetCDF / Zarr) Organizes Data
    # =========================================================================
    ax4 = fig.add_subplot(2, 2, 4)
    ax4.set_title("4. Modern Xarray Dataset: Shared Coordinate Axes + Contiguous 3D Cubes", fontsize=12, fontweight="bold")
    ax4.axis("off")

    # Shared Coordinates Box
    coord_box = FancyBboxPatch((0.05, 0.68), 0.90, 0.22, boxstyle="round,pad=0.02", facecolor="#d9f0d3", edgecolor="black")
    ax4.add_patch(coord_box)
    ax4.text(0.08, 0.82, "SHARED COORDINATE RULERS (Stored Once in Memory):", fontsize=10, fontweight="bold")
    ax4.text(0.08, 0.73, "• time (24): [00:00, 01:00, ..., 23:00]\n• latitude (45): [60.0°N ... 49.0°N]    • longitude (49): [-10.0°W ... 2.0°E]", fontsize=9)

    # Cube 1: t2m
    cube1 = FancyBboxPatch((0.05, 0.25), 0.42, 0.35, boxstyle="round,pad=0.02", facecolor="#aec7e8", edgecolor="black")
    ax4.add_patch(cube1)
    ax4.text(0.08, 0.48, "3D Variable Cube: 't2m'\nShape: (24, 45, 49)", fontsize=10, fontweight="bold")
    ax4.text(0.08, 0.32, "Contiguous 3D Array in Kelvin/°C\nInstant slicing via:\nds['t2m'].sel(time=..., lat=...)", fontsize=9)

    # Cube 2: msl
    cube2 = FancyBboxPatch((0.53, 0.25), 0.42, 0.35, boxstyle="round,pad=0.02", facecolor="#ffbb78", edgecolor="black")
    ax4.add_patch(cube2)
    ax4.text(0.56, 0.48, "3D Variable Cube: 'msl'\nShape: (24, 45, 49)", fontsize=10, fontweight="bold")
    ax4.text(0.56, 0.32, "Contiguous 3D Array in Pascals\nCompletely separated from t2m\nfor fast parallel reads!", fontsize=9)

    ax4.text(
        0.05, 0.05,
        "Cloud-Native Advantage: In Zarr on S3/Blob/GCS, each 3D variable cube is split into compressed\n"
        "mini-cubes (chunks) so Xarray can fetch only the exact variable, region, and time window you query.",
        fontsize=9, color="#006400", fontweight="bold"
    )

    plt.tight_layout(rect=[0, 0, 1, 0.95])
    plt.savefig(out_path)
    plt.close(fig)
    print(f"[SUCCESS] Visual guide saved to: {out_path.resolve()}")

if __name__ == "__main__":
    create_visual_guide()