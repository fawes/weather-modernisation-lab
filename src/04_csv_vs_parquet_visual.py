"""
Compares CSV vs. Parquet for Xarray tabular exports and generates a visual diagram.
"""

from pathlib import Path
import time
import xarray as xr
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch

def run_parquet_comparison():
    nc_path = Path("data/raw/era5_uk_sample.nc")
    out_dir = Path("data/output")
    out_dir.mkdir(parents=True, exist_ok=True)

    # 1. Open the full UK 3D Xarray Dataset (24h x 45 lats x 49 lons = 52,920 rows)
    ds = xr.open_dataset(nc_path, engine="netcdf4")
    ds["t2m_c"] = ds["t2m"] - 273.15
    df = ds[["t2m_c", "msl"]].to_dataframe().reset_index()
    ds.close()

    csv_path = out_dir / "uk_full_grid.csv"
    parquet_path = out_dir / "uk_full_grid.parquet"

    # 2. Write and time CSV vs Parquet
    t0 = time.perf_counter()
    df.to_csv(csv_path, index=False)
    csv_write_ms = (time.perf_counter() - t0) * 1000

    t0 = time.perf_counter()
    df.to_parquet(parquet_path, engine="pyarrow", compression="snappy", index=False)
    pq_write_ms = (time.perf_counter() - t0) * 1000

    # 3. Read ONLY 1 column ('t2m_c') and time CSV vs Parquet
    t0 = time.perf_counter()
    _ = pd.read_csv(csv_path, usecols=["t2m_c"])
    csv_read_ms = (time.perf_counter() - t0) * 1000

    t0 = time.perf_counter()
    _ = pd.read_parquet(parquet_path, columns=["t2m_c"])
    pq_read_ms = (time.perf_counter() - t0) * 1000

    csv_kb = csv_path.stat().st_size / 1024
    pq_kb = parquet_path.stat().st_size / 1024

    print(f"=== BENCHMARK (52,920 Weather Grid Rows) ===")
    print(f"CSV File Size:     {csv_kb:8.1f} KB | Write: {csv_write_ms:6.1f} ms | Read 1 Col: {csv_read_ms:5.1f} ms")
    print(f"Parquet File Size: {pq_kb:8.1f} KB | Write: {pq_write_ms:6.1f} ms | Read 1 Col: {pq_read_ms:5.1f} ms")
    print(f"Storage Reduction: {csv_kb / pq_kb:.1f}x smaller with Parquet!")

    # 4. Generate Visual Diagram + Live Benchmark Chart
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6.5), dpi=150)
    fig.suptitle("Why Parquet Replaces CSV for Tabular Weather Outputs", fontsize=15, fontweight="bold", y=0.98)

    # Left Panel: Architectural Memory Layout (Row vs Columnar)
    ax1.set_title("1. Physical Storage Layout: CSV (Row) vs. Parquet (Columnar)", fontsize=11, fontweight="bold")
    ax1.axis("off")

    # CSV Box
    ax1.add_patch(FancyBboxPatch((0.03, 0.55), 0.94, 0.36, boxstyle="round,pad=0.02", facecolor="#ffcccc", edgecolor="#b22222"))
    ax1.text(0.06, 0.84, "CSV (Row-Oriented Plain Text) - Reads Everything sequentially:", fontsize=10, fontweight="bold", color="#800000")
    ax1.text(0.06, 0.62, "Row 1: [00:00 | 60.0°N | -10.0°W | 12.1°C | 1012 hPa]\n"
                         "Row 2: [00:00 | 60.0°N |  -9.7°W | 12.3°C | 1012 hPa]\n"
                         "Row 3: [00:00 | 60.0°N |  -9.5°W | 12.4°C | 1013 hPa]", fontsize=9, family="monospace")

    # Parquet Box
    ax1.add_patch(FancyBboxPatch((0.03, 0.06), 0.94, 0.42, boxstyle="round,pad=0.02", facecolor="#d9f0d3", edgecolor="#006400"))
    ax1.text(0.06, 0.41, "Parquet (Column-Oriented Binary + Footer) - Skips Unneeded Columns:", fontsize=10, fontweight="bold", color="#004d00")
    ax1.text(0.06, 0.12, "Col Chunk [time]:  [00:00 x 2,205] (Run-Length Compressed!)\n"
                         "Col Chunk [lat]:   [60.0, 59.75 ...] (Binary Float32)\n"
                         "Col Chunk [t2m_c]: [12.1, 12.3, 12.4 ...] <-- Read ONLY this block!\n"
                         "Col Chunk [msl]:   [1012, 1012, 1013 ...] (Skipped during Temp query)\n"
                         "Footer Metadata:   {t2m_c: min=9.2°C, max=25.4°C, types=preserved}", fontsize=9, family="monospace")

    # Right Panel: Your Laptop's Actual Benchmark Results
    ax2.set_title("2. Your WSL2 Benchmark (52,920 UK Weather Rows)", fontsize=11, fontweight="bold")
    metrics = ["File Size (KB)", "Write Time (ms)", "Read 1 Col (ms)"]
    csv_vals = [csv_kb, csv_write_ms, csv_read_ms]
    pq_vals = [pq_kb, pq_write_ms, pq_read_ms]

    x = range(len(metrics))
    w = 0.35
    bars1 = ax2.bar([i - w/2 for i in x], csv_vals, width=w, label="Legacy CSV", color="#d62728")
    bars2 = ax2.bar([i + w/2 for i in x], pq_vals, width=w, label="Modern Parquet", color="#2ca02c")

    ax2.set_xticks(list(x))
    ax2.set_xticklabels(metrics, fontweight="bold")
    ax2.set_ylabel("KB / Milliseconds (Lower is Better)")
    ax2.legend()
    ax2.grid(axis="y", linestyle="--", alpha=0.4)

    for bar in list(bars1) + list(bars2):
        h = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2, h + max(csv_vals)*0.02, f"{h:.1f}", ha="center", fontsize=9, fontweight="bold")

    out_img = out_dir / "csv_vs_parquet_visual.png"
    plt.tight_layout(rect=[0, 0, 1, 0.94])
    plt.savefig(out_img)
    plt.close(fig)
    print(f"[SUCCESS] Visual benchmark saved to: {out_img.resolve()}")

if __name__ == "__main__":
    run_parquet_comparison()