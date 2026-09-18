# SQL over Multi-Dimensional Arrays — DuckDB + xarray-sql on ARCO-ERA5

This project demonstrates how to **query petabyte-scale, labeled multi-dimensional arrays with SQL**.
The data is [ARCO-ERA5](https://console.cloud.google.com/storage/browser/gcp-public-data-arco-era5) —
Google's Analysis-Ready Cloud-Optimized rechunking of the ERA5 weather reanalysis, stored as Zarr on
Google Cloud Storage:

- `gs://gcp-public-data-arco-era5/ar/full_37-1h-0p25deg-chunk-1.zarr-v3`
- hourly steps from **1940‑01‑01** to the present, 0.25° global grid (721 × 1440), 37 pressure levels
- ≈ **3.3 PB** uncompressed (`ds.nbytes / 1024⁴ ≈ 3344.7 TB`) — you only ever read a few MB of it

The notebook (`intro-xarray-xarraysql-era5.ipynb`) shows two complementary SQL engines working on the
same in-memory xarray `Dataset`:

| Engine | What it does |
|---|---|
| **xarray-sql ≥ 0.3** (`XarrayContext`, DataFusion-backed) | Pivots the Dataset into **lazily-read Arrow partitions** registered as SQL tables. Filters on dimension columns (e.g. `time BETWEEN …`) **prune partitions** before any data is read. Results round-trip back to xarray with `XarrayDataFrame.to_dataset(dims=…, template=…)`. |
| **DuckDB + jupysql** (`%sql` / `%%sql` magic) | Plain DuckDB connected to the kernel. The community `zarr` extension inspects remote Zarr **metadata**; for value queries a small xarray slice is materialized and handed to DuckDB through the Arrow columnar interface. |

> Note: xarray-sql 0.3 removed its old DuckDB backend (`xarray_sql.register()` / `xarray_sql.to_dataset()`
> no longer exist). Everything runs through the DataFusion-backed `XarrayContext` shown here.

## Repository contents

| File | Purpose |
|---|---|
| `intro-xarray-xarraysql-era5.ipynb` | Main tutorial — SQL queries + visualizations on ARCO-ERA5 |
| `power_plants_pipeline.py` | Standalone demo: a Dask/pandas power-plant pipeline (fabricated data) that saves `usa_capacity_coal_wind.png` |
| `sqls.sql` | Example analytical SQL: forecast-vs-ERA5 RMSE join (pattern used with the registered `era5` tables) |
| `pyproject.toml`, `uv.lock` | Dependencies, locked with [uv](https://docs.astral.sh/uv/) |

## Setup

Requirements: **Python ≥ 3.14**, [uv](https://docs.astral.sh/uv/), and **internet access** (the notebook
reads public GCS data anonymously — no credentials needed).

```bash
# create .venv and install exactly the locked dependencies
uv sync
```

## Run and observe

### Interactive

```bash
uv run jupyter lab intro-xarray-xarraysql-era5.ipynb
```

Then **Run → Run All Cells**. A full pass takes ~2–3 minutes (metadata + a few Zarr chunks over the
network; the heavy queries are pruned to 1–2 time partitions).

What you should observe, section by section:

1. **Open + inspect** — `ds` repr shows ~4 PB / 1.3M hourly time steps / 721 × 1440 grid; cell 2 prints
   the dataset size in TB (~3344.7).
2. **hvplot** — an interactive global map of `2m_temperature` for a chosen hour, plus a time-scrubber
   widget animating three days of 2020-01-01 → 2020-01-03.
3. **Xarray-SQL** — `ctx.from_dataset('era5', ds, chunks={'time': 6}, table_names=…)` splits the
   mixed-dimension Dataset into two SQL tables (`era5.surface` on `(time, latitude, longitude)` and
   `era5.atmosphere` on `(time, level, latitude, longitude)`). `show tables` / `show columns` list them;
   the aggregate query returns mean temperature (°C) per pressure level for a 6-hour window; the last
   query's global result is round-tripped into an 8 MB `xr.Dataset`.
4. **DuckDB + XQL** — `%%sql` runs against DuckDB: `read_zarr_metadata(…)` lists every array with its
   dims/shape; a small California slice of `2m_temperature` is materialized with xarray and queried via
   DuckDB, returning real °C values.
5. **Round-trip** — a bounding-box aggregation over California (⚠ ERA5 longitude runs **0–360°**, so
   California is ~235–246°E) produces `ca_ds`, a `(time: 6, latitude: 39, longitude: 42)` dataset in °C
   with coordinates, attributes and dtypes recovered from the original dataset via `template=ds`.

### Headless (no browser)

```bash
uv run jupyter nbconvert --to notebook --execute --inplace intro-xarray-xarraysql-era5.ipynb
```

### Power-plant demo script

```bash
uv run python power_plants_pipeline.py    # writes usa_capacity_coal_wind.png
```

## Notes & troubleshooting

- **Network**: every data read goes to the public ARCO-ERA5 bucket over HTTPS; offline execution will
  fail at `xr.open_zarr(...)`.
- **DuckDB `zarr` extension**: it can list this store's metadata, but cannot yet decode its data
  chunks (queries would return fill values). The notebook therefore materializes slices with xarray and
  hands them to DuckDB via Arrow — the same pattern works with any Arrow source.
- **gRPC log lines** like `I… fork_posix.cc:71] Other threads are currently calling into gRPC, skipping
  fork() handlers` are **benign INFO noise** from the Google Cloud client stack (grpcio) when the kernel
  forks a subprocess. Set `GRPC_VERBOSITY=ERROR` in the environment to silence them.
- The registered tables are **lazy**: data is only read during query execution, and filters on
  dimension columns prune whole time partitions.
