# Converting CSV Files to Optimized Parquet

A hands-on benchmark showing why **Parquet** beats raw **CSV** for analytical
workloads. Using [DuckDB](https://duckdb.org/) and `psutil`, this project
generates a large CSV dataset, converts it to a compressed Parquet file, and
measures the time and peak memory of each approach.

## What it demonstrates

- How to stream a 100M-row CSV into a compact Parquet file without loading it
  all into memory.
- The performance difference between querying a raw CSV and a Parquet file.
- Why **explicit typing** matters: with `all_varchar = true`, numeric columns
  become text and aggregations such as `SUM(revenue)` fail with a
  `BinderException`.
- How to measure peak process memory from a background thread while a query
  runs.

## Project structure

| File | Purpose |
| --- | --- |
| `generate_csv_data.py` | Generates `data/events.csv` (100M rows) using DuckDB's `generate_series`. |
| `generate_csv_pyarrow.py` | Generates `data/events_py.csv` with the same shape using NumPy + PyArrow only (no DuckDB), in parallel batches with bounded memory. |
| `convert_to_parquet.py` | Reads the CSV with explicit column types and writes `data/events.parquet` using ZSTD compression. Reports conversion time and peak memory. |
| `auto_inferencing.py` | Runs a `GROUP BY country` aggregation on the CSV using DuckDB's default type auto-inference. Reports time and peak memory. |
| `query_parquet_file.py` | Runs the same aggregation directly against the Parquet file. Reports time and peak memory. |
| `everything_varchar.py` | Forces every column to `VARCHAR` to show the error you get when summing text instead of numbers. |

## Dataset schema

The generated `events.csv` contains 100M rows with the following columns:

| Column | Type | Description |
| --- | --- | --- |
| `event_date` | `DATE` | Random date between 2024-01-01 and 2026-01-31. |
| `country` | `VARCHAR` | One of `US`, `UK`, `DE`, `FR`, `IN`, `JP`. |
| `channel` | `VARCHAR` | One of `search`, `social`, `email`, `direct`. |
| `user_id` | `INTEGER` | Random value between 1 and 200,000. |
| `order_id` | `INTEGER` | Random value between 1 and 900,000. |
| `revenue` | `DOUBLE` | Skewed value; 15% of rows are `0.0`. |

## Requirements

- Python 3.13+
- [uv](https://docs.astral.sh/uv/) (recommended) or `pip`
- Dependencies: `duckdb` and `psutil`

## Setup

```sh
uv sync
```

Or with pip:

```sh
pip install duckdb psutil
```

## Usage

Run the scripts from the project root, in order. The first step generates a
multi-gigabyte file, so make sure you have disk space available.

```sh
# 1. Generate the source CSV (~3.7 GB)
uv run python generate_csv_data.py

#    Or generate an equivalent file with NumPy + PyArrow instead of DuckDB
uv run python generate_csv_pyarrow.py

# 2. Convert the CSV to a compressed Parquet file (~950 MB)
uv run python convert_to_parquet.py

# 3. Query the CSV and the Parquet file to compare performance
uv run python auto_inferencing.py
uv run python query_parquet_file.py

# 4. See what happens when numeric columns are treated as text
uv run python everything_varchar.py
```

### `generate_csv_pyarrow.py` options

`generate_csv_pyarrow.py` accepts a few flags so you can tune or smoke-test it:

```sh
uv run python generate_csv_pyarrow.py \
    --rows 100000000 \
    --batch-size 1000000 \
    --workers 8 \
    --output data/events_py.csv \
    --seed 42
```

It uses a `ProcessPoolExecutor` to build batches in parallel and writes them
through a `pyarrow.csv.CSVWriter` in order. Only a bounded window of batches
(`2 x workers`) is ever kept in memory, so resident memory stays roughly
constant even for hundreds of millions of rows.

## Sample output

`convert_to_parquet.py` prints a summary like:

```
==================================================
CSV to Parquet Export Metrics (Explicit Typing):
==================================================
Total Conversion Time:   45.1234 seconds
Peak Memory Usage:       612.34 MB
Original CSV Size:       3785.60 MB
Optimized Parquet Size:  945.71 MB
==================================================
```

`auto_inferencing.py` and `query_parquet_file.py` print the query time and peak
memory for the same `GROUP BY country` aggregation, so you can compare the two
storage formats directly.

## Notes

- Parquet output is written with **ZSTD** compression, which typically reduces
  file size by ~4x compared with the raw CSV while also being columnar and
  typed.
- Peak memory is sampled every 5 ms by a daemon thread calling
  `psutil.Process().memory_info().rss`.
- Generated data in `data/` is not intended to be committed to version control.
