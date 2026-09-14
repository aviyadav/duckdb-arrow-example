# How to Use DuckDB to Run Analytics on PostgreSQL & RustFS

A minimal Python demo showing how to run analytics on live data sources
with DuckDB — no heavy data pipelines or ETL required:
1. **PostgreSQL** — DuckDB attaches the database in place using the
   `postgres` extension. Direct database connections use
   [psycopg 3](https://www.psycopg.org/psycopg3/).
2. **RustFS (S3-compatible Object Storage)** — DuckDB reads and writes
   Parquet data directly to RustFS bucket storage using the `httpfs`
   extension and DuckDB Secrets.

## Prerequisites

- Python >= 3.14
- [uv](https://docs.astral.sh/uv/) (package/project manager)
- PostgreSQL instance (local, WSL, Docker, or remote)
- RustFS instance (running S3 storage on port 9000 and console on port 9001)

## Setup

```bash
uv sync
```

### Configuration Files

#### 1. PostgreSQL (`.env-postgres`)

```ini
DB_HOST=localhost
DB_NAME=postgres
DB_USER=postgres
DB_PASSWORD=your_password
DB_PORT=5432
```

#### 2. RustFS (`.env-rustfs`)

```ini
rustfs-console=http://localhost:9001/rustfs/console
rustfs-admin=your_admin_user
rustfs-password=your_admin_password
```

| Variable          | Description                                  | Default |
| ----------------- | -------------------------------------------- | ------- |
| `rustfs-console`  | RustFS web console URL                       | —       |
| `rustfs-admin`    | RustFS admin access key                      | —       |
| `rustfs-password` | RustFS admin secret key                      | —       |
| `RUSTFS_S3_ENDPOINT` | Optional override for S3 API endpoint     | `localhost:9000` |

*Both `.env-postgres` and `.env-rustfs` contain secrets — do not commit them.*

## Usage

### PostgreSQL Workflow

```bash
# 1. Verify the PostgreSQL connection
uv run python test_connection.py

# 2. Create the demo table and populate it with 100 rows
uv run python create_demo_table.py

# 3. Attach PostgreSQL to DuckDB and query it
uv run python main.py
```

### RustFS S3 Warehouse Workflow

```bash
# 1. Generate 1000 demo rows and write to s3://warehouse/demo_sales.parquet
uv run python create_demo_rustfs.py

# 3. Query RustFS data with PyArrow and Polars
uv run python main_polars_arrow.py
```

## Demo Datasets

Both generators produce deterministic, reproducible sales data (fixed
random seed and fixed date range across 2025):

| Column       | Type             | Description                              |
| ------------ | ---------------- | ---------------------------------------- |
| `id`         | `INTEGER` PK     | Row id (1–100 for Postgres, 1–1000 for RustFS) |
| `product`    | `TEXT`           | Product name (12 items)                  |
| `category`   | `TEXT`           | Electronics / Furniture / Appliances / Stationery |
| `region`     | `TEXT`           | North / South / East / West              |
| `quantity`   | `INTEGER`        | Units sold (1–10)                        |
| `unit_price` | `NUMERIC(10, 2)` | Price per unit (10–2000)                 |
| `sale_date`  | `DATE`           | Sale date (spread across 2025)           |

## Linting

```bash
uv run ruff check .
```

## Project Structure

| File                    | Purpose                                                          |
| ----------------------- | ---------------------------------------------------------------- |
| `test_connection.py`    | Verify PostgreSQL connection and print server version            |
| `create_demo_table.py`  | Create `demo_sales` table in PostgreSQL with 100 rows             |
| `main.py`               | Attach PostgreSQL to DuckDB and run analytics queries            |
| `create_demo_rustfs.py` | Generate 1000 demo rows and write Parquet to `s3://warehouse/`   |
| `main_rustfs.py`        | Query RustFS S3 Parquet dataset with DuckDB and show analytics   |
| `main_polars_arrow.py`  | Read and analyze RustFS Parquet data with PyArrow & Polars       |
| `pyproject.toml`        | Project metadata and dependencies (managed by uv)                |
| `uv.lock`               | Locked dependency versions                                       |
| `.env-postgres`         | PostgreSQL database credentials (not committed)                  |
| `.env-rustfs`           | RustFS console URL and S3 credentials (not committed)            |

## Troubleshooting

- **PostgreSQL connection hangs:** check that PostgreSQL is running and
  reachable at host/port. If running inside WSL with client scripts on
  Windows, ensure localhost forwarding works.
- **RustFS S3 connection:** RustFS exposes the web console on port 9001
  and S3 API on port 9000. DuckDB connects to port 9000 with path-style
  access and `USE_SSL false` for local setups.
- **Don't mix environments:** running `uv sync` / `uv run` from both
  Windows and WSL recreates `.venv` each time. Stick to one environment.
