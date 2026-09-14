## main_postgres_adbc.py ##
"""Read and analyze data from PostgreSQL using PyArrow ADBC and Polars."""

import os
from pathlib import Path
from typing import Any

# PostgreSQL NUMERIC arrives via ADBC as an unregistered 'arrow.opaque'
# extension type; tell Polars to load it as its storage type. Polars reads
# this setting at import time, so it must be set before importing polars.
os.environ.setdefault("POLARS_UNKNOWN_EXTENSION_TYPE_BEHAVIOR", "load_as_storage")

import adbc_driver_postgresql.dbapi as adbc_dbapi
import polars as pl
import pyarrow as pa
from dotenv import dotenv_values, load_dotenv
from polars.exceptions import PolarsError


def load_postgres_config() -> dict[str, Any]:
    """Load PostgreSQL connection details from .env-postgres."""
    env_path = Path(".env-postgres").expanduser()
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
        env_vals = dotenv_values(env_path)
    else:
        print(f"Warning: Configuration file not found at {env_path}")
        env_vals = {}

    config = {
        "host": os.getenv("DB_HOST") or env_vals.get("DB_HOST") or "localhost",
        "name": os.getenv("DB_NAME") or env_vals.get("DB_NAME") or "postgres",
        "user": os.getenv("DB_USER") or env_vals.get("DB_USER"),
        "password": os.getenv("DB_PASSWORD") or env_vals.get("DB_PASSWORD"),
        "port": os.getenv("DB_PORT") or env_vals.get("DB_PORT") or "5432",
    }
    config["uri"] = (
        f"postgresql://{config['user']}:{config['password']}"
        f"@{config['host']}:{config['port']}/{config['name']}"
    )
    return config


def read_with_pyarrow_adbc(config: dict[str, Any]) -> pa.Table:
    """Read the demo_sales table from PostgreSQL as an Arrow Table via ADBC."""
    print("=" * 65)
    print(" A. READING VIA PYARROW (ADBC PostgreSQL driver)")
    print("=" * 65)

    with adbc_dbapi.connect(config["uri"]) as conn, conn.cursor() as cursor:
        cursor.execute("SELECT * FROM demo_sales")
        arrow_table = cursor.fetch_arrow_table()

    print(
        f"Arrow Table loaded: {arrow_table.num_rows} rows, "
        f"{arrow_table.num_columns} columns"
    )
    print("Schema:")
    for field in arrow_table.schema:
        print(f"  - {field.name}: {field.type}")

    # Zero-copy conversion from PyArrow Table into Polars DataFrame
    df_from_arrow = pl.from_arrow(arrow_table)
    print(f"Converted to Polars DataFrame (zero-copy): {df_from_arrow.shape}\n")
    return arrow_table


def read_with_polars(config: dict[str, Any]) -> None:
    """Read the demo_sales table via Polars (ADBC engine) and run analytics."""
    print("=" * 65)
    print(" B. READING & QUERYING VIA POLARS (read_database_uri / ADBC)")
    print("=" * 65)

    df = pl.read_database_uri(
        "SELECT * FROM demo_sales",
        config["uri"],
        engine="adbc",
    ).with_columns(pl.col("unit_price").cast(pl.Float64))
    lf = df.lazy()

    print("1. First 5 Sample Records:")
    print(lf.limit(100).collect())

    print("\n2. Revenue & Units by Product Category:")
    category_summary = (
        lf.group_by("category")
        .agg(
            total_orders=pl.len(),
            total_units=pl.col("quantity").sum(),
            total_revenue=(pl.col("quantity") * pl.col("unit_price")).sum().round(2),
            avg_unit_price=pl.col("unit_price").mean().round(2),
        )
        .sort("total_revenue", descending=True)
        .collect()
    )
    print(category_summary)

    print("\n3. Regional Performance Breakdown:")
    regional_summary = (
        lf.group_by("region")
        .agg(
            total_orders=pl.len(),
            total_units=pl.col("quantity").sum(),
            total_revenue=(pl.col("quantity") * pl.col("unit_price")).sum().round(2),
        )
        .sort("total_revenue", descending=True)
        .collect()
    )
    print(regional_summary)

    print("\n4. Top 5 Best-Selling Products by Revenue:")
    top_products = (
        lf.group_by("product", "category")
        .agg(
            total_units=pl.col("quantity").sum(),
            total_revenue=(pl.col("quantity") * pl.col("unit_price")).sum().round(2),
        )
        .sort("total_revenue", descending=True)
        .limit(5)
        .collect()
    )
    print(top_products)

    print("\n5. Overall Summary Metrics:")
    overall_metrics = lf.select(
        total_transactions=pl.len(),
        total_units_sold=pl.col("quantity").sum(),
        gross_revenue=(pl.col("quantity") * pl.col("unit_price")).sum().round(2),
        avg_order_value=(pl.col("quantity") * pl.col("unit_price")).mean().round(2),
    ).collect()
    print(overall_metrics)


def main() -> None:
    """Orchestrate configuration loading, ADBC reading, and Polars querying."""
    config = load_postgres_config()
    print(f"Connecting to PostgreSQL at {config['host']}:{config['port']}...\n")

    try:
        read_with_pyarrow_adbc(config)
        read_with_polars(config)
    except (adbc_dbapi.Error, pa.ArrowException, PolarsError, OSError) as error:
        print(f"Error reading from PostgreSQL: {error}")


if __name__ == "__main__":
    main()
