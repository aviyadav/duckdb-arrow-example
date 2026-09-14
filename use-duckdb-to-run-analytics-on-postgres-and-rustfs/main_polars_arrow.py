## main_polars_arrow.py ##
"""Read and analyze data from RustFS S3 warehouse using PyArrow and Polars."""
import os
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import polars as pl
import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq
from dotenv import dotenv_values, load_dotenv
from polars.exceptions import PolarsError


def load_rustfs_config() -> dict[str, Any]:
    """Load and parse RustFS credentials and S3 endpoint from .env-rustfs."""
    env_path = Path(".env-rustfs").expanduser()
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
        env_vals = dotenv_values(env_path)
    else:
        print(f"Warning: Configuration file not found at {env_path}")
        env_vals = {}

    console_url = (
        os.getenv("rustfs-console")
        or env_vals.get("rustfs-console")
        or os.getenv("RUSTFS_CONSOLE")
        or "http://localhost:9001/rustfs/console"
    )
    admin_user = (
        os.getenv("rustfs-admin")
        or env_vals.get("rustfs-admin")
        or os.getenv("RUSTFS_ADMIN")
    )
    admin_password = (
        os.getenv("rustfs-password")
        or env_vals.get("rustfs-password")
        or os.getenv("RUSTFS_PASSWORD")
    )

    parsed_console = urlparse(console_url)
    s3_host = parsed_console.hostname or "localhost"
    s3_port = 9000 if parsed_console.port == 9001 else (parsed_console.port or 9000)
    s3_endpoint = os.getenv("RUSTFS_S3_ENDPOINT", f"{s3_host}:{s3_port}")
    use_ssl = parsed_console.scheme == "https"
    scheme = "https" if use_ssl else "http"
    endpoint_url = f"{scheme}://{s3_endpoint}"

    return {
        "admin_user": admin_user,
        "admin_password": admin_password,
        "s3_endpoint": s3_endpoint,
        "endpoint_url": endpoint_url,
        "use_ssl": use_ssl,
        "scheme": scheme,
        "bucket": "warehouse",
        "file_key": "demo_sales.parquet",
    }


def read_with_pyarrow(config: dict[str, Any]) -> pa.Table:
    """Read Parquet dataset from RustFS using PyArrow S3FileSystem."""
    print("=" * 65)
    print(" A. READING VIA PYARROW (S3FileSystem)")
    print("=" * 65)

    s3_fs = pafs.S3FileSystem(
        access_key=config["admin_user"],
        secret_key=config["admin_password"],
        endpoint_override=config["s3_endpoint"],
        scheme=config["scheme"],
    )

    pyarrow_path = f"{config['bucket']}/{config['file_key']}"
    arrow_table = pq.read_table(pyarrow_path, filesystem=s3_fs)

    print(
        f"Arrow Table loaded: {arrow_table.num_rows} rows, {arrow_table.num_columns} columns"
    )
    print("Schema:")
    for field in arrow_table.schema:
        print(f"  - {field.name}: {field.type}")

    # Zero-copy conversion from PyArrow Table into Polars DataFrame
    df_from_arrow = pl.from_arrow(arrow_table)
    print(f"Converted to Polars DataFrame (zero-copy): {df_from_arrow.shape}\n")
    return arrow_table


def read_with_polars(config: dict[str, Any]) -> None:
    """Scan and analyze Parquet dataset from RustFS using Polars LazyFrame."""
    print("=" * 65)
    print(" B. READING & QUERYING VIA POLARS (LazyFrame / scan_parquet)")
    print("=" * 65)

    storage_options = {
        "aws_access_key_id": config["admin_user"],
        "aws_secret_access_key": config["admin_password"],
        "aws_endpoint_url": config["endpoint_url"],
        "aws_region": "us-east-1",
        "aws_allow_http": "true" if not config["use_ssl"] else "false",
    }

    s3_uri = f"s3://{config['bucket']}/{config['file_key']}"
    lf = pl.scan_parquet(s3_uri, storage_options=storage_options)

    print("1. First 5 Sample Records:")
    print(lf.limit(5).collect())

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
    overall_metrics = (
        lf.select(
            total_transactions=pl.len(),
            total_units_sold=pl.col("quantity").sum(),
            gross_revenue=(pl.col("quantity") * pl.col("unit_price")).sum().round(2),
            avg_order_value=(pl.col("quantity") * pl.col("unit_price"))
            .mean()
            .round(2),
        )
        .collect()
    )
    print(overall_metrics)


def main() -> None:
    """Orchestrate configuration loading, PyArrow reading, and Polars querying."""
    config = load_rustfs_config()
    print(f"Connecting to RustFS S3 endpoint at {config['endpoint_url']}...\n")

    try:
        read_with_pyarrow(config)
        read_with_polars(config)
    except (pa.ArrowException, PolarsError, OSError) as error:
        print(f"Error accessing RustFS: {error}")


if __name__ == "__main__":
    main()
