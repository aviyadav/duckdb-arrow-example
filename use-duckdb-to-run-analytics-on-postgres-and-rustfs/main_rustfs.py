## main_rustfs.py ##
"""Query analytical data stored in RustFS warehouse using DuckDB."""
import os
from pathlib import Path
from urllib.parse import urlparse

import duckdb
from dotenv import dotenv_values, load_dotenv

# 1. Expand the '~' to the full home directory path
env_path = Path(".env-rustfs").expanduser()

# 2. Load environment variables from .env-rustfs
if env_path.exists():
    load_dotenv(dotenv_path=env_path)
    env_vals = dotenv_values(env_path)
else:
    print(f"Warning: Configuration file not found at {env_path}")
    env_vals = {}

# 3. Fetch credentials and console URL
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

# 4. Derive S3 API endpoint
parsed_console = urlparse(console_url)
s3_host = parsed_console.hostname or "localhost"
s3_port = 9000 if parsed_console.port == 9001 else (parsed_console.port or 9000)
s3_endpoint = os.getenv("RUSTFS_S3_ENDPOINT", f"{s3_host}:{s3_port}")
use_ssl = parsed_console.scheme == "https"

# 5. Connect to DuckDB and query RustFS warehouse
target_path = "s3://warehouse/demo_sales.parquet"

try:
    con = duckdb.connect()

    # Load httpfs extension for S3 access
    try:
        con.execute("LOAD httpfs;")
    except duckdb.Error:
        con.execute("INSTALL httpfs;")
        con.execute("LOAD httpfs;")

    # Configure RustFS S3 credentials via DuckDB Secret
    con.execute(
        """
        CREATE OR REPLACE SECRET rustfs_secret (
            TYPE S3,
            KEY_ID ?,
            SECRET ?,
            ENDPOINT ?,
            URL_STYLE 'path',
            USE_SSL ?
        );
        """,
        [admin_user, admin_password, s3_endpoint, use_ssl],
    )

    # Create a convenient view over the parquet dataset in RustFS
    con.execute(
        f"CREATE OR REPLACE VIEW sales AS SELECT * FROM '{target_path}';"
    )

    print("=" * 65)
    print(" 1. SAMPLE RECORDS (LIMIT 5)")
    print("=" * 65)
    con.sql("SELECT * FROM sales LIMIT 5;").show()

    print("\n" + "=" * 65)
    print(" 2. REVENUE & UNITS BY PRODUCT CATEGORY")
    print("=" * 65)
    con.sql(
        """
        SELECT
            category,
            COUNT(*) AS total_orders,
            SUM(quantity) AS total_units,
            ROUND(SUM(quantity * unit_price), 2) AS total_revenue,
            ROUND(AVG(unit_price), 2) AS avg_unit_price
        FROM sales
        GROUP BY category
        ORDER BY total_revenue DESC;
        """
    ).show()

    print("\n" + "=" * 65)
    print(" 3. REGIONAL PERFORMANCE")
    print("=" * 65)
    con.sql(
        """
        SELECT
            region,
            COUNT(*) AS total_orders,
            SUM(quantity) AS total_units,
            ROUND(SUM(quantity * unit_price), 2) AS total_revenue
        FROM sales
        GROUP BY region
        ORDER BY total_revenue DESC;
        """
    ).show()

    print("\n" + "=" * 65)
    print(" 4. TOP 5 BEST-SELLING PRODUCTS BY REVENUE")
    print("=" * 65)
    con.sql(
        """
        SELECT
            product,
            category,
            SUM(quantity) AS total_units,
            ROUND(SUM(quantity * unit_price), 2) AS total_revenue
        FROM sales
        GROUP BY product, category
        ORDER BY total_revenue DESC
        LIMIT 5;
        """
    ).show()

    print("\n" + "=" * 65)
    print(" 5. OVERALL SUMMARY METRICS")
    print("=" * 65)
    con.sql(
        """
        SELECT
            COUNT(*) AS total_transactions,
            SUM(quantity) AS total_units_sold,
            ROUND(SUM(quantity * unit_price), 2) AS gross_revenue,
            ROUND(AVG(quantity * unit_price), 2) AS avg_order_value
        FROM sales;
        """
    ).show()

except duckdb.Error as error:
    print(f"Error querying data from RustFS: {error}")
