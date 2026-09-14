## create_demo_rustfs.py ##
"""Generate 1000 demo sales records and store them in RustFS (S3 warehouse)."""
import os
import random
from datetime import date, timedelta
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

# 4. Derive the S3 API endpoint from console URL or environment
parsed_console = urlparse(console_url)
s3_host = parsed_console.hostname or "localhost"
# MinIO / RustFS default S3 API is on port 9000 when console is on 9001
s3_port = 9000 if parsed_console.port == 9001 else (parsed_console.port or 9000)
s3_endpoint = os.getenv("RUSTFS_S3_ENDPOINT", f"{s3_host}:{s3_port}")
use_ssl = parsed_console.scheme == "https"

# 5. Build 1000 reproducible demo rows spread over 2025
PRODUCTS = [
    ("Laptop", "Electronics"),
    ("Smartphone", "Electronics"),
    ("Headphones", "Electronics"),
    ("Monitor", "Electronics"),
    ("Desk", "Furniture"),
    ("Chair", "Furniture"),
    ("Bookshelf", "Furniture"),
    ("Coffee Maker", "Appliances"),
    ("Blender", "Appliances"),
    ("Toaster", "Appliances"),
    ("Notebook", "Stationery"),
    ("Pen Set", "Stationery"),
]
REGIONS = ["North", "South", "East", "West"]

random.seed(42)  # Fixed seed so re-runs produce the same data
start_date = date(2025, 1, 1)
rows = []
for row_id in range(1, 1001):
    product, category = random.choice(PRODUCTS)
    rows.append(
        (
            row_id,
            product,
            category,
            random.choice(REGIONS),
            random.randint(1, 10),
            round(random.uniform(10, 2000), 2),
            start_date + timedelta(days=random.randint(0, 365)),
        )
    )

# 6. Connect to DuckDB, configure S3 secret, and write to RustFS warehouse
try:
    print(f"Connecting to RustFS S3 endpoint at {s3_endpoint}...")
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

    # Stage data into a temporary table
    con.execute(
        """
        CREATE OR REPLACE TEMPORARY TABLE demo_sales (
            id         INTEGER PRIMARY KEY,
            product    VARCHAR NOT NULL,
            category   VARCHAR NOT NULL,
            region     VARCHAR NOT NULL,
            quantity   INTEGER NOT NULL,
            unit_price DECIMAL(10, 2) NOT NULL,
            sale_date  DATE NOT NULL
        );
        """
    )
    con.executemany("INSERT INTO demo_sales VALUES (?, ?, ?, ?, ?, ?, ?);", rows)

    # Write out Parquet file to s3://warehouse/
    target_path = "s3://warehouse/demo_sales.parquet"
    print(f"Writing Parquet data to {target_path}...")
    con.execute(
        f"COPY demo_sales TO '{target_path}' (FORMAT PARQUET, OVERWRITE_OR_IGNORE 1);"
    )

    # Verify rows written
    count = con.execute(f"SELECT COUNT(*) FROM '{target_path}';").fetchone()[0]
    print(f"Successfully generated and wrote {count} rows to RustFS ({target_path})!")

except duckdb.Error as error:
    print(f"Error storing demo data in RustFS: {error}")
