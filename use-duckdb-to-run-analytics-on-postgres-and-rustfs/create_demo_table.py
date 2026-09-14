## create_demo_table.py ##
"""Create a demo sales table in PostgreSQL and populate it with 100 rows."""
import os
import random
from datetime import date, timedelta
from pathlib import Path

import psycopg
from dotenv import load_dotenv

# 1. Expand the '~' to the full home directory path
env_path = Path(".env-postgres").expanduser()

# 2. Load the specific env file
if env_path.exists():
    load_dotenv(dotenv_path=env_path)
else:
    print(f"Warning: Configuration file not found at {env_path}")

# 3. Fetch the variables from the environment
db_host = os.getenv("DB_HOST")
db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_port = os.getenv("DB_PORT", "5432")  # Default to 5432 if not specified

# 4. Build 100 reproducible demo rows spread over the past year
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
start_date = date(2025, 1, 1)  # Fixed start so the demo data is deterministic
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

# 5. Create the table and insert the rows
try:
    print(f"Connecting to PostgreSQL at {db_host}:{db_port}...")
    with psycopg.connect(
        host=db_host,
        dbname=db_name,
        user=db_user,
        password=db_password,
        port=db_port,
        connect_timeout=10,
    ) as connection, connection.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS demo_sales;")
        cursor.execute(
            """
            CREATE TABLE demo_sales (
                id         INTEGER PRIMARY KEY,
                product    TEXT NOT NULL,
                category   TEXT NOT NULL,
                region     TEXT NOT NULL,
                quantity   INTEGER NOT NULL,
                unit_price NUMERIC(10, 2) NOT NULL,
                sale_date  DATE NOT NULL
            );
            """
        )
        cursor.executemany(
            """
            INSERT INTO demo_sales
                (id, product, category, region, quantity, unit_price, sale_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            rows,
        )
        cursor.execute("SELECT COUNT(*) FROM demo_sales;")
        row_count = cursor.fetchone()[0]
    print(f"Created table 'demo_sales' with {row_count} rows.")
except psycopg.Error as error:
    print(f"Error creating demo table: {error}")
