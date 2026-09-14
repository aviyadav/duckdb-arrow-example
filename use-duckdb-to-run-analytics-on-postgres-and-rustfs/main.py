import os
from pathlib import Path

import duckdb
from dotenv import load_dotenv

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

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# 4. Connect to DuckDB and Attach PostgreSQL
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# Create an in-memory DuckDB database (or specify a file path like 'my_duck.db')
con = duckdb.connect(database=":memory:")

# Construct the standard PostgreSQL connection URI
# Format: postgresql://username:password@host:port/database
pg_uri = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

try:
    # Install and load the spatial/postgres extension inside DuckDB
    con.execute("INSTALL postgres;")
    con.execute("LOAD postgres;")

    # Attach the PostgreSQL database as a schema/database alias in DuckDB
    # We will name the attached alias 'my_pg_db'
    con.execute(f"ATTACH '{pg_uri}' AS my_pg_db (TYPE POSTGRES);")
    print("Successfully attached PostgreSQL to DuckDB!")

    # 5. Query your PostgreSQL tables directly from DuckDB!
    # Replace 'your_table_name' with an actual table in your Postgres DB
    result = con.execute("SELECT * FROM my_pg_db.demo_sales LIMIT 5;").fetchall()
    print(result)

except duckdb.Error as e:
    print(f"Failed to attach PostgreSQL: {e}")
