## test_connection.py ##
import os
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

# 4. Connect to the PostgreSQL database
try:
    print(f"Connecting to PostgreSQL at {db_host}:{db_port}...")
    connection = psycopg.connect(
        host=db_host,
        dbname=db_name,
        user=db_user,
        password=db_password,
        port=db_port,
        connect_timeout=10,
    )

    # Create a cursor to perform database operation
    cursor = connection.cursor()

    # Execute a simple query to verify connection
    cursor.execute("SELECT version();")
    db_version = cursor.fetchone()
    print(f"Successfully connected to PostgreSQL! Version: {db_version[0]}")

    # Close the cursor and connection
    cursor.close()
    connection.close()
except psycopg.Error as error:
    print(f"Error connecting to PostgreSQL: {error}")
