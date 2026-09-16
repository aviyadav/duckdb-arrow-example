import base64
import os
import duckdb
import keyring
import sys
from keyring.errors import NoKeyringError

PARQUET_KEYRING_SERVICE = "medium.parquet.encrypt"
PARQUET_KEYRING_ACCOUNT = "transfer.parquet_encrypt_key"
ENV_VAR_NAME = "PARQUET_ENCRYPTION_KEY"
INPUT_FILE = "customers_encrypted.parquet"

def get_parquet_key() -> str:
    # 1. Check environment variable first
    env_key = os.environ.get(ENV_VAR_NAME)
    if env_key:
        return env_key
        
    # 2. Fallback to OS keyring
    try:
        key = keyring.get_password(PARQUET_KEYRING_SERVICE, PARQUET_KEYRING_ACCOUNT)
        if key:
            return key
    except NoKeyringError:
        pass
        
    print(f"❌ Error: Key not found.", file=sys.stderr)
    print(f"Please set the {ENV_VAR_NAME} environment variable or configure a keyring.", file=sys.stderr)
    sys.exit(1)

def main():
    hex_key = get_parquet_key()
    raw_bytes = bytes.fromhex(hex_key)
    b64_key = base64.b64encode(raw_bytes).decode("ascii")
    
    con = duckdb.connect(database=":memory:")
    KEY_ALIAS = "my_master_key"
    con.execute(f"PRAGMA add_parquet_key('{KEY_ALIAS}', '{b64_key}');")
    
    print(f"Reading encrypted Parquet file: {INPUT_FILE}...\n")
    
    query = f"""
    SELECT * FROM read_parquet(
        '{INPUT_FILE}',
        encryption_config={{ footer_key: '{KEY_ALIAS}' }}
    ) LIMIT 5;
    """
    
    print("--- Sample Records (Top 5) ---")
    con.sql(query).show()
    
    count_query = f"""
    SELECT COUNT(*) as total_rows FROM read_parquet(
        '{INPUT_FILE}',
        encryption_config={{ footer_key: '{KEY_ALIAS}' }}
    );
    """
    total_rows = con.execute(count_query).fetchone()[0]
    print(f"\nTotal rows in encrypted file: {total_rows}")
    con.close()

if __name__ == "__main__":
    main()