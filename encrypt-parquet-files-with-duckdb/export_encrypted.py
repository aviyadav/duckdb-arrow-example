import base64
import os
import duckdb
import keyring
import sys
from keyring.errors import NoKeyringError

DB_FILE = "demo.duckdb"
PARQUET_KEYRING_SERVICE = "medium.parquet.encrypt"
PARQUET_KEYRING_ACCOUNT = "transfer.parquet_encrypt_key"
ENV_VAR_NAME = "PARQUET_ENCRYPTION_KEY"
OUTPUT_FILE = "customers_encrypted.parquet"

def get_parquet_key() -> str:
    # 1. Check environment variable first (headless fallback)
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
        
    print("❌ Error: Parquet encryption key not found.", file=sys.stderr)
    print(f"Please set the {ENV_VAR_NAME} environment variable or configure a keyring.", file=sys.stderr)
    sys.exit(1)

def main():
    hex_key = get_parquet_key()
    raw_bytes = bytes.fromhex(hex_key)
    b64_key = base64.b64encode(raw_bytes).decode("ascii")
    
    con = duckdb.connect(DB_FILE)
    KEY_ALIAS = "my_master_key"
    con.execute(f"PRAGMA add_parquet_key('{KEY_ALIAS}', '{b64_key}');")
    
    print(f"Exporting table 'customers' to encrypted Parquet file: {OUTPUT_FILE}...")
    
    query = f"""
    COPY customers TO '{OUTPUT_FILE}' (
        FORMAT PARQUET,
        ENCRYPTION_CONFIG {{
            footer_key: '{KEY_ALIAS}'
        }}
    );
    """
    con.execute(query)
    print("✅ Export completed successfully!")
    con.close()

if __name__ == "__main__":
    main()