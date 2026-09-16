import os
import secrets
import keyring
from keyring.errors import NoKeyringError

SERVICE_NAME = "medium.parquet.encrypt"
ACCOUNT_NAME = "transfer.parquet_encrypt_key"
ENV_VAR_NAME = "PARQUET_ENCRYPTION_KEY"

def main():
    # 1. Generate 32 cryptographically secure random bytes (256 bits) as hex
    hex_key = secrets.token_hex(32)
    
    # 2. Try to save to OS keyring, fallback to environment variable
    try:
        keyring.set_password(SERVICE_NAME, ACCOUNT_NAME, hex_key)
        print("✅ Key successfully generated and saved to OS keyring!")
    except NoKeyringError:
        print("⚠️  Warning: No OS keyring backend found (common in headless Linux/WSL).")
        print(f"➡️  Falling back to environment variable: {ENV_VAR_NAME}")
        print(f"Please add this to your terminal or .env file:")
        print(f"export {ENV_VAR_NAME}='{hex_key}'\n")
        # Set it in the current process so subsequent scripts work immediately
        os.environ[ENV_VAR_NAME] = hex_key
        
    print(f"Service: {SERVICE_NAME}")
    print(f"Account: {ACCOUNT_NAME}")
    print(f"Length: {len(hex_key)} characters (256 bits)")

if __name__ == "__main__":
    main()