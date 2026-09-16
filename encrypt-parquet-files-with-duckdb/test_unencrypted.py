import duckdb

INPUT_FILE = "customers_encrypted.parquet"

def test_duckdb_unencrypted():
    print("--- 1. Testing DuckDB (Standard read_parquet) ---")
    try:
        con = duckdb.connect()
        con.execute(f"SELECT * FROM read_parquet('{INPUT_FILE}') LIMIT 5;").fetchall()
        print("SUCCESS (Unexpected): File was read without encryption!")
    except Exception as e:
        print("FAILED (Expected): DuckDB refused to read the encrypted file.")
        print(f"Error details: {e}\n")

def test_pyarrow_unencrypted():
    print("--- 2. Testing PyArrow (pq.read_table) ---")
    try:
        import pyarrow.parquet as pq
        pq.read_table(INPUT_FILE)
        print("SUCCESS (Unexpected): File was read without encryption!")
    except ImportError:
        print("SKIPPED: pyarrow is not installed.\n")
    except Exception as e:
        print("FAILED (Expected): PyArrow refused to read the encrypted file.")
        print(f"Error details: {e}\n")

def test_fastparquet_unencrypted():
    print("--- 3. Testing FastParquet (ParquetFile) ---")
    try:
        from fastparquet import ParquetFile
        ParquetFile(INPUT_FILE)
        print("SUCCESS (Unexpected): File was read without encryption!")
    except ImportError:
        print("SKIPPED: fastparquet is not installed.\n")
    except Exception as e:
        print("FAILED (Expected): FastParquet refused to read the encrypted file.")
        print(f"Error details: {e}\n")

def inspect_raw_bytes():
    print("--- 4. Inspecting Raw Magic Bytes at End-of-File ---")
    try:
        with open(INPUT_FILE, "rb") as f:
            f.seek(-4, 2) # Go to the last 4 bytes of the file
            magic_bytes = f.read(4)
            
        print(f"Last 4 bytes of file: {magic_bytes}")
        if magic_bytes == b"PAR1":
            print("Result: File has standard unencrypted Parquet magic bytes ('PAR1').")
        elif magic_bytes == b"PARE":
            print("Result: File has modular encryption magic bytes ('PARE').")
        else:
            print("Result: File footer is fully encrypted (magic bytes obscured).")
    except Exception as e:
        print(f"Error inspecting file bytes: {e}")

def main():
    print(f"Attempting unencrypted reads on: {INPUT_FILE}\n")
    test_duckdb_unencrypted()
    test_pyarrow_unencrypted()
    test_fastparquet_unencrypted()
    inspect_raw_bytes()

if __name__ == "__main__":
    main()