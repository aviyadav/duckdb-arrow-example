import threading
import time
from pathlib import Path

import duckdb
import psutil

# File paths
csv_path = Path("data") / "events.csv"
parquet_path = Path("data") / "events.parquet"

# Ensure the source file exists
if not csv_path.exists():
    raise FileNotFoundError(f"Please generate the CSV dataset first. Missing: {csv_path}")

con = duckdb.connect()

# --- BACKGROUND MEMORY MONITOR LOGIC ---
peak_memory = 0
monitor_active = True

def monitor_memory():
    """Continuously tracks the process memory to catch the peak spike."""
    global peak_memory
    process = psutil.Process()
    while monitor_active:
        try:
            current_mem = process.memory_info().rss
            peak_memory = max(peak_memory, current_mem)
        except Exception:  # noqa: BLE001 - monitor must never crash the run
            break
        time.sleep(0.005)

mem_thread = threading.Thread(target=monitor_memory, daemon=True)
mem_thread.start()

# --- START TIMING ---
start_time = time.perf_counter()

# Read CSV with explicit types (no auto-inference) and write to Parquet
con.execute(f"""
    COPY (
        SELECT
            event_date,
            country,
            channel,
            user_id,
            order_id,
            revenue
        FROM read_csv(
            '{csv_path}',
            header = true,
            delim = ',',
            columns = {{
                'event_date': 'DATE',
                'country': 'VARCHAR',
                'channel': 'VARCHAR',
                'user_id': 'INTEGER',
                'order_id': 'INTEGER',
                'revenue': 'DOUBLE'
            }}
        )
    ) TO '{parquet_path}' (FORMAT 'PARQUET', COMPRESSION 'ZSTD');
""")

end_time = time.perf_counter()
# --- END TIMING ---

monitor_active = False
mem_thread.join()

# --- PRINT RESULTS ---
total_duration = end_time - start_time
peak_mb = peak_memory / (1024 * 1024)
csv_size_mb = csv_path.stat().st_size / (1024 * 1024)
parquet_size_mb = parquet_path.stat().st_size / (1024 * 1024)

print("=" * 50)
print("CSV to Parquet Export Metrics (Explicit Typing):")
print("=" * 50)
print(f"Total Conversion Time:   {total_duration:.4f} seconds")
print(f"Peak Memory Usage:       {peak_mb:.2f} MB")
print(f"Original CSV Size:       {csv_size_mb:.2f} MB")
print(f"Optimized Parquet Size:  {parquet_size_mb:.2f} MB")
print("=" * 50)
print(f"File successfully saved to: {parquet_path}")
