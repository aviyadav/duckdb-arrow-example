import threading
import time
from pathlib import Path

import duckdb
import psutil

parquet_path = Path("data") / "events.parquet"

# Ensure the source file exists
if not parquet_path.exists():
    raise FileNotFoundError(
        f"Please run the Parquet export script first. Missing: {parquet_path}"
    )

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

# Run aggregation directly on the compressed Parquet file
query = f"""
    SELECT
        country,
        ROUND(SUM(revenue), 2) AS total_revenue
    FROM read_parquet('{parquet_path}')
    GROUP BY country
    ORDER BY total_revenue DESC;
"""

# Fetch the results to force full query evaluation
results = con.execute(query).fetchall()

end_time = time.perf_counter()
# --- END TIMING ---

monitor_active = False
mem_thread.join()

# --- PRINT RESULTS ---
total_duration = end_time - start_time
peak_mb = peak_memory / (1024 * 1024)

print("=" * 50)
print("DuckDB Parquet Performance Metrics:")
print("=" * 50)
print(f"Total Query Execution Time: {total_duration:.4f} seconds")
print(f"Peak Memory Usage:          {peak_mb:.2f} MB")
print("=" * 50)

print("\nQuery Results:")
for country, total_revenue in results:
    print(f"Country: {country:<3} | Total Revenue: ${total_revenue:,.2f}")
