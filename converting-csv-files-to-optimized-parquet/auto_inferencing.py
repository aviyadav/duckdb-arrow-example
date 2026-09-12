import threading
import time
from pathlib import Path

import duckdb
import psutil

csv_path = Path("data") / "events.csv"

# Ensure the source file exists before testing
if not csv_path.exists():
    raise FileNotFoundError(f"Please generate the dataset first. Missing: {csv_path}")

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
            current_mem = process.memory_info().rss     # Physical RAM in bytes
            peak_memory = max(peak_memory, current_mem)
        except Exception:  # noqa: BLE001 - monitor must never crash the run
            break
        time.sleep(0.005)       # Sample every 5ms for pricision

# Start the memory tracking thread
mem_thread = threading.Thread(target=monitor_memory, daemon=True)
mem_thread.start()

# --- START TIMING ---
start_time = time.perf_counter()


# Run the query using DuckDB's default settings (auto-inference active)
query = f"""
    SELECT
        country,
        ROUND(SUM(revenue), 2) AS total_revenue
    FROM read_csv(
        '{csv_path}',
        header = true,
        delim = ','
        -- no all_varchar or columns parameters = default auto-inference
    )
    GROUP BY country
    ORDER BY total_revenue DESC;
"""

# Fetch the results to force full query evaluation
results = con.execute(query).fetchall()

end_time = time.perf_counter()
# --- END TIMING ---

# Stop the memory tracking thread safely
monitor_active = False
mem_thread.join()

# --- PRINT RESULTS ---
total_duration = end_time - start_time
peak_mb = peak_memory / (1024 * 1024)

print("=" * 50)
print("DuckDB Auto-Inference Metrics:")
print("=" * 50)
print(f"Total Query Execution Time: {total_duration:.4f} seconds")
print(f"Peak Memory Usage:          {peak_mb:.2f} MB")
print("=" * 50)
print("\nQuery Results:")
for row in results:
    print(f"Country: {row[0]:<3} | Total Revenue: ${row[1]:,}")
