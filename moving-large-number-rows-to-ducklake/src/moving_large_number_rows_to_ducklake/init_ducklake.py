import os
import time
import duckdb
import psutil

def main():
    process = psutil.Process(os.getpid())

    # Track initial resource metrics
    start_wall = time.perf_counter()
    start_cpu = process.cpu_times()
    start_io = process.io_counters() if hasattr(process, "io_counters") else None

    # --- Start DuckLake Processing ---
    con = duckdb.connect()

    # Step 1: Install and load DuckLake extension
    con.execute("INSTALL ducklake; LOAD ducklake;")

    # Step 2: Attach DuckLake catalog
    con.execute("""
        ATTACH 'ducklake:metadata.ducklake' AS my_lake (
            DATA_PATH 'data',
            OVERRIDE_DATA_PATH true
        );
    """)

    # Step 3: Switch context into the DuckLake catalog
    con.execute("USE my_lake;")

    # Step 4: Ingest the partitioned events dataset
    con.execute("""
        CREATE TABLE IF NOT EXISTS events AS
        SELECT *
        FROM read_parquet(
            'data/base_flowlogs_partitioned/**/*.parquet',
            hive_partitioning = true,
            union_by_name = true
        );
    """)

    print("DuckLake initialized successfully!")
    # ---------------------------------

    # Calculate resource metrics
    elapsed_sec = time.perf_counter() - start_wall
    end_cpu = process.cpu_times()
    end_io = process.io_counters() if hasattr(process, "io_counters") else None

    user_cpu = end_cpu.user - start_cpu.user
    sys_cpu = end_cpu.system - start_cpu.system
    peak_ram_mb = process.memory_info().rss / (1024 * 1024)

    print(f"\n--- Resource Execution Report ---")
    print(f"Elapsed Wall Time : {elapsed_sec:.2f} seconds")
    print(f"CPU Time (User)   : {user_cpu:.2f} seconds")
    print(f"CPU Time (System) : {sys_cpu:.2f} seconds")
    print(f"Peak RAM Usage    : {peak_ram_mb:.2f} MB")

    if start_io and end_io:
        read_mb = (end_io.read_bytes - start_io.read_bytes) / (1024 * 1024)
        written_mb = (end_io.write_bytes - start_io.write_bytes) / (1024 * 1024)
        print(f"Disk Read Volume  : {read_mb:.2f} MB")
        print(f"Disk Write Volume : {written_mb:.2f} MB")

if __name__ == "__main__":
    main()
