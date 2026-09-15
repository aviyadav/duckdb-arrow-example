import sys
import time

import duckdb

try:
    import resource
except ImportError:
    resource = None


def run_analysis():
    print("Connecting to DuckLake catalog...")

    # Establish connection to DuckDB
    conn = duckdb.connect()

    # Load DuckLake extension and attach catalog
    conn.execute("INSTALL ducklake; LOAD ducklake;")
    conn.execute(
        "ATTACH 'ducklake:metadata.ducklake' AS my_lake (DATA_PATH 'data', READ_ONLY true);"
    )
    conn.execute("USE my_lake;")

    # Aggregate event revenue by calendar month.
    query = """
    SELECT
        YEAR(event_date) AS event_year,
        MONTH(event_date) AS event_month,
        ROUND(SUM(revenue), 2) AS total_revenue
    FROM events
    GROUP BY event_year, event_month
    ORDER BY event_year DESC, event_month DESC;
    """

    print("Executing query and gathering resource metrics...\n")

    # Record baseline resource usage
    start_wall = time.perf_counter()
    start_usage = (
        resource.getrusage(resource.RUSAGE_SELF) if resource is not None else None
    )

    # Execute query and fetch results
    result_relation = conn.sql(query)
    results = result_relation.fetchall()

    # Record ending resource usage
    end_wall = time.perf_counter()
    end_usage = (
        resource.getrusage(resource.RUSAGE_SELF) if resource is not None else None
    )

    # Print Query Results
    print("--- QUERY RESULTS ---")
    print(f"{'Year':<6} | {'Month':<6} | {'Total Revenue':>15}")
    print("-" * 34)
    for year, month, total_revenue in results:
        print(f"{year:<6} | {month:<6} | {total_revenue:>15,.2f}")
    print("-" * 34 + "\n")

    # Calculate and Print Performance Metrics
    execution_time_ms = (end_wall - start_wall) * 1000
    print("--- PERFORMANCE METRICS ---")
    print(f"Total Execution Time (Wall-clock): {execution_time_ms:.2f} ms")

    if start_usage is not None and end_usage is not None:
        user_cpu = (end_usage.ru_utime - start_usage.ru_utime) * 1000
        sys_cpu = (end_usage.ru_stime - start_usage.ru_stime) * 1000

        if sys.platform == "darwin":
            peak_memory_mb = end_usage.ru_maxrss / 1024.0 / 1024.0
        else:
            peak_memory_mb = end_usage.ru_maxrss / 1024.0

        print(f"User CPU Time                  : {user_cpu:.2f} ms")
        print(f"System CPU Time                : {sys_cpu:.2f} ms")
        print(f"Peak Process Memory (Max RSS)  : {peak_memory_mb:.2f} MB")

    conn.close()


if __name__ == "__main__":
    run_analysis()
