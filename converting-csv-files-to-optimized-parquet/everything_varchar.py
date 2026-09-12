from pathlib import Path

import duckdb

csv_path = Path("data") / "events.csv"
con = duckdb.connect()


print("Attempting to SUM raw VARCHAR strings without casting...\n")

try:
    # This query disables auto-inference and tries to sum texts string directly
    con.execute(f"""
        SELECT
            country,
            SUM(revenue) AS total_revenue
        FROM read_csv(
            '{csv_path}',
            header = true,
            delim = ',',
            all_varchar = true  -- Disables auto-inference, forcing revenue to VARCHAR
        )
        GROUP BY country;
    """)
except duckdb.BinderException as e:
    print("❌ EXPECTED ERROR CAUGHT:")
    print(e)
