import duckdb


def main():
    con = duckdb.connect()
    con.execute("INSTALL ducklake; LOAD ducklake;")
    con.execute("ATTACH 'ducklake:metadata.ducklake' AS my_lake (DATA_PATH 'data');")
    con.execute("USE my_lake;")

    # DuckLake tables are stored as Parquet rather than native DuckDB blocks, so
    # pragma_storage_info() has no rows for them. Read the physical Parquet footer
    # metadata to report exact compressed and uncompressed bytes per column.
    query = """
    SELECT
        path_in_schema AS column_name,
        type AS physical_type,
        compression,
        COUNT(DISTINCT file_name) AS file_count,
        SUM(num_values) AS value_count,
        ROUND(SUM(total_compressed_size) / 1024.0 / 1024.0, 2) AS compressed_mb,
        ROUND(SUM(total_uncompressed_size) / 1024.0 / 1024.0, 2) AS uncompressed_mb,
        ROUND(
            100.0 * SUM(total_compressed_size)
            / NULLIF(SUM(total_uncompressed_size), 0),
            2
        ) AS compressed_percent
    FROM parquet_metadata('data/main/events/*.parquet')
    GROUP BY path_in_schema, type, compression
    ORDER BY compressed_mb DESC;
    """

    result = con.execute(query)
    columns = [description[0] for description in result.description]
    rows = result.fetchall()

    if not rows:
        raise SystemExit("No DuckLake event Parquet files found in data/main/events")

    widths = [len(column) for column in columns]
    for row in rows:
        for index, value in enumerate(row):
            widths[index] = max(widths[index], len(str(value)))

    print("  ".join(column.ljust(widths[index]) for index, column in enumerate(columns)))
    print("  ".join("-" * width for width in widths))
    for row in rows:
        print("  ".join(str(value).ljust(widths[index]) for index, value in enumerate(row)))

    con.close()


if __name__ == "__main__":
    main()
