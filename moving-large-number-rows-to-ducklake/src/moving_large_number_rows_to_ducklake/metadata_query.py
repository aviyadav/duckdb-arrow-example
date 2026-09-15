import duckdb

def main():
    # Connect and attach your DuckLake catalog
    con = duckdb.connect()
    con.execute("INSTALL ducklake; LOAD ducklake;")
    con.execute("ATTACH 'ducklake:metadata.ducklake' AS my_lake (DATA_PATH 'data');")
    con.execute("USE my_lake;")

    # 1. List attached database catalogs
    print("--- Attached Databases ---")
    df_databases = con.execute("SELECT * FROM duckdb_databases();").df()
    print(df_databases.to_string(index=False))

    print("\n--- Tables Managed in my_lake ---")
    # 2. List all tables managed inside the DuckLake catalog
    query_tables = """
    SELECT
        database_name,
        schema_name,
        table_name,
        internal,
        temporary
    FROM duckdb_tables()
    WHERE database_name = 'my_lake';
    """
    df_tables = con.execute(query_tables).df()
    print(df_tables.to_string(index=False))


if __name__ == "__main__":
    main()
