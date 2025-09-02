import duckdb
import polars as pl

df = pl.DataFrame({
    "city": ["New York", "Los Angeles", "Chicago", "Paris", "Tokyo", "Berlin"],
    "population": [8_419_600, 3_980_400, 2_716_000, 2_200_000, 14_000_000, 3_600_000]
})

conn = duckdb.connect()

conn.register("cities", df.to_arrow())

results = conn.execute("""
    SELECT city, population
    FROM cities
    WHERE population > 3_000_000
    ORDER BY population DESC
""").fetchdf()

print(results)

arrow_table = conn.execute("""
    SELECT city, population
    FROM cities
""").arrow()

print(type(arrow_table))

pl_df = pl.from_arrow(arrow_table)
print(pl_df)

pl_df.write_parquet("cities.parquet")

query = """
    SELECT city, population
    FROM read_parquet('cities.parquet')
    WHERE population < 3_000_000
"""

res = conn.execute(query).arrow().to_pandas()
print(res)