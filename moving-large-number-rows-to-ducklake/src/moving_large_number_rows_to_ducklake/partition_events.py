"""Convert events.parquet into a Hive-partitioned Parquet dataset.

Reads data/events.parquet and writes a Hive-style partitioned dataset to
data/base_flowlogs_partitioned, e.g.:

    data/base_flowlogs_partitioned/event_date=2024-01-01/events.parquet
    data/base_flowlogs_partitioned/event_date=2024-01-02/events.parquet
    ...

Partition columns are encoded in the directory names (standard Hive layout),
so they are reconstructed automatically when reading back with
`hive_partitioning = true`.

The write happens in two phases: DuckDB's parallel COPY ... PARTITION_BY does
the split, then each partition's per-thread chunk files are consolidated into
a single parquet file to avoid the small-files problem.
"""

import argparse
import os
import shutil
import sys
import time
from pathlib import Path

import duckdb
import psutil

DEFAULT_INPUT = Path("data/events.parquet")
DEFAULT_OUTPUT = Path("data/base_flowlogs_partitioned")
DEFAULT_PARTITION_COLUMNS = ["event_date"]


def sql_string(value: str) -> str:
    """Quote a value as a SQL string literal."""
    return "'" + value.replace("'", "''") + "'"


def convert(
    input_path: Path,
    output_path: Path,
    partition_columns: list[str],
    con: duckdb.DuckDBPyConnection,
) -> None:
    """Write the input parquet file as a Hive-partitioned parquet dataset."""
    partition_clause = ", ".join(partition_columns)
    con.execute(f"""
        COPY (SELECT * FROM read_parquet({sql_string(str(input_path.as_posix()))}))
        TO {sql_string(str(output_path.as_posix()))}
        (
            FORMAT PARQUET,
            PARTITION_BY ({partition_clause}),
            OVERWRITE_OR_IGNORE TRUE
        );
    """)


def consolidate(output_path: Path, con: duckdb.DuckDBPyConnection) -> None:
    """Merge per-thread chunk files so each partition holds a single file.

    The parallel partitioned writer flushes a new file whenever a thread's
    buffer fills, which yields thousands of tiny files. Rewriting each
    partition into one file keeps the dataset scan-friendly.
    """
    merged_name = "events.parquet"
    merged = 0
    for partition_dir in sorted(p for p in output_path.iterdir() if p.is_dir()):
        chunks = sorted(partition_dir.glob("*.parquet"))
        if len(chunks) == 1 and chunks[0].name == merged_name:
            continue
        staged = partition_dir / f"{merged_name}.tmp"
        con.execute(
            f"COPY (SELECT * FROM read_parquet({sql_string(str((partition_dir / '*.parquet').as_posix()))})) "
            f"TO {sql_string(str(staged.as_posix()))} (FORMAT PARQUET);"
        )
        for chunk in chunks:
            chunk.unlink()
        staged.replace(partition_dir / merged_name)
        merged += 1
    if merged:
        print(f"Consolidated chunks into one file for {merged:,} partitions.")


def dataset_stats(root: Path) -> tuple[int, int, float]:
    """Return (partition_dir_count, file_count, total_size_mb) for the dataset."""
    partition_dirs: set[str] = set()
    file_count = 0
    total_bytes = 0
    for file in root.rglob("*.parquet"):
        file_count += 1
        total_bytes += file.stat().st_size
        partition_dirs.add(file.parent.relative_to(root).as_posix())
    return len(partition_dirs), file_count, total_bytes / (1024 * 1024)


def row_count(parquet_path: str, con: duckdb.DuckDBPyConnection) -> int:
    """Count rows via parquet metadata (no full data scan)."""
    return con.execute(
        f"SELECT COUNT(*) FROM read_parquet({sql_string(parquet_path)})"
    ).fetchone()[0]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Convert a parquet file to a Hive-partitioned parquet dataset "
            "using DuckDB COPY ... PARTITION_BY."
        )
    )
    parser.add_argument(
        "--input", type=Path, default=DEFAULT_INPUT,
        help=f"Source parquet file (default: {DEFAULT_INPUT})",
    )
    parser.add_argument(
        "--output", type=Path, default=DEFAULT_OUTPUT,
        help=f"Target dataset directory (default: {DEFAULT_OUTPUT})",
    )
    parser.add_argument(
        "-p", "--partition-by", action="append",
        help=(
            "Column to partition by; repeat for nested partitions, e.g. "
            "-p country -p channel (default: event_date)"
        ),
    )
    parser.add_argument(
        "--overwrite", action="store_true",
        help="Delete the output directory first if it is not empty",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    input_path = args.input
    output_path = args.output
    partition_columns = args.partition_by or DEFAULT_PARTITION_COLUMNS

    if not input_path.is_file():
        sys.exit(f"error: input file not found: {input_path}")

    if output_path.exists() and any(output_path.iterdir()):
        if not args.overwrite:
            sys.exit(
                f"error: output directory '{output_path}' is not empty; "
                "pass --overwrite to replace it"
            )
        print(f"Removing existing dataset at {output_path} ...")
        shutil.rmtree(output_path)

    process = psutil.Process(os.getpid())
    start_wall = time.perf_counter()
    start_cpu = process.cpu_times()
    start_io = process.io_counters() if hasattr(process, "io_counters") else None

    con = duckdb.connect()
    # Letting go of input row order keeps the partitioned write streaming
    # instead of buffering, and allows fully parallel file writes.
    con.execute("SET preserve_insertion_order = false;")

    print(f"Partitioning {input_path} by {partition_columns} -> {output_path} ...")
    input_rows = row_count(str(input_path), con)
    convert(input_path, output_path, partition_columns, con)
    consolidate(output_path, con)

    glob_pattern = (output_path / "**" / "*.parquet").as_posix()
    output_rows = con.execute(
        "SELECT COUNT(*) FROM read_parquet("
        f"{sql_string(glob_pattern)}, hive_partitioning = TRUE)"
    ).fetchone()[0]
    con.close()

    partition_count, file_count, size_mb = dataset_stats(output_path)

    print("\n--- Partitioning Report ---")
    print(f"Input rows        : {input_rows:,}")
    print(f"Output rows       : {output_rows:,}")
    if input_rows != output_rows:
        sys.exit("error: row count mismatch between input and output dataset")
    print(f"Partition dirs    : {partition_count:,}")
    print(f"Parquet files     : {file_count:,}")
    print(f"Dataset size      : {size_mb:.2f} MB")

    sample = sorted(p.name for p in output_path.iterdir() if p.is_dir())[:3]
    if sample:
        print(f"Sample partitions : {', '.join(sample)} ...")

    elapsed_sec = time.perf_counter() - start_wall
    end_cpu = process.cpu_times()
    user_cpu = end_cpu.user - start_cpu.user
    sys_cpu = end_cpu.system - start_cpu.system
    peak_ram_mb = process.memory_info().rss / (1024 * 1024)

    print("\n--- Resource Execution Report ---")
    print(f"Elapsed Wall Time : {elapsed_sec:.2f} seconds")
    print(f"CPU Time (User)   : {user_cpu:.2f} seconds")
    print(f"CPU Time (System) : {sys_cpu:.2f} seconds")
    print(f"Peak RAM Usage    : {peak_ram_mb:.2f} MB")

    if start_io:
        end_io = process.io_counters()
        read_mb = (end_io.read_bytes - start_io.read_bytes) / (1024 * 1024)
        written_mb = (end_io.write_bytes - start_io.write_bytes) / (1024 * 1024)
        print(f"Disk Read Volume  : {read_mb:.2f} MB")
        print(f"Disk Write Volume : {written_mb:.2f} MB")


if __name__ == "__main__":
    main()
