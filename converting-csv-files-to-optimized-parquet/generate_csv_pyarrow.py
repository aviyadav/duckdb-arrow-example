"""Generate a large events CSV using NumPy + PyArrow only (no DuckDB).

Data shape matches ``generate_csv_data.py`` so the two generators are
interchangeable, but generation is done in parallel batches and streamed to
disk. A bounded window of in-flight batches keeps resident memory roughly
constant regardless of the total row count, so this can produce hundreds of
millions of rows without an out-of-memory failure.
"""

from __future__ import annotations

import argparse
import os
import time
from concurrent.futures import Future, ProcessPoolExecutor
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.csv as pacsv

# --- Data definition (mirrors generate_csv_data.py) ---
COUNTRIES = np.array(["US", "UK", "DE", "FR", "IN", "JP"])
CHANNELS = np.array(["search", "social", "email", "direct"])

_DATE_START = np.datetime64("2024-01-01", "D")
_DATE_END = np.datetime64("2026-01-31", "D")
_N_DAYS = int((_DATE_END - _DATE_START).astype(np.int64)) + 1
_BASE_DAY = int(_DATE_START.astype(np.int64))  # days since the Unix epoch

SCHEMA = pa.schema(
    [
        pa.field("event_date", pa.date32()),
        pa.field("country", pa.string()),
        pa.field("channel", pa.string()),
        pa.field("user_id", pa.int32()),
        pa.field("order_id", pa.int32()),
        pa.field("revenue", pa.float64()),
    ]
)


def generate_batch(batch_index: int, count: int, base_seed: int) -> pa.Table:
    """Build one batch of rows as a PyArrow table.

    Runs in a worker process. Each batch gets its own RNG seed derived from
    ``base_seed`` so the output is reproducible and workers never share state.
    """
    rng = np.random.default_rng(base_seed + batch_index)

    # Random date within [2024-01-01, 2026-01-31], stored as days since epoch.
    day_offsets = rng.integers(0, _N_DAYS, size=count, dtype=np.int32)
    event_date = pa.array(_BASE_DAY + day_offsets, type=pa.date32())

    country = pa.array(
        np.take(COUNTRIES, rng.integers(0, len(COUNTRIES), size=count)),
        type=pa.string(),
    )
    channel = pa.array(
        np.take(CHANNELS, rng.integers(0, len(CHANNELS), size=count)),
        type=pa.string(),
    )

    # 1..200000 and 1..900000 inclusive.
    user_id = pa.array(
        rng.integers(1, 200_001, size=count, dtype=np.int32), type=pa.int32()
    )
    order_id = pa.array(
        rng.integers(1, 900_001, size=count, dtype=np.int32), type=pa.int32()
    )

    # Gamma-like skewed revenue: sum of two exponentials, 15% zeroed out.
    # Clip to avoid log(0) when the RNG yields an exact 0.0.
    revenue = (-np.log(np.clip(rng.random(count), 1e-12, None)) - np.log(
        np.clip(rng.random(count), 1e-12, None)
    )) * 30.0
    revenue[rng.random(count) < 0.15] = 0.0
    revenue = np.round(revenue, 2)
    revenue_col = pa.array(revenue, type=pa.float64())

    return pa.Table.from_arrays(
        [event_date, country, channel, user_id, order_id, revenue_col],
        schema=SCHEMA,
    )


def generate_csv(
    csv_path: Path,
    rows: int,
    batch_size: int,
    workers: int,
    base_seed: int,
) -> None:
    """Generate ``rows`` records into ``csv_path`` with bounded memory usage."""
    total_batches = (rows + batch_size - 1) // batch_size
    # Keep a small multiple of the pool size in flight: enough to stay busy,
    # bounded so we never accumulate the whole dataset in RAM.
    max_inflight = max(1, workers * 2)

    csv_path.parent.mkdir(parents=True, exist_ok=True)

    print("=" * 50)
    print("Generating events CSV (NumPy + PyArrow):")
    print("=" * 50)
    print(f"Output file:     {csv_path}")
    print(f"Rows:            {rows:,}")
    print(f"Batch size:      {batch_size:,}")
    print(f"Batches:         {total_batches:,}")
    print(f"Worker procs:    {workers}")
    print(f"Max in flight:   {max_inflight} batches")
    print("=" * 50)

    start_time = time.perf_counter()
    written = 0

    with (
        pacsv.CSVWriter(
            str(csv_path),
            SCHEMA,
            write_options=pacsv.WriteOptions(
                include_header=True, delimiter=",", quoting_style="none"
            ),
        ) as writer,
        ProcessPoolExecutor(max_workers=workers) as executor,
    ):
        pending: dict[int, Future[pa.Table]] = {}
        next_submit = 0
        next_write = 0

        while next_write < total_batches:
            # Fill the in-flight window.
            while next_submit < total_batches and len(pending) < max_inflight:
                count = min(batch_size, rows - next_submit * batch_size)
                pending[next_submit] = executor.submit(
                    generate_batch, next_submit, count, base_seed
                )
                next_submit += 1

            # Write strictly in order so the file stays deterministic.
            table = pending.pop(next_write).result()
            writer.write_table(table)
            written += table.num_rows
            del table  # release the batch before pulling the next one
            next_write += 1

            if next_write % 10 == 0 or next_write == total_batches:
                elapsed = time.perf_counter() - start_time
                rate = written / elapsed if elapsed else 0.0
                print(
                    f"\rWrote {written:,} / {rows:,} rows "
                    f"({rate:,.0f} rows/s)",
                    end="",
                    flush=True,
                )

    total_duration = time.perf_counter() - start_time
    size_mb = csv_path.stat().st_size / (1024 * 1024)

    print()
    print("=" * 50)
    print("Generation complete:")
    print(f"Rows written:  {written:,}")
    print(f"Total time:    {total_duration:.2f} seconds")
    print(f"Throughput:    {written / total_duration:,.0f} rows/s")
    print(f"File size:     {size_mb:.2f} MB")
    print(f"Saved to:      {csv_path}")
    print("=" * 50)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a large events CSV with NumPy + PyArrow (no DuckDB)."
    )
    parser.add_argument("--rows", type=int, default=100_000_000)
    parser.add_argument("--batch-size", type=int, default=1_000_000)
    parser.add_argument(
        "--workers",
        type=int,
        default=os.cpu_count() or 1,
        help="Number of worker processes (default: CPU count).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data") / "events_py.csv",
        help="Destination CSV path.",
    )
    parser.add_argument("--seed", type=int, default=42)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.rows <= 0:
        raise ValueError("--rows must be positive")
    if args.batch_size <= 0:
        raise ValueError("--batch-size must be positive")

    generate_csv(
        csv_path=args.output,
        rows=args.rows,
        batch_size=args.batch_size,
        workers=args.workers,
        base_seed=args.seed,
    )


if __name__ == "__main__":
    main()