#!/usr/bin/env python3
"""
WSPR CSV Ingester using clickhouse-driver and pandas.

Optimizations:
  - pandas read_csv with chunksize for streaming
  - NumPy vectorized conversions for SNR and frequency
  - clickhouse-driver native protocol with INSERT FORMAT TabSeparated
  - LZ4 compression for network transfer

Usage:
    python wspr_ingest.py /path/to/wsprspots-2020-12.csv.gz
    python wspr_ingest.py /path/to/directory/
"""

import sys
import os
import gzip
import time
import argparse
from pathlib import Path
from datetime import datetime
from typing import Iterator, List

import numpy as np
import pandas as pd
from clickhouse_driver import Client


# =============================================================================
# Configuration
# =============================================================================

CHUNK_SIZE = 1_000_000  # 1M rows per chunk
CH_BATCH_SIZE = 5_000_000  # Flush to ClickHouse every 5M rows

# Column names for WSPR CSV
WSPR_COLUMNS = [
    'id', 'timestamp', 'reporter', 'reporter_grid', 'snr',
    'frequency', 'callsign', 'grid', 'power', 'drift',
    'distance', 'azimuth', 'band', 'version', 'code'
]

# Data types for pandas
WSPR_DTYPES = {
    'id': 'uint64',
    'timestamp': 'int64',
    'reporter': 'str',
    'reporter_grid': 'str',
    'snr': 'int8',
    'frequency': 'float64',
    'callsign': 'str',
    'grid': 'str',
    'power': 'int8',
    'drift': 'int8',
    'distance': 'uint32',
    'azimuth': 'uint16',
    'band': 'int32',
    'version': 'str',
    'code': 'uint8'
}


# =============================================================================
# Sanitization (vectorized with NumPy)
# =============================================================================

def sanitize_string_column(series: pd.Series) -> pd.Series:
    """Remove quotes and brackets from string column (vectorized)."""
    return series.str.strip('"\'`<>[]{}()')


def convert_frequency_to_hz(series: pd.Series) -> np.ndarray:
    """Convert frequency from MHz to Hz (vectorized with NumPy)."""
    return (series.values * 1_000_000).astype(np.uint64)


def normalize_grid(series: pd.Series) -> pd.Series:
    """Uppercase grid locators (vectorized)."""
    return series.str.upper()


# =============================================================================
# File Processing
# =============================================================================

def read_wspr_csv(file_path: str, chunksize: int = CHUNK_SIZE) -> Iterator[pd.DataFrame]:
    """
    Stream WSPR CSV file in chunks using pandas.
    Handles both gzipped and plain CSV files.
    """
    # Determine if gzipped
    compression = 'gzip' if file_path.endswith('.gz') else None

    # Read CSV in chunks
    chunks = pd.read_csv(
        file_path,
        names=WSPR_COLUMNS,
        dtype=WSPR_DTYPES,
        compression=compression,
        chunksize=chunksize,
        na_values=['', 'NA', 'NaN'],
        keep_default_na=False,
        on_bad_lines='skip',  # Skip malformed rows
        low_memory=False
    )

    return chunks


def process_chunk(df: pd.DataFrame) -> pd.DataFrame:
    """
    Process a chunk of WSPR data with vectorized operations.

    Optimizations:
      - NumPy vectorized frequency conversion
      - Pandas vectorized string operations
      - In-place modifications where possible
    """
    # Sanitize string columns (vectorized)
    df['reporter'] = sanitize_string_column(df['reporter'].fillna(''))
    df['callsign'] = sanitize_string_column(df['callsign'].fillna(''))

    # Normalize grid locators (vectorized)
    df['reporter_grid'] = normalize_grid(df['reporter_grid'].fillna(''))
    df['grid'] = normalize_grid(df['grid'].fillna(''))

    # Convert frequency from MHz to Hz (NumPy vectorized)
    df['frequency'] = convert_frequency_to_hz(df['frequency'].fillna(0))

    # Fill other NaN values
    df['version'] = df['version'].fillna('')
    df['code'] = df['code'].fillna(0).astype(np.uint8)

    # Convert timestamp to datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s', utc=True)

    # Add mode column (WSPR for all)
    df['mode'] = 'WSPR'

    # Add column_count (number of columns in original CSV)
    df['column_count'] = 15

    return df


# =============================================================================
# ClickHouse Operations
# =============================================================================

def get_clickhouse_client(host: str = 'localhost', port: int = 9000,
                         database: str = 'wspr') -> Client:
    """Create ClickHouse client with optimized settings."""
    return Client(
        host=host,
        port=port,
        database=database,
        settings={
            'max_execution_time': 60,
            'async_insert': 1,
            'wait_for_async_insert': 0,
            'async_insert_max_data_size': 100000000,
            'async_insert_busy_timeout_ms': 1000,
            'max_insert_block_size': 1048576,
        },
        compression='lz4'
    )


def truncate_partition(client: Client, table: str, file_path: str) -> None:
    """Drop partition based on filename (YYYYMM)."""
    filename = os.path.basename(file_path)

    # Extract YYYY-MM from wsprspots-YYYY-MM.csv.gz
    if filename.startswith('wsprspots-') and len(filename) > 17:
        year_month = filename[10:17]  # YYYY-MM
        parts = year_month.split('-')
        if len(parts) == 2:
            partition = parts[0] + parts[1]  # YYYYMM
            try:
                client.execute(f"ALTER TABLE {table} DROP PARTITION '{partition}'")
                print(f"Truncated partition {partition}")
            except Exception as e:
                if 'NO_SUCH_DATA_PART' not in str(e) and 'not found' not in str(e):
                    print(f"Partition truncate warning: {e}")


def insert_dataframe(client: Client, table: str, df: pd.DataFrame) -> int:
    """
    Insert DataFrame into ClickHouse using native protocol.

    Uses INSERT INTO ... VALUES format for optimal performance.
    """
    # Column order for INSERT
    columns = [
        'id', 'timestamp', 'reporter', 'reporter_grid', 'snr',
        'frequency', 'callsign', 'grid', 'power', 'drift',
        'distance', 'azimuth', 'band', 'mode', 'version', 'code', 'column_count'
    ]

    # Prepare data as list of tuples
    data = df[columns].values.tolist()

    # Insert using parameterized query
    query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES"
    client.execute(query, data)

    return len(data)


# =============================================================================
# Main Pipeline
# =============================================================================

def ingest_file(client: Client, file_path: str, table: str = 'spots') -> tuple:
    """
    Ingest a single WSPR CSV file into ClickHouse.

    Returns: (rows_processed, elapsed_time)
    """
    print(f"Processing: {os.path.basename(file_path)}")

    # Truncate partition for idempotent ingestion
    truncate_partition(client, table, file_path)

    start_time = time.time()
    total_rows = 0
    pending_rows = 0
    pending_df = None

    # Process file in chunks
    for chunk_num, chunk in enumerate(read_wspr_csv(file_path)):
        # Process chunk with vectorized operations
        processed = process_chunk(chunk)

        # Accumulate chunks
        if pending_df is None:
            pending_df = processed
        else:
            pending_df = pd.concat([pending_df, processed], ignore_index=True)

        pending_rows = len(pending_df)

        # Flush to ClickHouse when batch is large enough
        if pending_rows >= CH_BATCH_SIZE:
            rows = insert_dataframe(client, table, pending_df)
            total_rows += rows

            elapsed = time.time() - start_time
            mrps = total_rows / elapsed / 1_000_000
            print(f"  Flushed {rows:,} rows (total: {total_rows:,}, {mrps:.2f} Mrps)")

            pending_df = None
            pending_rows = 0

    # Flush remaining rows
    if pending_df is not None and len(pending_df) > 0:
        rows = insert_dataframe(client, table, pending_df)
        total_rows += rows

    elapsed = time.time() - start_time
    return total_rows, elapsed


def discover_files(path: str) -> List[str]:
    """Discover WSPR CSV files in path."""
    path = Path(path)

    if path.is_file():
        return [str(path)]

    files = []
    for ext in ['*.csv.gz', '*.csv']:
        files.extend(path.glob(ext))

    return sorted([str(f) for f in files])


def main():
    parser = argparse.ArgumentParser(
        description='WSPR CSV Ingester using pandas and clickhouse-driver'
    )
    parser.add_argument('path', help='Path to CSV file or directory')
    parser.add_argument('--host', default='localhost', help='ClickHouse host')
    parser.add_argument('--port', type=int, default=9000, help='ClickHouse port')
    parser.add_argument('--database', default='wspr', help='ClickHouse database')
    parser.add_argument('--table', default='spots', help='ClickHouse table')
    parser.add_argument('--chunk-size', type=int, default=CHUNK_SIZE,
                        help=f'Rows per chunk (default: {CHUNK_SIZE:,})')

    args = parser.parse_args()

    print("=" * 60)
    print("WSPR Python Ingester")
    print("=" * 60)
    print(f"Input: {args.path}")
    print(f"ClickHouse: {args.host}:{args.port}/{args.database}.{args.table}")
    print(f"Chunk Size: {args.chunk_size:,} rows")
    print()

    # Discover files
    files = discover_files(args.path)
    if not files:
        print("Error: No CSV files found")
        sys.exit(1)

    print(f"Found {len(files)} file(s) to process")
    print()

    # Connect to ClickHouse
    client = get_clickhouse_client(
        host=args.host,
        port=args.port,
        database=args.database
    )

    # Process files
    overall_start = time.time()
    total_rows = 0

    for file_path in files:
        try:
            rows, elapsed = ingest_file(client, file_path, args.table)
            total_rows += rows

            mrps = rows / elapsed / 1_000_000 if elapsed > 0 else 0
            print(f"  Completed: {rows:,} rows in {elapsed:.1f}s ({mrps:.2f} Mrps)")
            print()
        except Exception as e:
            print(f"  Error: {e}")
            continue

    # Final statistics
    overall_elapsed = time.time() - overall_start
    overall_mrps = total_rows / overall_elapsed / 1_000_000 if overall_elapsed > 0 else 0

    print("=" * 60)
    print("Final Statistics")
    print("=" * 60)
    print(f"Total Rows: {total_rows:,}")
    print(f"Elapsed: {overall_elapsed:.1f}s")
    print(f"Throughput: {overall_mrps:.2f} Mrps")
    print("=" * 60)


if __name__ == '__main__':
    main()
