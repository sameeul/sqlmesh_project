#!/usr/bin/env python3
"""
Stream a tab-separated .csv file into parquet without loading all data in memory.
Defaults target OMOP concept_ancestor.csv -> concept_ancestor.parquet.
"""

from __future__ import annotations

import argparse
from typing import Iterator

import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.parquet as pq


def _batch_reader(path: str, block_size: int) -> Iterator[pa.RecordBatch]:
    read_options = pv.ReadOptions(block_size=block_size, use_threads=True)
    parse_options = pv.ParseOptions(delimiter="\t")
    convert_options = pv.ConvertOptions(strings_can_be_null=False)
    with pv.open_csv(
        path,
        read_options=read_options,
        parse_options=parse_options,
        convert_options=convert_options,
    ) as reader:
        for batch in reader:
            yield batch


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Convert tab-separated CSV to parquet using streaming batches."
    )
    parser.add_argument(
        "--csv",
        default="models/step04_aux/step04_aux_files/omop/concept_ancestor.csv",
        help="Path to tab-separated CSV file",
    )
    parser.add_argument(
        "--parquet",
        default="models/step04_aux/step04_aux_files/omop/concept_ancestor.parquet",
        help="Destination parquet path",
    )
    parser.add_argument(
        "--block-size",
        type=int,
        default=1 << 20,
        help="Bytes to read per block (default: 1 MiB)",
    )
    args = parser.parse_args()

    writer = None
    total_rows = 0
    total_columns = 0
    try:
        for batch in _batch_reader(args.csv, args.block_size):
            if writer is None:
                writer = pq.ParquetWriter(args.parquet, batch.schema)
                total_columns = batch.num_columns
            writer.write_batch(batch)
            total_rows += batch.num_rows
    finally:
        if writer is not None:
            writer.close()

    print(f"Rows written: {total_rows:,}")
    print(f"Columns: {total_columns}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
