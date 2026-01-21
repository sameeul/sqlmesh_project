#!/usr/bin/env python3
"""
Convert .tsv files in models/step04_aux/step04_aux_files/act_ref to .csv files.

Usage examples:
  - Convert all TSVs in the default act_ref folder:
      scripts/tsv_to_csv_act_ref.py

  - Convert a single file and write to a specific output:
      scripts/tsv_to_csv_act_ref.py --file models/.../race_xwalk_table.tsv

The script streams rows and doesn't load entire files into memory.
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path
from typing import Iterable, Tuple


DEFAULT_DIR = Path("models/step04_aux/step04_aux_files/act_ref")


def tsv_to_csv_path(tsv_path: Path, dest_dir: Path | None = None) -> Path:
    dest_dir = dest_dir or tsv_path.parent
    return (dest_dir / tsv_path.stem).with_suffix(".csv")


def convert_file(tsv: Path, csv_out: Path) -> int:
    """Stream-convert a TSV to CSV. Returns number of rows written."""
    csv_out.parent.mkdir(parents=True, exist_ok=True)
    rows = 0
    # Use utf-8 with surrogateescape for robustness with arbitrary bytes
    with tsv.open("r", encoding="utf-8", errors="surrogateescape", newline="") as fin, \
        csv_out.open("w", encoding="utf-8", newline="") as fout:
        reader = csv.reader(fin, delimiter="\t")
        writer = csv.writer(fout)
        for row in reader:
            writer.writerow(row)
            rows += 1
    return rows


def find_tsv_files(src_dir: Path) -> Iterable[Path]:
    if not src_dir.exists():
        return ()
    return sorted(p for p in src_dir.iterdir() if p.is_file() and p.suffix.lower() in {".tsv", ".txt", ".csv"} )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Convert TSV files in the act_ref folder to CSV files (streaming).",
    )
    parser.add_argument(
        "--dir",
        type=Path,
        default=DEFAULT_DIR,
        help="Directory containing .tsv files (default: %(default)s)",
    )
    parser.add_argument(
        "--file",
        type=Path,
        help="Optional single TSV file to convert instead of the whole directory",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        help="Optional output directory to place converted CSVs (defaults to source dir)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing CSV files if present",
    )
    args = parser.parse_args(argv)

    to_convert: list[Tuple[Path, Path]] = []

    if args.file:
        src = args.file
        if not src.exists():
            print(f"File not found: {src}", file=sys.stderr)
            return 2
        dst = args.out_dir and tsv_to_csv_path(src, args.out_dir) or tsv_to_csv_path(src)
        to_convert.append((src, dst))
    else:
        src_dir = args.dir
        files = list(find_tsv_files(src_dir))
        if not files:
            print(f"No TSV files found in: {src_dir}")
            return 0
        for src in files:
            # Only treat files with tab-separated content as TSV; many files may be .csv already
            dst = args.out_dir and tsv_to_csv_path(src, args.out_dir) or tsv_to_csv_path(src)
            to_convert.append((src, dst))

    converted = 0
    for src, dst in to_convert:
        if dst.exists() and not args.overwrite:
            print(f"Skipping (exists): {dst}")
            continue
        try:
            rows = convert_file(src, dst)
            print(f"Converted: {src} -> {dst} ({rows:,} rows)")
            converted += 1
        except Exception as e:
            print(f"Failed to convert {src}: {e}", file=sys.stderr)

    print(f"Total files converted: {converted}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
