#!/usr/bin/env python3
"""analyze_chimera_csv.py

Unified tool for analyzing Chimera CSV files with multiple modes:
  - space-usage: Calculate total space used by paths matching a pattern
  - user-summary: Aggregate storage usage by user (from 'user' column)
  - extract-paths: Extract file paths to a text file (filtered by pattern)

The CSV is expected to have columns:
  lfnpath;pnfsid;checksum;size;timestamp;uri;user

Example usage:
  # Calculate space used by a pattern
  python analyze_chimera_csv.py space-usage data.csv --pattern "/store/user/jhornung/CROWN"

  # Summarize storage by user
  python analyze_chimera_csv.py user-summary data.csv -o user_report.txt

  # Extract paths matching a pattern to a text file
  python analyze_chimera_csv.py extract-paths data.csv --pattern "/store/user/alice" -o paths.txt

  # Extract all paths from multiple CSVs
  python analyze_chimera_csv.py extract-paths "*.csv" --recursive
"""

from __future__ import annotations

import argparse
import glob
import os
import re
import sys
from typing import TextIO

# Check for pandas availability
def _ensure_pandas():
    """Check that pandas is available."""
    try:
        import pandas as pd  # type: ignore
        return pd
    except ImportError:
        print("ERROR: pandas is required for this script. Install with: pip install pandas", file=sys.stderr)
        raise SystemExit(3)


def human_readable(n: float, binary: bool = False) -> str:
    """Convert bytes to human-readable format.

    Args:
        n: Number of bytes
        binary: If True, use binary units (1024), else decimal (1000)

    Returns:
        Human-readable string like "1.23 TB"
    """
    base = 1024 if binary else 1000
    units = ["B", "KB", "MB", "GB", "TB", "PB", "EB"] if not binary else ["B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"]

    for unit in units:
        if abs(n) < base:
            return f"{n:.2f} {unit}"
        n /= base
    return f"{n:.2f} {'ZB' if not binary else 'ZiB'}"


def process_csv_space_usage(
    path: str,
    pattern: str,
    delimiter: str = ";",
    has_header: bool = True,
    path_col: int | str = 0,
    size_col: int | str = 3,
    regex: bool = False,
    chunksize: int = 100_000,
) -> tuple[int, int]:
    """Calculate total space used by rows matching a pattern.

    Args:
        path: Path to CSV file
        pattern: Pattern to match against path column
        delimiter: CSV delimiter
        has_header: Whether CSV has a header row
        path_col: Path column index (0-based) or name
        size_col: Size column index (0-based) or name
        regex: Treat pattern as regex
        chunksize: Rows per chunk for memory efficiency

    Returns:
        Tuple of (matched_row_count, total_bytes)
    """
    pd = _ensure_pandas()

    if not os.path.isfile(path):
        raise FileNotFoundError(path)

    header_val = 0 if has_header else None

    # Convert string indices to int if needed
    if isinstance(path_col, str) and path_col.isdigit():
        path_col = int(path_col)
    if isinstance(size_col, str) and size_col.isdigit():
        size_col = int(size_col)

    usecols = [path_col, size_col]
    read_kwargs = dict(sep=delimiter, header=header_val, usecols=usecols, dtype=str, na_filter=False)

    matcher = re.compile(pattern) if regex else None

    total = 0
    count = 0

    for chunk in pd.read_csv(path, chunksize=chunksize, **read_kwargs):
        s_path = chunk.iloc[:, 0].astype(str).str.strip()
        s_size = chunk.iloc[:, 1].astype(str).str.strip()

        if matcher is not None:
            mask = s_path.str.match(matcher)
        else:
            mask = s_path.str.contains(pattern, regex=False)

        filtered_sizes = s_size[mask]

        for v in filtered_sizes:
            try:
                total += int(v)
                count += 1
            except (ValueError, TypeError):
                continue

    return count, total


def process_csv_user_summary(
    path: str,
    delimiter: str = ";",
    has_header: bool = True,
    user_col: int | str = "user",
    size_col: int | str = "size",
    chunksize: int = 1_000_000,
    min_tb: float = 0.0,
) -> dict[str, int]:
    """Aggregate storage usage by user.

    Args:
        path: Path to CSV file
        delimiter: CSV delimiter
        has_header: Whether CSV has a header row
        user_col: User column index or name
        size_col: Size column index or name
        chunksize: Rows per chunk
        min_tb: Minimum TB to include in results (0 = all)

    Returns:
        Dictionary mapping user -> total_bytes
    """
    pd = _ensure_pandas()
    import numpy as np

    if not os.path.isfile(path):
        raise FileNotFoundError(path)

    header_val = 0 if has_header else None

    # Build dtype dict for efficient reading
    if has_header:
        dtype_dict = {
            "lfnpath": str,
            "pnfsid": str,
            "checksum": str,
            "size": np.uint64,
            "timestamp": np.uint32,
            "uri": str,
            "user": str,
        }
        usecols = [user_col, size_col] if isinstance(user_col, str) else None
    else:
        dtype_dict = str
        usecols = [user_col, size_col]

    user_sizes: dict[str, int] = {}

    try:
        for chunk in pd.read_csv(path, sep=delimiter, header=header_val, dtype=dtype_dict, chunksize=chunksize, na_filter=False):
            # Get user and size columns
            if has_header:
                user_series = chunk[user_col if isinstance(user_col, str) else chunk.columns[user_col]]
                size_series = chunk[size_col if isinstance(size_col, str) else chunk.columns[size_col]]
            else:
                user_series = chunk.iloc[:, user_col if isinstance(user_col, int) else 6]
                size_series = chunk.iloc[:, size_col if isinstance(size_col, int) else 3]

            # Group and sum
            grouped = pd.DataFrame({"user": user_series, "size": size_series}).groupby("user")["size"].sum()

            for user, size in grouped.items():
                user_sizes[user] = user_sizes.get(user, 0) + int(size)
    except Exception as e:
        raise ValueError(f"Failed to process {path}: {e}") from e

    return user_sizes


def extract_paths_from_csv(
    path: str,
    output_path: str,
    pattern: str = "",
    delimiter: str = ";",
    has_header: bool = True,
    path_col: int | str = 0,
    regex: bool = False,
    chunksize: int = 100_000,
    append: bool = False,
) -> int:
    """Extract file paths from CSV to a text file.

    Args:
        path: Path to CSV file
        output_path: Path to output text file
        pattern: Optional pattern to filter paths
        delimiter: CSV delimiter
        has_header: Whether CSV has a header row
        path_col: Path column index or name
        regex: Treat pattern as regex
        chunksize: Rows per chunk
        append: Append to output file instead of overwriting

    Returns:
        Number of paths written
    """
    pd = _ensure_pandas()

    if not os.path.isfile(path):
        raise FileNotFoundError(path)

    header_val = 0 if has_header else None

    if isinstance(path_col, str) and path_col.isdigit():
        path_col = int(path_col)

    usecols = [path_col]
    read_kwargs = dict(sep=delimiter, header=header_val, usecols=usecols, dtype=str, na_filter=False)

    matcher = re.compile(pattern) if regex and pattern else None
    written = 0
    mode = "a" if append else "w"

    with open(output_path, mode, encoding="utf-8") as out_f:
        for chunk in pd.read_csv(path, chunksize=chunksize, **read_kwargs):
            values = chunk.iloc[:, 0].astype(str).str.strip()
            values = values[values != ""]

            # Apply pattern filter if specified
            if pattern:
                if matcher is not None:
                    mask = values.str.match(matcher)
                else:
                    mask = values.str.contains(pattern, regex=False)
                values = values[mask]

            for v in values:
                out_f.write(v + "\n")
                written += 1

    return written


def cmd_space_usage(args: argparse.Namespace) -> int:
    """Handle space-usage subcommand."""
    csvfile = os.path.expanduser(args.csvfile)

    if not os.path.isfile(csvfile):
        print(f"ERROR: File not found: {csvfile}", file=sys.stderr)
        return 2

    try:
        count, total = process_csv_space_usage(
            csvfile,
            pattern=args.pattern,
            delimiter=args.delimiter,
            has_header=args.has_header,
            path_col=args.path_col,
            size_col=args.size_col,
            regex=args.regex,
            chunksize=args.chunksize,
        )
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 3

    print(f"File:         {csvfile}")
    print(f"Pattern:      {args.pattern}")
    print(f"Matched rows: {count:,}")
    print(f"Total bytes:  {total:,}  ({human_readable(total)})")

    return 0


def cmd_user_summary(args: argparse.Namespace) -> int:
    """Handle user-summary subcommand."""
    csvfile = os.path.expanduser(args.csvfile)

    if not os.path.isfile(csvfile):
        print(f"ERROR: File not found: {csvfile}", file=sys.stderr)
        return 2

    try:
        user_sizes = process_csv_user_summary(
            csvfile,
            delimiter=args.delimiter,
            has_header=args.has_header,
            user_col=args.user_col,
            size_col=args.size_col,
            chunksize=args.chunksize,
        )
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 3

    # Convert to sorted list
    results = [(user, size_bytes, size_bytes / 1e12) for user, size_bytes in user_sizes.items()]
    results.sort(key=lambda x: x[1], reverse=True)

    # Filter by minimum TB if specified
    if args.min_tb > 0:
        results = [(u, b, tb) for u, b, tb in results if tb >= args.min_tb]

    # Calculate column widths
    max_user_len = max((len(user) for user, _, _ in results), default=4)
    max_user_len = max(max_user_len, len("User"))
    size_col_width = max(18, len("Occupied Size [TB]"))

    # Prepare output
    lines = []
    header = f"{'User'.ljust(max_user_len)}\t{'Occupied Size [TB]'.rjust(size_col_width)}"
    lines.append(header)

    for user, size_bytes, size_tb in results:
        line = f"{user.ljust(max_user_len)}\t{size_tb:>{size_col_width}.2f}"
        lines.append(line)

    # Output
    if args.output and args.output != "-":
        with open(args.output, "w", encoding="utf-8") as f:
            for line in lines:
                f.write(line + "\n")
        print(f"Results written to '{args.output}'")
        print(f"Total users: {len(results)}")
    else:
        for line in lines:
            print(line)

    # Summary stats
    total_bytes = sum(b for _, b, _ in results)
    print(f"\nTotal storage: {human_readable(total_bytes)} ({total_bytes / 1e12:.2f} TB)")

    return 0


def cmd_extract_paths(args: argparse.Namespace) -> int:
    """Handle extract-paths subcommand."""
    # Handle glob pattern for input files
    input_files = glob.glob(args.csvfile, recursive=args.recursive)

    if not input_files:
        print(f"ERROR: No files match pattern: {args.csvfile}", file=sys.stderr)
        return 2

    input_files = sorted(input_files)
    total_written = 0

    for i, csvfile in enumerate(input_files):
        # Determine output file
        if args.output:
            output_path = args.output
            append = i > 0  # Append for subsequent files when single output
        else:
            # Create .txt file next to CSV
            output_path = os.path.splitext(csvfile)[0] + ".txt"
            append = False

        try:
            written = extract_paths_from_csv(
                csvfile,
                output_path,
                pattern=args.pattern,
                delimiter=args.delimiter,
                has_header=args.has_header,
                path_col=args.path_col,
                regex=args.regex,
                chunksize=args.chunksize,
                append=append,
            )
            total_written += written

            if args.verbose or not args.output:
                print(f"Wrote {written:>7} paths -> {output_path}")

        except Exception as e:
            print(f"ERROR processing {csvfile}: {e}", file=sys.stderr)
            if not args.continue_on_error:
                return 3

    if args.output:
        print(f"\nTotal: {total_written:,} paths written to {args.output}")
    else:
        print(f"\nTotal: {total_written:,} paths extracted from {len(input_files)} file(s)")

    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Analyze Chimera CSV files - calculate space, summarize by user, or extract paths",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    subparsers = parser.add_subparsers(dest="command", required=True, help="Available commands")

    # Common arguments
    common_parser = argparse.ArgumentParser(add_help=False)
    common_parser.add_argument("--delimiter", default=";", help="CSV delimiter (default: ';')")
    common_parser.add_argument("--no-header", action="store_true", help="CSV has no header row (default: header is expected)")
    common_parser.add_argument("--chunksize", type=int, default=100_000, help="Rows per chunk (default: 100000)")

    # ========== space-usage subcommand ==========
    space_parser = subparsers.add_parser(
        "space-usage",
        parents=[common_parser],
        help="Calculate total space used by paths matching a pattern",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s data.csv --pattern "/store/user/alice"
  %(prog)s data.csv --pattern "^/store/user/[^/]+/CROWN" --regex
""",
    )
    space_parser.add_argument("csvfile", help="Path to CSV file")
    space_parser.add_argument("--pattern", required=True, help="Pattern to match against path column")
    space_parser.add_argument("--regex", action="store_true", help="Treat pattern as regex")
    space_parser.add_argument("--path-col", default="0", help="Path column index or name (default: 0)")
    space_parser.add_argument("--size-col", default="3", help="Size column index or name (default: 3)")

    # ========== user-summary subcommand ==========
    user_parser = subparsers.add_parser(
        "user-summary",
        parents=[common_parser],
        help="Summarize storage usage by user",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s data.csv
  %(prog)s data.csv -o user_report.txt --min-tb 0.1
""",
    )
    user_parser.add_argument("csvfile", help="Path to CSV file")
    user_parser.add_argument("-o", "--output", help="Output file (default: stdout)")
    user_parser.add_argument("--user-col", default="user", help="User column name or index (default: 'user')")
    user_parser.add_argument("--size-col", default="size", help="Size column name or index (default: 'size')")
    user_parser.add_argument("--min-tb", type=float, default=0.0, help="Minimum TB to include in output (default: 0)")

    # ========== extract-paths subcommand ==========
    extract_parser = subparsers.add_parser(
        "extract-paths",
        parents=[common_parser],
        help="Extract file paths to a text file",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s data.csv -o paths.txt
  %(prog)s data.csv --pattern "/store/user/alice" -o alice_paths.txt
  %(prog)s "*.csv" --recursive
""",
    )
    extract_parser.add_argument("csvfile", help="Path to CSV file or glob pattern")
    extract_parser.add_argument("-o", "--output", help="Output text file (default: <csvfile>.txt)")
    extract_parser.add_argument("--pattern", default="", help="Pattern to filter paths (default: extract all)")
    extract_parser.add_argument("--regex", action="store_true", help="Treat pattern as regex")
    extract_parser.add_argument("--path-col", default="0", help="Path column index or name (default: 0)")
    extract_parser.add_argument("--recursive", action="store_true", help="Search for CSV files recursively")
    extract_parser.add_argument("--continue-on-error", action="store_true", help="Continue if a file fails")
    extract_parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")

    args = parser.parse_args(argv)

    # Handle --no-header flag: default is True (header expected), --no-header sets it to False
    args.has_header = not getattr(args, 'no_header', False)

    # Dispatch to subcommand
    if args.command == "space-usage":
        return cmd_space_usage(args)
    elif args.command == "user-summary":
        return cmd_user_summary(args)
    elif args.command == "extract-paths":
        return cmd_extract_paths(args)
    else:
        parser.print_help()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
