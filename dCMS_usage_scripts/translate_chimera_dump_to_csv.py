#!/usr/bin/env python3
"""translate_chimera_dump_to_csv.py

Translate a Chimera dump (custom text format) into a CSV file with columns:
  lfnpath;pnfsid;checksum;size;timestamp;uri;user

The 'user' column is extracted from /store/user/<username>/ patterns in the path.
For /store/user/rucio/ paths or paths without a user match, a default value is used.

The script is configurable for:
  - Input chimera dump file(s) (glob patterns supported)
  - Prefix to remove from paths
  - Paths to include/neglect (multiple filter options)
  - Custom regex for user extraction
  - Default user when no match is found

Example usage:
  # Basic usage
  python translate_chimera_dump_to_csv.py chimera_* /pnfs/gridka.de/cms/disk-only -o output.csv

  # Only include dCMS storage (exclude rucio)
  python translate_chimera_dump_to_csv.py chimera_* /pnfs/gridka.de/cms/disk-only -o output.csv \
      --include "/pnfs/gridka.de/cms/disk-only/store/user" \
      --exclude "/pnfs/gridka.de/cms/disk-only/store/user/rucio"

  # Neglect tape and certain paths
  python translate_chimera_dump_to_csv.py chimera_* /pnfs/gridka.de/cms/disk-only -o output.csv \
      --exclude "/pnfs/gridka.de/cms/tape/" \
      --exclude "/store/test" \
      --exclude "/store/temp"
"""

from __future__ import annotations

import argparse
import glob
import os
import re
import sys
from pathlib import Path, PurePath
from typing import TextIO

# Default regex to extract username from /store/user/<username>/
USER_RE_DEFAULT = re.compile(r"/store/user/([^/]+)/")

# Default user when no match found
DEFAULT_USER = "CMS"


def is_subpath_of(child: str, parent: str) -> bool:
    """Check if child path is a subpath of (or equal to) parent path.

    Args:
        child: The potential child/subpath
        parent: The potential parent path

    Returns:
        True if child is equal to or a subdirectory of parent
    """
    try:
        PurePath(child).relative_to(parent)
        return True
    except ValueError:
        return False


def extract_user(path: str, user_regex: re.Pattern | None = None, default_user: str = DEFAULT_USER) -> str:
    """Extract username from path using regex.

    For /store/user/<username>/ patterns, extracts <username>.
    For /store/user/rucio/ or non-matching paths, returns default_user.

    Args:
        path: The file path to extract user from
        user_regex: Compiled regex with one capture group for username
        default_user: Value to return when no match is found

    Returns:
        Extracted username or default_user
    """
    regex = user_regex or USER_RE_DEFAULT
    match = regex.search(path)
    if match:
        username = match.group(1)
        # Treat 'rucio' as CMS (central) storage
        if username.lower() == "rucio":
            return default_user
        return username
    return default_user


def compute_include_overrides(include_paths: list[str], exclude_paths: list[str]) -> dict[str, list[str]]:
    """Precompute which include paths override which exclude paths.

    An include path overrides an exclude path if the include is more specific
    (i.e., the include is a subdirectory of the exclude).

    Args:
        include_paths: List of include paths
        exclude_paths: List of exclude paths

    Returns:
        Dictionary mapping each exclude path to the list of include paths that override it
    """
    overrides: dict[str, list[str]] = {}
    for exc in exclude_paths:
        overrides[exc] = [
            inc for inc in include_paths
            if inc != exc and is_subpath_of(inc, exc)
        ]
    return overrides


def should_process_path(
    path: str,
    include_paths: list[str],
    exclude_paths: list[str],
    include_overrides: dict[str, list[str]],
) -> bool:
    """Check if a path should be processed based on include/exclude paths.

    Logic:
    - If include_paths is specified, path must match at least one include path
    - If exclude_paths is specified, path must not match any exclude path
      UNLESS the path matches an include path that is MORE SPECIFIC than
      the matching exclude path (precomputed in include_overrides).

    This allows for "exclude A except B" logic by using:
      --exclude A --include B  (where B is a subdirectory of A)

    Args:
        path: The path to check
        include_paths: List of paths that path must contain (OR logic)
        exclude_paths: List of paths that path must not contain (AND logic)
        include_overrides: Precomputed mapping of excludes to their overriding includes

    Returns:
        True if path should be processed, False otherwise
    """
    # Find all matching include paths
    matching_includes = [p for p in include_paths if p in path] if include_paths else []

    # Find all matching exclude paths
    matching_excludes = [p for p in exclude_paths if p in path] if exclude_paths else []

    # Check exclude patterns
    if matching_excludes:
        # Path matches at least one exclude pattern
        # Check if there's a MORE SPECIFIC include pattern that saves it
        saved_by_include = False
        for exc in matching_excludes:
            # Get includes that override this exclude (precomputed)
            overriding_includes = include_overrides.get(exc, [])
            # Check if any of those overriding includes match the path
            if any(inc in path for inc in overriding_includes):
                saved_by_include = True
                break

        if not saved_by_include:
            return False  # Excluded and no more-specific include saves it

    # If include paths are specified, path must match at least one
    if include_paths and not matching_includes:
        return False

    return True


def process_chimera_dump(
    input_files: list[str],
    output_file: TextIO,
    prefix_to_remove: str,
    include_paths: list[str],
    exclude_paths: list[str],
    user_regex: re.Pattern | None = None,
    default_user: str = DEFAULT_USER,
    add_header: bool = True,
    verbose: bool = False,
) -> tuple[int, int]:
    """Process chimera dump files and write CSV output.

    Args:
        input_files: List of input file paths (chimera dumps)
        output_file: File handle to write CSV output
        prefix_to_remove: Prefix to remove from paths (e.g., /pnfs/gridka.de/cms)
        include_paths: Paths that must be contained in file paths
        exclude_paths: Paths that must not be contained in file paths
        user_regex: Custom regex for user extraction
        default_user: Default user when no match found
        add_header: Whether to add CSV header
        verbose: Print progress information

    Returns:
        Tuple of (total_lines_processed, total_lines_written)
    """
    if add_header:
        output_file.write("lfnpath;pnfsid;checksum;size;timestamp;uri;user\n")

    # Precompute which includes override which excludes
    include_overrides = compute_include_overrides(include_paths, exclude_paths)

    current_pnfs_dir: str | None = None
    lines_processed = 0
    lines_written = 0
    skipped_include = 0
    skipped_exclude = 0
    skipped_checksum = 0
    skipped_other = 0  # empty lines, directory markers, malformed lines

    for input_file in input_files:
        if verbose:
            print(f"Processing: {input_file}", file=sys.stderr)

        with open(input_file, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                lines_processed += 1
                line = line.strip()

                if not line:
                    skipped_other += 1
                    continue

                # Lines starting with /pnfs are directory markers
                if line.startswith("/pnfs"):
                    current_pnfs_dir = line
                    skipped_other += 1
                    continue

                if current_pnfs_dir is None:
                    skipped_other += 1
                    continue

                # Check if current directory should be processed
                if not should_process_path(current_pnfs_dir, include_paths, exclude_paths, include_overrides):
                    if include_paths and not any(p in current_pnfs_dir for p in include_paths):
                        skipped_include += 1
                    else:
                        skipped_exclude += 1
                    continue

                # Parse the file entry line
                info = line.split()
                if len(info) < 5:
                    skipped_other += 1
                    continue

                # Check for adler32 checksum (8 hex characters) to avoid duplicates
                if len(info[2]) != 8:
                    skipped_checksum += 1
                    continue

                # Build the full path
                new_line = ";".join(info)
                full_path = os.path.join(current_pnfs_dir, new_line)

                # Remove prefix
                if prefix_to_remove and full_path.startswith(prefix_to_remove):
                    full_path = full_path[len(prefix_to_remove):]
                elif prefix_to_remove:
                    full_path = full_path.replace(prefix_to_remove, "")

                # Extract user
                user = extract_user(full_path, user_regex, default_user)

                # Write output with user column
                output_file.write(f"{full_path};{user}\n")
                lines_written += 1

    if verbose:
        print(f"\nSummary:", file=sys.stderr)
        print(f"  Lines processed: {lines_processed}", file=sys.stderr)
        print(f"  Lines written:   {lines_written}", file=sys.stderr)
        print(f"  Skipped (include filter): {skipped_include}", file=sys.stderr)
        print(f"  Skipped (exclude filter): {skipped_exclude}", file=sys.stderr)
        print(f"  Skipped (checksum):       {skipped_checksum}", file=sys.stderr)
        print(f"  Skipped (other):          {skipped_other} (empty lines, directory markers, malformed)", file=sys.stderr)

    return lines_processed, lines_written


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Translate Chimera dump(s) to CSV with user extraction",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic conversion
  %(prog)s chimera_* /pnfs/gridka.de/cms/disk-only -o output.csv

  # Only dCMS user storage (excluding rucio)
  %(prog)s chimera_* /pnfs/gridka.de/cms/disk-only -o dcms.csv \\
      --include "/pnfs/gridka.de/cms/disk-only/store/user" \\
      --exclude "/pnfs/gridka.de/cms/disk-only/store/user/rucio"

  # Exclude tape storage
  %(prog)s chimera_* /pnfs/gridka.de/cms/disk-only -o notape.csv \\
      --exclude "/pnfs/gridka.de/cms/tape/"

  # Custom user regex
  %(prog)s chimera_* /pnfs/gridka.de/cms/disk-only -o output.csv \\
      --user-regex "/store/(?:user|group)/([^/]+)/"
""",
    )

    parser.add_argument(
        "input_pattern",
        help="Input file(s) or glob pattern for chimera dump files",
    )
    parser.add_argument(
        "prefix_to_remove",
        help="Prefix to remove from paths (e.g., /pnfs/gridka.de/cms/disk-only)",
    )
    parser.add_argument(
        "-o", "--output",
        default="-",
        help="Output CSV file (default: stdout, use '-' for stdout)",
    )
    parser.add_argument(
        "--include",
        action="append",
        default=[],
        dest="include_paths",
        metavar="PATH",
        help="Include paths containing this path prefix. When used with --exclude, acts as an exception "
             "(more specific include takes priority). When used alone, only matching paths are processed.",
    )
    parser.add_argument(
        "--exclude",
        action="append",
        default=[],
        dest="exclude_paths",
        metavar="PATH",
        help="Skip paths containing this path prefix (can be specified multiple times), "
             "unless path also matches a more specific --include path",
    )
    parser.add_argument(
        "--user-regex",
        default=None,
        help="Custom regex to extract username (must have one capture group). "
             "Default: /store/user/([^/]+)/",
    )
    parser.add_argument(
        "--default-user",
        default=DEFAULT_USER,
        help=f"Default user when no match found or for rucio paths (default: {DEFAULT_USER})",
    )
    parser.add_argument(
        "--no-header",
        action="store_true",
        help="Don't write CSV header row",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Print progress and summary to stderr",
    )

    # Convenience presets
    preset_group = parser.add_argument_group("Convenience presets")
    preset_group.add_argument(
        "--dcms-only",
        action="store_true",
        help="Shortcut: include only dCMS user storage, exclude rucio "
             "(equivalent to --include /pnfs/gridka.de/cms/disk-only/store/user "
             "--exclude /pnfs/gridka.de/cms/disk-only/store/user/rucio)",
    )
    preset_group.add_argument(
        "--cms-disk",
        action="store_true",
        help="Shortcut: include CMS disk storage, exclude dCMS user storage but keep rucio "
             "(equivalent to --include /pnfs/gridka.de/cms/disk-only "
             "--exclude /pnfs/gridka.de/cms/disk-only/store/user "
             "--include /pnfs/gridka.de/cms/disk-only/store/user/rucio)",
    )
    preset_group.add_argument(
        "--cms-tape",
        action="store_true",
        help="Shortcut: include only CMS tape storage "
             "(equivalent to --include /pnfs/gridka.de/cms/tape)",
    )

    args = parser.parse_args(argv)

    # Expand glob pattern
    input_files = glob.glob(args.input_pattern)
    if not input_files:
        print(f"ERROR: No files match pattern: {args.input_pattern}", file=sys.stderr)
        return 2

    input_files = sorted(input_files)

    # Apply presets
    include_paths = list(args.include_paths)
    exclude_paths = list(args.exclude_paths)

    if args.dcms_only:
        include_paths.append("/pnfs/gridka.de/cms/disk-only/store/user")
        exclude_paths.append("/pnfs/gridka.de/cms/disk-only/store/user/rucio")

    if args.cms_disk:
        # Include CMS disk storage, exclude dCMS user storage but keep rucio
        include_paths.append("/pnfs/gridka.de/cms/disk-only")
        exclude_paths.append("/pnfs/gridka.de/cms/disk-only/store/user")
        include_paths.append("/pnfs/gridka.de/cms/disk-only/store/user/rucio")

    if args.cms_tape:
        # Include only CMS tape storage
        include_paths.append("/pnfs/gridka.de/cms/tape")

    # Compile user regex if provided
    user_regex = None
    if args.user_regex:
        try:
            user_regex = re.compile(args.user_regex)
        except re.error as e:
            print(f"ERROR: Invalid regex '{args.user_regex}': {e}", file=sys.stderr)
            return 2

    # Open output file
    if args.output == "-":
        output_file = sys.stdout
    else:
        output_file = open(args.output, "w", encoding="utf-8")

    try:
        if args.verbose:
            print(f"Input files: {len(input_files)}", file=sys.stderr)
            print(f"Prefix to remove: {args.prefix_to_remove}", file=sys.stderr)
            print(f"Include paths: {include_paths or '(none - include all)'}", file=sys.stderr)
            print(f"Exclude paths: {exclude_paths or '(none)'}", file=sys.stderr)
            print(f"Default user: {args.default_user}", file=sys.stderr)
            print("", file=sys.stderr)

        lines_processed, lines_written = process_chimera_dump(
            input_files=input_files,
            output_file=output_file,
            prefix_to_remove=args.prefix_to_remove,
            include_paths=include_paths,
            exclude_paths=exclude_paths,
            user_regex=user_regex,
            default_user=args.default_user,
            add_header=not args.no_header,
            verbose=args.verbose,
        )

        if args.verbose:
            print(f"\nOutput: {args.output if args.output != '-' else 'stdout'}", file=sys.stderr)

    finally:
        if output_file is not sys.stdout:
            output_file.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
