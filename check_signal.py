#!/usr/bin/env python3
"""Check signal folder: count signals in signal.csv, snapshots, and validate fields."""

import csv
from collections import defaultdict
from pathlib import Path


SIGNAL_DIR = Path(__file__).parent / "signal"
SIGNAL_CSV = SIGNAL_DIR / "signal.csv"
SNAPSHOT_DIR = SIGNAL_DIR / "snapshot"


def count_signals(csv_path: Path) -> int:
    """Return number of data rows in signal.csv (excluding header)."""
    with open(csv_path, newline="") as f:
        return max(0, sum(1 for _ in f) - 1)


def count_snapshots(snapshot_dir: Path) -> int:
    """Return number of CSV files in snapshot directory."""
    if not snapshot_dir.is_dir():
        return 0
    return sum(1 for f in snapshot_dir.iterdir() if f.suffix == ".csv")


def validate_snapshots(snapshot_dir: Path) -> dict:
    """
    Check every snapshot CSV for empty/missing values.

    Returns a dict:
        {
          "files_ok": int,
          "files_with_issues": int,
          "issues": {
              filename: {column: [row_numbers]}
          }
        }
    """
    issues: dict[str, dict[str, list[int]]] = {}

    snapshot_files = sorted(f for f in snapshot_dir.iterdir() if f.suffix == ".csv")

    for path in snapshot_files:
        file_issues: dict[str, list[int]] = defaultdict(list)

        with open(path, newline="") as f:
            reader = csv.DictReader(f)
            for row_num, row in enumerate(reader, start=2):  # row 1 is header
                for col, value in row.items():
                    if value is None or value.strip() == "":
                        file_issues[col].append(row_num)

        if file_issues:
            issues[path.name] = dict(file_issues)

    files_with_issues = len(issues)
    files_ok = len(snapshot_files) - files_with_issues

    return {
        "files_ok": files_ok,
        "files_with_issues": files_with_issues,
        "issues": issues,
    }


def print_validation_report(result: dict) -> None:
    total = result["files_ok"] + result["files_with_issues"]
    print(f"\n  Snapshot validation ({total} files):")
    print(f"    OK            : {result['files_ok']}")
    print(f"    With issues   : {result['files_with_issues']}")

    if not result["issues"]:
        print("    All fields populated — no missing values found.")
        return

    # --- Column-level summary ---
    col_file_count: dict[str, int] = defaultdict(int)
    col_total_empty: dict[str, int] = defaultdict(int)
    for cols in result["issues"].values():
        for col, rows in cols.items():
            col_file_count[col] += 1
            col_total_empty[col] += len(rows)

    print(f"\n  Column summary (affected files / total empty cells):")
    for col in sorted(col_file_count, key=lambda c: -col_file_count[c]):
        print(f"    {col:<12}: {col_file_count[col]:>4} files, {col_total_empty[col]:>8} empty cells")

    # --- Per-file detail ---
    print(f"\n  Per-file detail:")
    for filename, cols in sorted(result["issues"].items()):
        print(f"    FILE: {filename}")
        for col, rows in sorted(cols.items()):
            if len(rows) <= 5:
                row_str = ", ".join(str(r) for r in rows)
            else:
                row_str = f"{', '.join(str(r) for r in rows[:5])}, ... ({len(rows)} total)"
            print(f"      column '{col}': empty in rows {row_str}")


def main() -> None:
    if not SIGNAL_CSV.exists():
        print(f"ERROR: {SIGNAL_CSV} not found")
        return

    signals = count_signals(SIGNAL_CSV)
    snapshots = count_snapshots(SNAPSHOT_DIR)

    print(f"Signal folder: {SIGNAL_DIR}")
    print(f"  Signals in signal.csv : {signals}")
    print(f"  Snapshots in snapshot/ : {snapshots}")

    if signals != snapshots:
        print(f"  WARNING: count mismatch — {abs(signals - snapshots)} difference")
    else:
        print("  OK: counts match")

    result = validate_snapshots(SNAPSHOT_DIR)
    print_validation_report(result)


if __name__ == "__main__":
    main()
