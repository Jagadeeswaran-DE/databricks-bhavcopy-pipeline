#!/usr/bin/env python3
"""
Samco BhavCopy Schema Validator
===============================

Checks whether all CSV files in each exchange folder (bse, mcx, nse, nsefo)
share the exact same column header schema.

Outputs
-------
- schema_validation_report.csv  — one row per file with match status
- schema_validation_summary.csv — one row per exchange group
- schema_validation_report.json — full validation details

Run
---
    python bhavcopy_schema_validator.py
"""

from __future__ import annotations

import csv
import json
import logging
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from pipeline_logging import configure_logging

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

ORGANIZED_DIR: str = "organized"
REPORT_CSV: str = "schema_validation_report.csv"
SUMMARY_CSV: str = "schema_validation_summary.csv"
REPORT_JSON: str = "schema_validation_report.json"
LOGS_DIR: str = "logs"

EXCHANGE_FOLDERS: list[str] = []  # empty = auto-detect subfolders with CSVs


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class FileValidation:
    exchange: str
    file_name: str
    column_count: int
    schema_match: bool
    reference_file: str
    missing_columns: list[str] = field(default_factory=list)
    extra_columns: list[str] = field(default_factory=list)
    actual_header: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "exchange": self.exchange,
            "file_name": self.file_name,
            "column_count": self.column_count,
            "schema_match": self.schema_match,
            "reference_file": self.reference_file,
            "missing_columns": self.missing_columns,
            "extra_columns": self.extra_columns,
            "actual_header": self.actual_header,
        }


@dataclass
class GroupValidation:
    exchange: str
    folder: str
    reference_file: str
    reference_header: list[str]
    files_total: int = 0
    files_matching: int = 0
    files_mismatching: int = 0
    schema_consistent: bool = True
    mismatched_files: list[str] = field(default_factory=list)
    file_results: list[FileValidation] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "exchange": self.exchange,
            "folder": self.folder,
            "reference_file": self.reference_file,
            "reference_header": self.reference_header,
            "column_count": len(self.reference_header),
            "files_total": self.files_total,
            "files_matching": self.files_matching,
            "files_mismatching": self.files_mismatching,
            "schema_consistent": self.schema_consistent,
            "mismatched_files": self.mismatched_files,
            "file_results": [result.to_dict() for result in self.file_results],
        }


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------


def setup_logging(logs_dir: Path) -> logging.Logger:
    return configure_logging(
        "bhavcopy_schema_validator",
        logs_dir / "bhavcopy_schema_validator.log",
    )


# ---------------------------------------------------------------------------
# Validation logic
# ---------------------------------------------------------------------------


def read_csv_header(path: Path) -> list[str]:
    with path.open(newline="", encoding="utf-8", errors="replace") as handle:
        return next(csv.reader(handle))


def compare_headers(reference: list[str], actual: list[str]) -> tuple[list[str], list[str]]:
    """Return (missing_columns, extra_columns) relative to reference."""
    ref_set = set(reference)
    act_set = set(actual)
    missing = [col for col in reference if col not in act_set]
    extra = [col for col in actual if col not in ref_set]
    return missing, extra


def discover_exchange_folders(root: Path) -> list[Path]:
    if EXCHANGE_FOLDERS:
        return [root / name for name in EXCHANGE_FOLDERS if (root / name).is_dir()]

    return sorted(
        folder
        for folder in root.iterdir()
        if folder.is_dir() and any(folder.glob("*.csv"))
    )


def validate_exchange_folder(folder: Path, log: logging.Logger) -> GroupValidation:
    exchange = folder.name.lower()
    files = sorted(folder.glob("*.csv"))

    if not files:
        log.warning("No CSV files found in %s", folder)
        return GroupValidation(
            exchange=exchange,
            folder=str(folder.resolve()),
            reference_file="",
            reference_header=[],
        )

    reference_file = files[0]
    reference_header = read_csv_header(reference_file)
    group = GroupValidation(
        exchange=exchange,
        folder=str(folder.resolve()),
        reference_file=reference_file.name,
        reference_header=reference_header,
        files_total=len(files),
    )

    log.info(
        "GROUP %s | reference=%s | columns=%d | files=%d",
        exchange.upper(),
        reference_file.name,
        len(reference_header),
        len(files),
    )

    for file_index, csv_file in enumerate(files, start=1):
        header = read_csv_header(csv_file)
        missing, extra = compare_headers(reference_header, header)
        match = header == reference_header

        result = FileValidation(
            exchange=exchange,
            file_name=csv_file.name,
            column_count=len(header),
            schema_match=match,
            reference_file=reference_file.name,
            missing_columns=missing,
            extra_columns=extra,
            actual_header=header if not match else [],
        )
        group.file_results.append(result)

        if match:
            group.files_matching += 1
            log.info(
                "FILE CHECK | %s | [%d/%d] PASS | %s",
                exchange.upper(),
                file_index,
                len(files),
                csv_file.name,
            )
        else:
            group.files_mismatching += 1
            group.schema_consistent = False
            group.mismatched_files.append(csv_file.name)
            log.error(
                "MISMATCH | %s/%s | columns=%d missing=%s extra=%s",
                exchange,
                csv_file.name,
                len(header),
                missing,
                extra,
            )

    status = "PASS" if group.schema_consistent else "FAIL"
    log.info(
        "%s | %s | matching=%d/%d",
        status,
        exchange.upper(),
        group.files_matching,
        group.files_total,
    )
    return group


def write_file_report_csv(groups: list[GroupValidation], csv_path: Path) -> None:
    fieldnames = [
        "exchange",
        "file_name",
        "schema_match",
        "column_count",
        "expected_column_count",
        "reference_file",
        "missing_columns",
        "extra_columns",
    ]

    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for group in groups:
            expected = len(group.reference_header)
            for result in group.file_results:
                writer.writerow(
                    {
                        "exchange": result.exchange,
                        "file_name": result.file_name,
                        "schema_match": result.schema_match,
                        "column_count": result.column_count,
                        "expected_column_count": expected,
                        "reference_file": result.reference_file,
                        "missing_columns": "; ".join(result.missing_columns),
                        "extra_columns": "; ".join(result.extra_columns),
                    }
                )


def write_summary_csv(groups: list[GroupValidation], csv_path: Path) -> None:
    fieldnames = [
        "exchange",
        "schema_consistent",
        "files_total",
        "files_matching",
        "files_mismatching",
        "column_count",
        "reference_file",
        "reference_columns",
        "mismatched_files",
    ]

    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for group in groups:
            writer.writerow(
                {
                    "exchange": group.exchange,
                    "schema_consistent": group.schema_consistent,
                    "files_total": group.files_total,
                    "files_matching": group.files_matching,
                    "files_mismatching": group.files_mismatching,
                    "column_count": len(group.reference_header),
                    "reference_file": group.reference_file,
                    "reference_columns": " | ".join(group.reference_header),
                    "mismatched_files": "; ".join(group.mismatched_files),
                }
            )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def run() -> int:
    organized_dir = Path(ORGANIZED_DIR)
    log = setup_logging(Path(LOGS_DIR))

    log.info("=" * 64)
    log.info("Samco BhavCopy Schema Validator — starting")
    log.info("=" * 64)

    if not organized_dir.is_dir():
        log.error("Organized folder not found: %s", organized_dir.resolve())
        return 1

    folders = discover_exchange_folders(organized_dir)
    if not folders:
        log.warning("No exchange folders with CSV files found.")
        return 0

    log.info("Source: %s", organized_dir.resolve())

    groups = [validate_exchange_folder(folder, log) for folder in folders]

    write_file_report_csv(groups, Path(REPORT_CSV))
    write_summary_csv(groups, Path(SUMMARY_CSV))

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "source_dir": str(organized_dir.resolve()),
        "all_groups_consistent": all(g.schema_consistent for g in groups),
        "groups": [group.to_dict() for group in groups],
    }
    Path(REPORT_JSON).write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    log.info("=" * 64)
    log.info("VALIDATION SUMMARY")
    for group in groups:
        mark = "OK" if group.schema_consistent else "FAIL"
        log.info(
            "[%s] %-6s | %3d/%3d files match | %2d columns",
            mark,
            group.exchange.upper(),
            group.files_matching,
            group.files_total,
            len(group.reference_header),
        )
    log.info("File report  : %s", Path(REPORT_CSV).resolve())
    log.info("Group summary: %s", Path(SUMMARY_CSV).resolve())
    log.info("JSON report  : %s", Path(REPORT_JSON).resolve())
    log.info("=" * 64)

    all_ok = all(group.schema_consistent for group in groups)
    if all_ok:
        log.info("RESULT: All exchange groups have a consistent schema.")
    else:
        log.error("RESULT: One or more exchange groups have schema mismatches.")

    return 0 if all_ok else 1


if __name__ == "__main__":
    try:
        sys.exit(run())
    except KeyboardInterrupt:
        logging.getLogger("bhavcopy_schema_validator").warning("Interrupted by user")
        sys.exit(130)
    except Exception as exc:
        logging.getLogger("bhavcopy_schema_validator").exception("Fatal error: %s", exc)
        sys.exit(1)
