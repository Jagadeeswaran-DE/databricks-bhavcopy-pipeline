#!/usr/bin/env python3
"""
Samco BhavCopy Schema Extractor
===============================

Reads CSV files in `organized/` exchange folders (bse, mcx, nse, nsefo) and
extracts column schemas with inferred data types.

Outputs
-------
- schemas/<exchange>_schema.json   — per-exchange schema
- schemas/all_schemas.json         — combined schema document
- schema_summary.csv               — flat column listing for all exchanges

Run
---
    python bhavcopy_schema_extractor.py
"""

from __future__ import annotations

import csv
import json
import logging
import re
import sys
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from pipeline_logging import configure_logging

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

ORGANIZED_DIR: str = "organized"
SCHEMA_DIR: str = "schemas"
SUMMARY_CSV: str = "schema_summary.csv"
LOGS_DIR: str = "logs"

# How many files / rows to sample per exchange for type inference
SAMPLE_FILES: int = 5
SAMPLE_ROWS: int = 200

# Folders to scan (empty = auto-detect subfolders with CSV files)
EXCHANGE_FOLDERS: list[str] = []  # e.g. ["bse", "mcx", "nse", "nsefo"]

DATE_PATTERNS: tuple[str, ...] = (
    r"^\d{2}-\w{3}-\d{4}$",       # 01-Jan-2026
    r"^\d{2} \w{3} \d{4}$",       # 01 Jan 2026
    r"^\d{4}-\d{2}-\d{2}$",       # 2026-01-01
    r"^\d{2}/\d{2}/\d{4}$",
)


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class ColumnSchema:
    name: str
    position: int
    inferred_type: str = "string"
    nullable: bool = False
    sample_values: list[str] = field(default_factory=list)
    type_counts: dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "position": self.position,
            "inferred_type": self.inferred_type,
            "nullable": self.nullable,
            "sample_values": self.sample_values,
            "type_counts": self.type_counts,
        }


@dataclass
class ExchangeSchema:
    exchange: str
    folder: str
    files_total: int = 0
    files_checked: int = 0
    row_count_sampled: int = 0
    schema_consistent: bool = True
    header: list[str] = field(default_factory=list)
    columns: list[ColumnSchema] = field(default_factory=list)
    mismatched_files: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "exchange": self.exchange,
            "folder": self.folder,
            "files_total": self.files_total,
            "files_checked": self.files_checked,
            "row_count_sampled": self.row_count_sampled,
            "schema_consistent": self.schema_consistent,
            "column_count": len(self.columns),
            "header": self.header,
            "columns": [col.to_dict() for col in self.columns],
            "mismatched_files": self.mismatched_files,
        }


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------


def setup_logging(logs_dir: Path) -> logging.Logger:
    return configure_logging(
        "bhavcopy_schema_extractor",
        logs_dir / "bhavcopy_schema_extractor.log",
    )


# ---------------------------------------------------------------------------
# Type inference
# ---------------------------------------------------------------------------


def is_empty(value: str) -> bool:
    return value is None or str(value).strip() == ""


def looks_like_date(value: str) -> bool:
    text = value.strip()
    for pattern in DATE_PATTERNS:
        if re.match(pattern, text, flags=re.IGNORECASE):
            return True
    return False


def infer_value_type(value: str) -> str:
    text = value.strip()
    if is_empty(text):
        return "null"

    if looks_like_date(text):
        return "date"

    try:
        if re.fullmatch(r"-?\d+", text):
            return "integer"
    except re.error:
        pass

    try:
        float(text.replace(",", ""))
        return "float"
    except ValueError:
        pass

    return "string"


def merge_type_counts(counter: Counter[str]) -> str:
    """Pick the best representative type from observed value types."""
    if not counter:
        return "string"

    non_null = Counter({k: v for k, v in counter.items() if k != "null"})
    if not non_null:
        return "string"

    if "date" in non_null:
        return "date"
    if "float" in non_null:
        return "float"
    if "integer" in non_null:
        return "integer"
    return non_null.most_common(1)[0][0]


def read_csv_header(path: Path) -> list[str]:
    with path.open(newline="", encoding="utf-8", errors="replace") as handle:
        reader = csv.reader(handle)
        return next(reader)


def sample_csv_rows(path: Path, max_rows: int) -> list[list[str]]:
    rows: list[list[str]] = []
    with path.open(newline="", encoding="utf-8", errors="replace") as handle:
        reader = csv.reader(handle)
        next(reader)  # skip header
        for row in reader:
            rows.append(row)
            if len(rows) >= max_rows:
                break
    return rows


# ---------------------------------------------------------------------------
# Schema extraction
# ---------------------------------------------------------------------------


def discover_exchange_folders(root: Path) -> list[Path]:
    if EXCHANGE_FOLDERS:
        return [root / name for name in EXCHANGE_FOLDERS if (root / name).is_dir()]

    return sorted(
        folder
        for folder in root.iterdir()
        if folder.is_dir() and any(folder.glob("*.csv"))
    )


def extract_exchange_schema(folder: Path, log: logging.Logger) -> ExchangeSchema:
    exchange = folder.name.lower()
    schema = ExchangeSchema(exchange=exchange, folder=str(folder.resolve()))
    files = sorted(folder.glob("*.csv"))
    schema.files_total = len(files)

    if not files:
        log.warning("No CSV files in %s", folder)
        return schema

    reference_header = read_csv_header(files[0])
    schema.header = reference_header
    schema.columns = [
        ColumnSchema(name=name, position=index)
        for index, name in enumerate(reference_header, start=1)
    ]

    # Validate headers across all files
    for file_index, csv_file in enumerate(files, start=1):
        log.info(
            "HEADER CHECK | %s | [%d/%d] %s",
            exchange.upper(),
            file_index,
            len(files),
            csv_file.name,
        )
        header = read_csv_header(csv_file)
        schema.files_checked += 1
        if header != reference_header:
            schema.schema_consistent = False
            schema.mismatched_files.append(csv_file.name)
            log.warning(
                "Header mismatch in %s/%s",
                exchange,
                csv_file.name,
            )

    # Sample rows from a subset of files for type inference
    sample_files = files[:SAMPLE_FILES]
    column_counters = [Counter() for _ in schema.columns]
    sample_values: list[list[str]] = [[] for _ in schema.columns]
    null_counts = [0 for _ in schema.columns]

    for file_index, csv_file in enumerate(sample_files, start=1):
        log.info(
            "TYPE SAMPLE | %s | [%d/%d] %s",
            exchange.upper(),
            file_index,
            len(sample_files),
            csv_file.name,
        )
        rows = sample_csv_rows(csv_file, SAMPLE_ROWS)
        schema.row_count_sampled += len(rows)
        for row in rows:
            for index, column in enumerate(schema.columns):
                value = row[index] if index < len(row) else ""
                value_type = infer_value_type(value)
                column_counters[index][value_type] += 1
                if value_type == "null":
                    null_counts[index] += 1
                elif len(sample_values[index]) < 3 and not is_empty(value):
                    sample_values[index].append(value.strip())

    for index, column in enumerate(schema.columns):
        column.type_counts = dict(column_counters[index])
        column.inferred_type = merge_type_counts(column_counters[index])
        column.nullable = null_counts[index] > 0
        column.sample_values = sample_values[index]

    log.info(
        "SCHEMA | %s | columns=%d files=%d consistent=%s",
        exchange.upper(),
        len(schema.columns),
        schema.files_total,
        schema.schema_consistent,
    )
    return schema


def write_summary_csv(schemas: list[ExchangeSchema], csv_path: Path) -> None:
    fieldnames = [
        "exchange",
        "column_position",
        "column_name",
        "inferred_type",
        "nullable",
        "sample_values",
        "files_total",
        "schema_consistent",
    ]

    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for schema in schemas:
            for column in schema.columns:
                writer.writerow(
                    {
                        "exchange": schema.exchange,
                        "column_position": column.position,
                        "column_name": column.name,
                        "inferred_type": column.inferred_type,
                        "nullable": column.nullable,
                        "sample_values": " | ".join(column.sample_values),
                        "files_total": schema.files_total,
                        "schema_consistent": schema.schema_consistent,
                    }
                )


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def run() -> int:
    organized_dir = Path(ORGANIZED_DIR)
    schema_dir = Path(SCHEMA_DIR)
    summary_path = Path(SUMMARY_CSV)
    log = setup_logging(Path(LOGS_DIR))

    log.info("=" * 64)
    log.info("Samco BhavCopy Schema Extractor — starting")
    log.info("=" * 64)

    if not organized_dir.is_dir():
        log.error("Organized folder not found: %s", organized_dir.resolve())
        return 1

    schema_dir.mkdir(parents=True, exist_ok=True)
    exchange_folders = discover_exchange_folders(organized_dir)

    if not exchange_folders:
        log.warning("No exchange folders with CSV files found in %s", organized_dir)
        return 0

    log.info("Source      : %s", organized_dir.resolve())
    log.info("Schema dir  : %s", schema_dir.resolve())
    log.info("Exchanges   : %s", ", ".join(f.name for f in exchange_folders))

    schemas: list[ExchangeSchema] = []
    for exchange_index, folder in enumerate(exchange_folders, start=1):
        log.info(
            "EXCHANGE [%d/%d] | %s",
            exchange_index,
            len(exchange_folders),
            folder.name.upper(),
        )
        schema = extract_exchange_schema(folder, log)
        schemas.append(schema)
        write_json(schema_dir / f"{schema.exchange}_schema.json", schema.to_dict())

    all_payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "source_dir": str(organized_dir.resolve()),
        "exchanges": [schema.to_dict() for schema in schemas],
    }
    write_json(schema_dir / "all_schemas.json", all_payload)
    write_summary_csv(schemas, summary_path)

    log.info("=" * 64)
    for schema in schemas:
        log.info(
            "%-6s | %2d columns | %3d files | consistent=%s",
            schema.exchange.upper(),
            len(schema.columns),
            schema.files_total,
            schema.schema_consistent,
        )
    log.info("Summary CSV : %s", summary_path.resolve())
    log.info("JSON schemas: %s", schema_dir.resolve())
    log.info("=" * 64)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(run())
    except KeyboardInterrupt:
        logging.getLogger("bhavcopy_schema_extractor").warning("Interrupted by user")
        sys.exit(130)
    except Exception as exc:
        logging.getLogger("bhavcopy_schema_extractor").exception("Fatal error: %s", exc)
        sys.exit(1)
