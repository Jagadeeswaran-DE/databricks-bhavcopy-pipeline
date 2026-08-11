#!/usr/bin/env python3
"""
Samco BhavCopy ZIP Extractor
============================

Extracts CSV files from BhavCopy ZIP archives in `downloads/` and writes a
per-ZIP summary CSV with exchange file counts (NSE, NSEFO, BSE, MCX).

Installation
------------
Uses only the Python standard library (no extra packages).

Run
---
    python bhavcopy_extractor.py
"""

from __future__ import annotations

import csv
import logging
import sys
import zipfile
from dataclasses import dataclass
from pathlib import Path

from pipeline_logging import configure_logging

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DOWNLOAD_DIR: str = "downloads"
EXTRACT_DIR: str = "extracted"
SUMMARY_CSV: str = "zip_file_counts.csv"
LOGS_DIR: str = "logs"
SKIP_ALREADY_EXTRACTED: bool = True

# Exchange suffixes found inside Samco BhavCopy ZIP files
EXCHANGE_SUFFIXES: dict[str, str] = {
    "NSE.csv": "nse",
    "NSEFO.csv": "nsefo",
    "BSE.csv": "bse",
    "MCX.csv": "mcx",
}


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class ZipSummary:
    zip_file: str
    nse: int = 0
    nsefo: int = 0
    bse: int = 0
    mcx: int = 0
    other: int = 0
    total_files: int = 0
    extract_folder: str = ""
    status: str = "ok"
    error: str = ""

    @property
    def as_row(self) -> dict[str, str | int]:
        return {
            "zip_file": self.zip_file,
            "nse_files": self.nse,
            "nsefo_files": self.nsefo,
            "bse_files": self.bse,
            "mcx_files": self.mcx,
            "other_files": self.other,
            "total_files": self.total_files,
            "extract_folder": self.extract_folder,
            "status": self.status,
            "error": self.error,
        }


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------


def setup_logging(logs_dir: Path) -> logging.Logger:
    return configure_logging(
        "bhavcopy_extractor", logs_dir / "bhavcopy_extractor.log"
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def classify_member(filename: str) -> str:
    """Map a ZIP member name to an exchange category."""
    name = Path(filename).name
    for suffix, category in EXCHANGE_SUFFIXES.items():
        if name.endswith(suffix):
            return category
    return "other"


def count_zip_members(zip_path: Path) -> ZipSummary:
    """Count exchange files inside a ZIP without extracting."""
    summary = ZipSummary(zip_file=zip_path.name)

    with zipfile.ZipFile(zip_path, "r") as archive:
        for member in archive.namelist():
            if member.endswith("/"):
                continue

            summary.total_files += 1
            category = classify_member(member)
            if category == "nse":
                summary.nse += 1
            elif category == "nsefo":
                summary.nsefo += 1
            elif category == "bse":
                summary.bse += 1
            elif category == "mcx":
                summary.mcx += 1
            else:
                summary.other += 1

    return summary


def extract_zip(zip_path: Path, extract_root: Path, log: logging.Logger) -> Path:
    """
    Extract one ZIP into `extract_root/<zip_stem>/`.

    Returns the folder where files were written.
    """
    target_dir = extract_root / zip_path.stem
    target_dir.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(zip_path, "r") as archive:
        archive.extractall(target_dir)
        log.info(
            "Extracted %s -> %s (%d files)",
            zip_path.name,
            target_dir,
            len([m for m in archive.namelist() if not m.endswith("/")]),
        )

    return target_dir


def already_extracted(zip_path: Path, extract_root: Path) -> bool:
    """True when the target extract folder exists and is non-empty."""
    target_dir = extract_root / zip_path.stem
    return target_dir.is_dir() and any(target_dir.iterdir())


def write_summary_csv(rows: list[dict[str, str | int]], csv_path: Path) -> None:
    """Write per-ZIP counts and a totals row to CSV."""
    fieldnames = [
        "zip_file",
        "nse_files",
        "nsefo_files",
        "bse_files",
        "mcx_files",
        "other_files",
        "total_files",
        "extract_folder",
        "status",
        "error",
    ]

    totals = {key: 0 for key in fieldnames if key.endswith("_files")}
    for row in rows:
        for key in totals:
            totals[key] += int(row[key])

    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
        writer.writerow(
            {
                "zip_file": "TOTAL",
                "nse_files": totals["nse_files"],
                "nsefo_files": totals["nsefo_files"],
                "bse_files": totals["bse_files"],
                "mcx_files": totals["mcx_files"],
                "other_files": totals["other_files"],
                "total_files": totals["total_files"],
                "extract_folder": "",
                "status": "",
                "error": "",
            }
        )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def run() -> int:
    download_dir = Path(DOWNLOAD_DIR)
    extract_root = Path(EXTRACT_DIR)
    summary_path = Path(SUMMARY_CSV)
    log = setup_logging(Path(LOGS_DIR))

    log.info("=" * 64)
    log.info("Samco BhavCopy Extractor — starting")
    log.info("=" * 64)

    if not download_dir.is_dir():
        log.error("Download folder not found: %s", download_dir.resolve())
        return 1

    extract_root.mkdir(parents=True, exist_ok=True)
    zip_files = sorted(download_dir.glob("*.zip"))

    if not zip_files:
        log.warning("No ZIP files found in %s", download_dir.resolve())
        return 0

    log.info("ZIP source  : %s", download_dir.resolve())
    log.info("Extract to  : %s", extract_root.resolve())
    log.info("Summary CSV : %s", summary_path.resolve())
    log.info("ZIP count   : %d", len(zip_files))

    rows: list[dict[str, str | int]] = []
    extracted = 0
    skipped = 0
    failed = 0

    for index, zip_path in enumerate(zip_files, start=1):
        log.info("Processing [%d/%d] %s", index, len(zip_files), zip_path.name)
        summary = ZipSummary(zip_file=zip_path.name)

        try:
            if SKIP_ALREADY_EXTRACTED and already_extracted(zip_path, extract_root):
                summary.extract_folder = str((extract_root / zip_path.stem).resolve())
                summary.status = "skipped"
                skipped += 1
                log.info("SKIP | already extracted: %s", summary.extract_folder)
            else:
                target_dir = extract_zip(zip_path, extract_root, log)
                summary.extract_folder = str(target_dir.resolve())
                summary.status = "extracted"
                extracted += 1

            counts = count_zip_members(zip_path)
            summary.nse = counts.nse
            summary.nsefo = counts.nsefo
            summary.bse = counts.bse
            summary.mcx = counts.mcx
            summary.other = counts.other
            summary.total_files = counts.total_files

            log.info(
                "COUNTS | %s | NSE=%d NSEFO=%d BSE=%d MCX=%d TOTAL=%d",
                zip_path.name,
                summary.nse,
                summary.nsefo,
                summary.bse,
                summary.mcx,
                summary.total_files,
            )

        except (zipfile.BadZipFile, OSError, ValueError) as exc:
            summary.status = "failed"
            summary.error = str(exc)
            failed += 1
            log.error("FAILED | %s | %s", zip_path.name, exc)

        rows.append(summary.as_row)

    write_summary_csv(rows, summary_path)

    log.info("=" * 64)
    log.info(
        "Finished | extracted=%d | skipped=%d | failed=%d | summary=%s",
        extracted,
        skipped,
        failed,
        summary_path.resolve(),
    )
    log.info("=" * 64)
    return 1 if failed else 0


if __name__ == "__main__":
    try:
        sys.exit(run())
    except KeyboardInterrupt:
        logging.getLogger("bhavcopy_extractor").warning("Interrupted by user")
        sys.exit(130)
    except Exception as exc:
        logging.getLogger("bhavcopy_extractor").exception("Fatal error: %s", exc)
        sys.exit(1)
