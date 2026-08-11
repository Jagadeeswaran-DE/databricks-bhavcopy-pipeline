#!/usr/bin/env python3
"""
Samco BhavCopy File Organizer
=============================

Sorts extracted BhavCopy CSV files into exchange folders based on filename
suffix (_BSE, _MCX, _NSE, _NSEFO). Matching is case-insensitive.

Example output layout::

    organized/
        bse/
            20260121_BSE.csv
        mcx/
            20260121_MCX.csv
        nse/
            20260121_NSE.csv
        nsefo/
            20260121_NSEFO.csv

Run
---
    python bhavcopy_organizer.py
"""

from __future__ import annotations

import csv
import logging
import re
import shutil
import sys
from dataclasses import dataclass, field
from pathlib import Path

from pipeline_logging import configure_logging

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SOURCE_DIR: str = "extracted"  # folder with ZIP subfolders or flat CSV files
OUTPUT_DIR: str = "organized"  # exchange folders created here
SUMMARY_CSV: str = "organized_file_counts.csv"
LOGS_DIR: str = "logs"
MOVE_FILES: bool = False  # False = copy, True = move files into organized/
SKIP_IF_EXISTS: bool = True  # skip when destination file already exists

# Exchange suffixes — longest match first (NSEFO before NSE)
EXCHANGE_PATTERNS: tuple[tuple[str, str], ...] = (
    (r"_NSEFO(?=\.|$)", "nsefo"),
    (r"_BSE(?=\.|$)", "bse"),
    (r"_MCX(?=\.|$)", "mcx"),
    (r"_NSE(?=\.|$)", "nse"),
)


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class OrganizeSummary:
    bse: int = 0
    mcx: int = 0
    nse: int = 0
    nsefo: int = 0
    unknown: int = 0
    skipped: int = 0
    copied: int = 0
    moved: int = 0
    failed: int = 0
    unknown_files: list[str] = field(default_factory=list)

    @property
    def total_processed(self) -> int:
        return self.bse + self.mcx + self.nse + self.nsefo + self.unknown


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------


def setup_logging(logs_dir: Path) -> logging.Logger:
    return configure_logging(
        "bhavcopy_organizer", logs_dir / "bhavcopy_organizer.log"
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def detect_exchange(filename: str) -> str | None:
    """
    Return exchange folder name from filename (case-insensitive).

    Checks NSEFO before NSE so `_NSEFO` is not misclassified as NSE.
    """
    upper_name = filename.upper()
    for pattern, exchange in EXCHANGE_PATTERNS:
        if re.search(pattern, upper_name, flags=re.IGNORECASE):
            return exchange
    return None


def iter_source_files(source_dir: Path) -> list[Path]:
    """Collect all files under source_dir recursively."""
    return sorted(p for p in source_dir.rglob("*") if p.is_file())


def transfer_file(source: Path, destination: Path, move: bool) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if move:
        shutil.move(str(source), str(destination))
    else:
        shutil.copy2(source, destination)


def write_summary_csv(summary: OrganizeSummary, csv_path: Path) -> None:
    rows = [
        {"exchange": "bse", "file_count": summary.bse},
        {"exchange": "mcx", "file_count": summary.mcx},
        {"exchange": "nse", "file_count": summary.nse},
        {"exchange": "nsefo", "file_count": summary.nsefo},
        {"exchange": "unknown", "file_count": summary.unknown},
        {"exchange": "TOTAL", "file_count": summary.total_processed},
    ]

    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["exchange", "file_count"])
        writer.writeheader()
        writer.writerows(rows)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def run() -> int:
    source_dir = Path(SOURCE_DIR)
    output_dir = Path(OUTPUT_DIR)
    summary_path = Path(SUMMARY_CSV)
    log = setup_logging(Path(LOGS_DIR))
    summary = OrganizeSummary()

    log.info("=" * 64)
    log.info("Samco BhavCopy Organizer — starting")
    log.info("=" * 64)

    if not source_dir.is_dir():
        log.error("Source folder not found: %s", source_dir.resolve())
        return 1

    output_dir.mkdir(parents=True, exist_ok=True)
    files = iter_source_files(source_dir)

    if not files:
        log.warning("No files found under %s", source_dir.resolve())
        return 0

    log.info("Source      : %s", source_dir.resolve())
    log.info("Output      : %s", output_dir.resolve())
    log.info("Mode        : %s", "move" if MOVE_FILES else "copy")
    log.info("File count  : %d", len(files))

    for index, source_file in enumerate(files, start=1):
        exchange = detect_exchange(source_file.name)

        if exchange is None:
            summary.unknown += 1
            summary.unknown_files.append(str(source_file))
            log.warning(
                "[%d/%d] UNKNOWN exchange | %s",
                index,
                len(files),
                source_file,
            )
            continue

        destination = output_dir / exchange / source_file.name

        if SKIP_IF_EXISTS and destination.exists():
            summary.skipped += 1
            setattr(summary, exchange, getattr(summary, exchange) + 1)
            log.info("[%d/%d] SKIP | %s", index, len(files), destination.name)
            continue

        try:
            transfer_file(source_file, destination, MOVE_FILES)
            current = getattr(summary, exchange)
            setattr(summary, exchange, current + 1)
            if MOVE_FILES:
                summary.moved += 1
            else:
                summary.copied += 1
            log.info(
                "[%d/%d] %s | %s -> %s",
                index,
                len(files),
                exchange.upper(),
                source_file.name,
                destination.parent.name,
            )
        except OSError as exc:
            summary.failed += 1
            log.error("[%d/%d] FAILED | %s | %s", index, len(files), source_file, exc)

    write_summary_csv(summary, summary_path)

    log.info("=" * 64)
    log.info(
        "Counts | BSE=%d MCX=%d NSE=%d NSEFO=%d UNKNOWN=%d",
        summary.bse,
        summary.mcx,
        summary.nse,
        summary.nsefo,
        summary.unknown,
    )
    log.info(
        "Actions | copied=%d moved=%d skipped=%d failed=%d",
        summary.copied,
        summary.moved,
        summary.skipped,
        summary.failed,
    )
    log.info("Summary CSV : %s", summary_path.resolve())
    log.info("=" * 64)
    return 1 if summary.failed else 0


if __name__ == "__main__":
    try:
        sys.exit(run())
    except KeyboardInterrupt:
        logging.getLogger("bhavcopy_organizer").warning("Interrupted by user")
        sys.exit(130)
    except Exception as exc:
        logging.getLogger("bhavcopy_organizer").exception("Fatal error: %s", exc)
        sys.exit(1)
