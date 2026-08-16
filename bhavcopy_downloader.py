#!/usr/bin/env python3
"""
Samco BhavCopy Downloader (chunked)
===================================

Downloads BhavCopy ZIP archives from:
https://www.samco.in/bhavcopy-nse-bse-mcx

The Samco backend struggles with large date ranges (full months often return
HTTP 500 or hang).  This script requests data in small configurable chunks
(default 10 days) for reliable downloads.

Requires Python 3.10+

Installation
------------
1. Create and activate a virtual environment (recommended):

       python -m venv .venv
       .venv\\Scripts\\activate        # Windows
       source .venv/bin/activate       # macOS / Linux

Execution
---------
       python bhavcopy_downloader.py

Edit the configuration constants below before running.
Progress is persisted in state.json — re-running resumes from the last
successful chunk without re-downloading completed files.
"""

from __future__ import annotations

import json
import logging
import os
import random
import shutil
import subprocess
import sys
import tempfile
import time
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from pipeline_logging import configure_logging

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

START_DATE: str = "2019-01-01"  # YYYY-MM-DD — inclusive range start
END_DATE: str = "2020-12-31"  # YYYY-MM-DD — inclusive range end
CHUNK_DAYS: int = 10  # days per download request (7–10 works best on Samco)
MAX_RETRIES: int = 5
MIN_DELAY: int = 3  # seconds — fast tier while downloads succeed
MAX_DELAY: int = 8  # seconds — fast tier upper bound
SLOW_MIN_DELAY: int = 15  # seconds — escalated tier after server errors
SLOW_MAX_DELAY: int = 45  # seconds — escalated tier upper bound
DOWNLOAD_DIR: str = "downloads"

# Derived paths (created automatically at startup)
SCREENSHOTS_DIR: str = "screenshots"
LOGS_DIR: str = "logs"
STATE_FILE: str = "state.json"

# Samco public BhavCopy endpoint. It accepts the same form fields as the page.
TARGET_URL: str = "https://www.samco.in/bhavcopy-nse-bse-mcx"
DOWNLOAD_ENDPOINT: str = "https://www.samco.in/bse_nse_mcx/getBhavcopy"
SEGMENTS: tuple[str, ...] = ("NSE", "NSEFO", "BSE", "MCX")

# HTTP timeout for the direct ZIP download request.
DOWNLOAD_TIMEOUT_SECONDS: int = 90

# HTTP status codes that trigger an automatic retry
RETRYABLE_HTTP_STATUSES: frozenset[int] = frozenset({500, 502, 503, 504})

# Realistic desktop user agents for rotation
USER_AGENTS: list[str] = [
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) "
        "Gecko/20100101 Firefox/125.0"
    ),
    (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:125.0) "
        "Gecko/20100101 Firefox/125.0"
    ),
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.0.0"
    ),
]


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DateChunk:
    """Inclusive date range for a single download request."""

    start: date
    end: date

    @property
    def key(self) -> str:
        """Stable identifier used in state.json."""
        return f"{self.start:%Y%m%d}_{self.end:%Y%m%d}"

    @property
    def label(self) -> str:
        return f"{self.start:%Y-%m-%d} → {self.end:%Y-%m-%d}"

    @property
    def output_filename(self) -> str:
        return f"{self.start:%Y%m%d}_{self.end:%Y%m%d}.zip"


    @property
    def start_iso(self) -> str:
        return self.start.strftime("%Y-%m-%d")

    @property
    def end_iso(self) -> str:
        return self.end.strftime("%Y-%m-%d")


class NoDataAvailable(RuntimeError):
    """Samco has no archive for this requested date range."""


@dataclass
class AdaptiveDelay:
    """
    Dynamic anti-blocking delays: short pauses while the server responds well,
    longer pauses only after negative responses (HTTP 500, timeouts, etc.).
    """

    level: int = 0  # 0=fast, 1=medium, 2=slow

    def _tiers(self) -> list[tuple[int, int]]:
        medium_min = (MIN_DELAY + SLOW_MIN_DELAY) // 2
        medium_max = (MAX_DELAY + SLOW_MAX_DELAY) // 2
        return [
            (MIN_DELAY, MAX_DELAY),
            (medium_min, medium_max),
            (SLOW_MIN_DELAY, SLOW_MAX_DELAY),
        ]

    def on_success(self) -> None:
        """Reset to fast tier after a successful download."""
        self.level = 0

    def on_failure(self) -> None:
        """Escalate one tier after a negative server response."""
        self.level = min(self.level + 1, len(self._tiers()) - 1)

    @property
    def tier_name(self) -> str:
        return ("fast", "medium", "slow")[self.level]

    def sleep(self, log: logging.Logger) -> None:
        """Pause between chunk downloads using the current delay tier."""
        lo, hi = self._tiers()[self.level]
        delay = random.uniform(lo, hi)
        log.info(
            "Anti-blocking delay [%s tier, level %d]: sleeping %.1f seconds",
            self.tier_name,
            self.level,
            delay,
        )
        time.sleep(delay)


@dataclass
class DownloadState:
    """Persisted progress so interrupted runs can resume."""

    completed: list[str] = field(default_factory=list)
    last_updated: str = ""

    def is_done(self, chunk: DateChunk) -> bool:
        return chunk.key in self.completed

    def mark_done(self, chunk: DateChunk) -> None:
        if chunk.key not in self.completed:
            self.completed.append(chunk.key)
        self.last_updated = datetime.now().isoformat(timespec="seconds")

    @classmethod
    def load(cls, path: Path) -> DownloadState:
        if not path.exists():
            return cls()
        try:
            data: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
            return cls(
                completed=list(data.get("completed", [])),
                last_updated=str(data.get("last_updated", "")),
            )
        except (json.JSONDecodeError, OSError) as exc:
            logging.getLogger("bhavcopy_downloader").warning(
                "Could not read state file (%s); starting fresh", exc
            )
            return cls()

    def save(self, path: Path) -> None:
        path.write_text(
            json.dumps(asdict(self), indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------


def setup_logging(logs_dir: Path) -> logging.Logger:
    """Configure INFO-level console output and detailed file logging."""
    return configure_logging(
        "bhavcopy_downloader", logs_dir / "bhavcopy_downloader.log"
    )


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------


def parse_date(value: str, name: str) -> date:
    """Parse a YYYY-MM-DD configuration string."""
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"Invalid {name} '{value}'. Use YYYY-MM-DD.") from exc


def resolve_end_date() -> date:
    """Return END_DATE from config, or today when left empty."""
    configured = os.getenv("BHAVCOPY_END_DATE", END_DATE)
    return parse_date(configured, "END_DATE") if configured.strip() else date.today()


def generate_chunks(start: date, end: date, chunk_days: int) -> list[DateChunk]:
    """
    Split [start, end] into consecutive inclusive chunks of `chunk_days` days.

    Chunks may cross month boundaries (fixed-size windows from the start date).

    Example (chunk_days=10):
        2026-01-01 → 2026-01-10
        2026-01-11 → 2026-01-20
        2026-01-21 → 2026-01-30
        2026-01-31 → 2026-02-09
        ...
    """
    if chunk_days < 1:
        raise ValueError(f"CHUNK_DAYS must be >= 1, got {chunk_days}")
    if start > end:
        raise ValueError(f"START_DATE ({start}) cannot be after END_DATE ({end}).")

    chunks: list[DateChunk] = []
    cursor = start
    step = timedelta(days=chunk_days)

    while cursor <= end:
        chunk_end = min(cursor + step - timedelta(days=1), end)
        chunks.append(DateChunk(start=cursor, end=chunk_end))
        cursor = chunk_end + timedelta(days=1)

    return chunks


# ---------------------------------------------------------------------------
# File / state validation
# ---------------------------------------------------------------------------


def validate_download(path: Path) -> None:
    """Raise when the saved file is missing or empty."""
    if not path.exists():
        raise FileNotFoundError(f"Downloaded file not found: {path}")
    size = path.stat().st_size
    if size <= 0:
        raise ValueError(f"Downloaded file is empty (0 bytes): {path}")


def chunk_already_on_disk(chunk: DateChunk, download_dir: Path) -> bool:
    """True when a valid non-empty ZIP for this chunk already exists."""
    path = download_dir / chunk.output_filename
    return path.exists() and path.stat().st_size > 0


# ---------------------------------------------------------------------------
# HTTP download helpers
# ---------------------------------------------------------------------------


def random_user_agent() -> str:
    return random.choice(USER_AGENTS)


def exponential_backoff(attempt: int, log: logging.Logger, delay_ctrl: AdaptiveDelay) -> None:
    """Wait before retrying a failed chunk; scales with attempt and delay tier."""
    tier_lo, tier_hi = delay_ctrl._tiers()[delay_ctrl.level]
    base = 2 ** (attempt - 1)
    wait = min(base * random.uniform(tier_lo / 3, tier_hi / 3), 120)
    log.info(
        "Exponential backoff before retry [%s tier]: %.1f seconds",
        delay_ctrl.tier_name,
        wait,
    )
    time.sleep(wait)


# ---------------------------------------------------------------------------
# Core download logic
# ---------------------------------------------------------------------------


def download_chunk(
    chunk: DateChunk,
    download_dir: Path,
    attempt: int,
    log: logging.Logger,
) -> Path:
    """Download one date chunk through Samco's public form endpoint."""
    output_path = download_dir / chunk.output_filename
    log.info(
        "Attempt %d/%d | %s | file=%s",
        attempt,
        MAX_RETRIES,
        chunk.label,
        chunk.output_filename,
    )
    user_agent = random_user_agent()
    fields = [("start_date", chunk.start_iso), ("end_date", chunk.end_iso)]
    fields.extend(("bhavcopy_data[]", segment) for segment in SEGMENTS)
    fields.append(("show_or_down", "2"))
    with tempfile.TemporaryDirectory(prefix="samco-bhavcopy-") as temporary_dir:
        temporary_path = Path(temporary_dir) / chunk.output_filename
        cookies_path = Path(temporary_dir) / "cookies.txt"

        def curl(command: list[str]) -> None:
            completed = subprocess.run(
                command,
                capture_output=True,
                text=True,
                timeout=DOWNLOAD_TIMEOUT_SECONDS + 15,
                check=False,
            )
            if completed.returncode != 0:
                detail = completed.stderr.strip() or completed.stdout.strip()
                raise RuntimeError(f"curl failed ({completed.returncode}): {detail}")

        curl([
            "curl", "--fail", "--silent", "--show-error", "--location",
            "--connect-timeout", "15", "--max-time", "30",
            "--cookie-jar", str(cookies_path), "--user-agent", user_agent,
            TARGET_URL,
        ])
        command = [
            "curl", "--fail", "--silent", "--show-error", "--location",
            "--connect-timeout", "15", "--max-time", str(DOWNLOAD_TIMEOUT_SECONDS),
            "--cookie", str(cookies_path), "--user-agent", user_agent,
            "--header", "Accept: application/octet-stream,*/*;q=0.8",
            "--header", f"Referer: {TARGET_URL}",
            "--header", "Origin: https://www.samco.in",
        ]
        for name, value in fields:
            command.extend(("--data-urlencode", f"{name}={value}"))
        command.extend(("--output", str(temporary_path), DOWNLOAD_ENDPOINT))
        curl(command)

        payload = temporary_path.read_bytes()
        if not payload.startswith(b"PK"):
            preview = payload[:200].decode("utf-8", errors="replace")
            if "no file available" in preview.lower():
                raise NoDataAvailable(
                    f"Samco has no BhavCopy archive for {chunk.label}"
                )
            raise RuntimeError(f"Samco returned a non-ZIP response: {preview}")
        # ``temporary_path`` is on the driver's local /tmp filesystem while
        # ``output_path`` is a Unity Catalog Volume. A direct os.replace across
        # those filesystems raises EXDEV (Invalid cross-device link). Copy to a
        # partial file in the Volume first, then rename within that filesystem.
        volume_partial_path = output_path.with_suffix(output_path.suffix + ".part")
        shutil.copyfile(temporary_path, volume_partial_path)
        volume_partial_path.replace(output_path)
    validate_download(output_path)
    log.info(
        "SUCCESS | %s | saved %s (%.1f KB)",
        chunk.label,
        output_path.name,
        output_path.stat().st_size / 1024,
    )
    return output_path


def download_chunk_with_retries(
    chunk: DateChunk,
    download_dir: Path,
    screenshots_dir: Path,
    state: DownloadState,
    state_path: Path,
    progress: tuple[int, int],
    delay_ctrl: AdaptiveDelay,
    log: logging.Logger,
    allow_split: bool = True,
) -> bool:
    """
    Retry a single chunk up to MAX_RETRIES times with exponential backoff.

    Returns True when the chunk is successfully downloaded and validated.
    """
    idx, total = progress
    log.info("Progress [%d/%d] %s", idx, total, chunk.label)

    last_error: Exception | None = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            download_chunk(chunk, download_dir, attempt, log)
            state.mark_done(chunk)
            state.save(state_path)
            delay_ctrl.on_success()
            return True

        except NoDataAvailable as exc:
            # A weekend, exchange holiday, or source range without an archive
            # must not block the rest of the DAG. Do not mark it complete in
            # state.json so a later backfill can retry it explicitly.
            log.warning("SKIP | %s | %s", chunk.label, exc)
            return True

        except (subprocess.TimeoutExpired, TimeoutError, ConnectionError, RuntimeError, OSError, ValueError) as exc:
            last_error = exc
            delay_ctrl.on_failure()
            log.error(
                "RETRY | %s | %s | attempt %d/%d | %s",
                type(exc).__name__,
                chunk.label,
                attempt,
                MAX_RETRIES,
                exc,
            )

        except Exception as exc:
            last_error = exc
            delay_ctrl.on_failure()
            log.exception(
                "RETRY | Unexpected error | %s | attempt %d/%d",
                chunk.label,
                attempt,
                MAX_RETRIES,
            )

        if attempt < MAX_RETRIES:
            log.warning(
                "Scheduling retry %d/%d for %s (fresh context)",
                attempt + 1,
                MAX_RETRIES,
                chunk.label,
            )
            exponential_backoff(attempt, log, delay_ctrl)

    log.error(
        "FAILED | %s | exhausted %d retries | last error: %s",
        chunk.label,
        MAX_RETRIES,
        last_error,
    )
    # Samco intermittently returns HTTP 500 for a particular multi-day range.
    # Split only that failed range into two smaller requests so the rest of a
    # long backfill remains resumable and the parent range is marked complete
    # only after both child requests succeed.
    if allow_split and chunk.start < chunk.end:
        midpoint = chunk.start + (chunk.end - chunk.start) // 2
        children = (DateChunk(chunk.start, midpoint), DateChunk(midpoint + timedelta(days=1), chunk.end))
        log.warning("SPLIT | %s | retrying as %s and %s", chunk.label, children[0].label, children[1].label)
        child_ok = all(
            download_chunk_with_retries(
                child, download_dir, screenshots_dir, state, state_path,
                progress, delay_ctrl, log, allow_split=True
            )
            for child in children
        )
        if child_ok:
            state.mark_done(chunk)
            state.save(state_path)
            log.info("RECOVERED | %s | completed via smaller child ranges", chunk.label)
            return True
    return False


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def ensure_directories() -> tuple[Path, Path, Path]:
    """Create downloads/, screenshots/, and logs/ if they do not exist."""
    download_dir = Path(DOWNLOAD_DIR)
    screenshots_dir = Path(SCREENSHOTS_DIR)
    logs_dir = Path(LOGS_DIR)
    for directory in (download_dir, screenshots_dir, logs_dir):
        directory.mkdir(parents=True, exist_ok=True)
    return download_dir, screenshots_dir, logs_dir


def run() -> int:
    """
    Orchestrate chunked downloads from START_DATE through END_DATE.

    Returns exit code 0 on full success, 1 if any chunk failed.
    """
    download_dir, screenshots_dir, logs_dir = ensure_directories()
    log = setup_logging(logs_dir)
    state_path = Path(STATE_FILE)
    state = DownloadState.load(state_path)

    log.info("=" * 64)
    log.info("Samco BhavCopy Downloader (chunked) — starting")
    log.info("=" * 64)

    start = parse_date(os.getenv("BHAVCOPY_START_DATE", START_DATE), "START_DATE")
    end = resolve_end_date()
    chunks = generate_chunks(start, end, CHUNK_DAYS)

    log.info("START_DATE  : %s", start)
    log.info("END_DATE    : %s", end)
    log.info("CHUNK_DAYS  : %d", CHUNK_DAYS)
    log.info("Total chunks: %d", len(chunks))
    log.info("DOWNLOAD_DIR: %s", download_dir.resolve())
    log.info("Source      : Samco direct HTTPS download endpoint")
    log.info("MAX_RETRIES : %d", MAX_RETRIES)
    log.info(
        "Delay tiers : fast %d–%ds | slow %d–%ds (escalates on errors)",
        MIN_DELAY,
        MAX_DELAY,
        SLOW_MIN_DELAY,
        SLOW_MAX_DELAY,
    )
    log.info("State file  : %s (%d completed)", state_path, len(state.completed))

    if not chunks:
        log.warning("No chunks to process.")
        return 0

    # --- Determine which chunks still need downloading ---
    pending: list[DateChunk] = []
    skipped = 0
    for chunk in chunks:
        if state.is_done(chunk) and chunk_already_on_disk(chunk, download_dir):
            skipped += 1
            log.info("SKIP | %s | already in state.json", chunk.label)
            continue
        if chunk_already_on_disk(chunk, download_dir):
            log.info("SKIP | %s | valid file on disk — updating state", chunk.label)
            state.mark_done(chunk)
            skipped += 1
            continue
        pending.append(chunk)

    state.save(state_path)
    log.info("Pending chunks: %d | Skipped: %d", len(pending), skipped)

    if not pending:
        log.info("All chunks already downloaded.")
        return 0

    successes = 0
    failures = 0
    total = len(pending)
    delay_ctrl = AdaptiveDelay()

    for index, chunk in enumerate(pending, start=1):
        ok = download_chunk_with_retries(
            chunk=chunk,
            download_dir=download_dir,
            screenshots_dir=screenshots_dir,
            state=state,
            state_path=state_path,
            progress=(index, total),
            delay_ctrl=delay_ctrl,
            log=log,
        )
        if ok:
            successes += 1
            if index < total:
                delay_ctrl.sleep(log)
        else:
            failures += 1

    log.info("=" * 64)
    log.info(
        "Finished | success: %d | failed: %d | skipped: %d | total range chunks: %d",
        successes,
        failures,
        skipped,
        len(chunks),
    )
    log.info("=" * 64)
    return 1 if failures else 0


if __name__ == "__main__":
    if sys.version_info < (3, 10):
        print("ERROR: Python 3.10+ is required.", file=sys.stderr)
        sys.exit(2)

    try:
        sys.exit(run())
    except KeyboardInterrupt:
        logging.getLogger("bhavcopy_downloader").warning("Interrupted by user")
        sys.exit(130)
    except Exception as exc:
        logging.getLogger("bhavcopy_downloader").exception("Fatal error: %s", exc)
        sys.exit(1)
