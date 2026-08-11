"""Logging helpers that stream task progress safely in Databricks."""

from __future__ import annotations

import logging
import sys
from pathlib import Path


class DatabricksProgressHandler(logging.StreamHandler):
    """Write live log lines without Databricks' unsupported stream flush."""

    def flush(self) -> None:
        # Databricks Serverless' notebook stream can raise ``Illegal seek``
        # when logging.StreamHandler calls flush. Each write is still captured.
        return


def configure_logging(name: str, log_path: Path) -> logging.Logger:
    """Create one live task-output handler and one persistent file handler."""
    # Databricks notebook/serverless streams can reject ``flush()`` with
    # ``OSError: [Errno 29] Illegal seek``.  Logging must never turn that
    # output-stream quirk into a visible error (or affect the pipeline).
    logging.raiseExceptions = False
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    logger.propagate = False

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    progress = DatabricksProgressHandler(sys.stdout)
    progress.setLevel(logging.INFO)
    progress.setFormatter(formatter)

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    logger.addHandler(progress)
    logger.addHandler(file_handler)
    return logger


def task_progress(message: str) -> None:
    """Emit an immediate, Databricks-visible lifecycle/progress line."""
    sys.stdout.write(message + "\n")
