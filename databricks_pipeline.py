#!/usr/bin/env python3
"""Run the non-browser BhavCopy pipeline stages in a Databricks job.

Raw ZIP archives must already be in ``<data-path>/downloads``. The script keeps
all derived files in the same Unity Catalog volume, then writes one Delta table
per exchange in the supplied Unity Catalog schema.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import time
import threading
from datetime import date
from pathlib import Path
from zoneinfo import ZoneInfo

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, regexp_extract, to_date

import bhavcopy_extractor
import bhavcopy_downloader
import bhavcopy_organizer
import bhavcopy_schema_extractor
import bhavcopy_schema_validator
from pipeline_logging import task_progress
from google_chat_notifier import (
    GoogleChatNotifier,
    config_from_args,
    ensure_audit_table,
    utc_now,
    write_audit,
)


EXCHANGES = ("bse", "mcx", "nse", "nsefo")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data-path",
        required=True,
        help="Unity Catalog volume path containing the downloads directory.",
    )
    parser.add_argument(
        "--target-schema",
        required=True,
        help="Three-level Unity Catalog schema name for the published Delta tables.",
    )
    parser.add_argument(
        "--stage",
        required=True,
        choices=(
            "download",
            "extract",
            "organize",
            "extract_schema",
            "validate_schema",
            "publish",
            "archive",
            "notify_summary",
        ),
        help="Pipeline stage executed by this Databricks task.",
    )
    parser.add_argument("--run-id", default=os.getenv("DB_RUN_ID", "unknown"))
    parser.add_argument("--job-run-id", default=os.getenv("DB_JOB_RUN_ID", "unknown"))
    parser.add_argument("--run-url", default=os.getenv("DB_RUN_URL", ""))
    parser.add_argument("--date-range-start", default=bhavcopy_downloader.START_DATE)
    parser.add_argument("--date-range-end", default=bhavcopy_downloader.END_DATE)
    parser.add_argument("--google-chat-enabled", default="false")
    parser.add_argument("--google-chat-webhook-secret-scope", default="")
    parser.add_argument("--google-chat-webhook-secret-key", default="")
    return parser.parse_args()


def require_success(name: str, status: int) -> None:
    if status != 0:
        raise RuntimeError(f"{name} exited with status {status}")


def expected_chunks(start: str, end: str) -> int:
    from datetime import date
    return ((date.fromisoformat(end) - date.fromisoformat(start)).days // 10) + 1


def resolve_runtime_dates(args: argparse.Namespace) -> None:
    """Resolve bundle's ``auto`` date values using the business timezone."""
    today = date.today()
    try:
        today = date.today()  # Databricks workers use the job's local date.
        today = date.fromisoformat(
            __import__("datetime").datetime.now(ZoneInfo("Asia/Kolkata")).date().isoformat()
        )
    except Exception:
        pass
    if str(args.date_range_start).lower() in {"auto", "today"}:
        args.date_range_start = today.isoformat()
    if str(args.date_range_end).lower() in {"auto", "today"}:
        args.date_range_end = today.isoformat()


def archive_processed(data_path: Path, target_schema: str, spark: SparkSession,
                      args: argparse.Namespace) -> None:
    """Move successfully processed active inputs into a durable processed area."""
    started = utc_now()
    ensure_audit_table(spark, target_schema)
    counts = {}
    for name in ("downloads", "extracted", "organized"):
        source = data_path / name
        destination = data_path / "processed" / name
        moved = 0
        if source.exists():
            for item in list(source.iterdir()):
                destination.mkdir(parents=True, exist_ok=True)
                target = destination / item.name
                if target.exists():
                    if item.is_dir():
                        shutil.copytree(item, target, dirs_exist_ok=True)
                        shutil.rmtree(item)
                    else:
                        item.unlink()
                else:
                    shutil.move(str(item), str(target))
                moved += 1
        counts[name] = moved
    ended = utc_now()
    write_audit(
        spark, target_schema, run_id=args.run_id, task_name="archive",
        status="SUCCEEDED", start_time=started, end_time=ended,
        input_count=sum(counts.values()), output_count=sum(counts.values()),
        message=json.dumps(counts), date_range_start=args.date_range_start,
        date_range_end=args.date_range_end,
    )
    task_progress("NODE COMPLETE | archive")


def run_stage(stage: str, data_path: Path, args: argparse.Namespace) -> None:
    os.chdir(data_path)
    spark = SparkSession.builder.getOrCreate()
    notifier = GoogleChatNotifier(config_from_args(args), spark)
    started = utc_now()
    task_name = {
        "download": "download", "extract": "extract", "organize": "organize",
        "extract_schema": "schema_extraction", "validate_schema": "validation",
    }[stage]
    ensure_audit_table(spark, args.target_schema)
    write_audit(spark, args.target_schema, run_id=args.run_id, task_name=task_name,
                status="STARTED", start_time=started, date_range_start=args.date_range_start,
                date_range_end=args.date_range_end, message="Task started")
    task_progress(f"NODE START | {stage}")
    try:
        if stage == "download":
            before_downloads = len(list((data_path / "downloads").glob("*.zip")))
            os.environ["BHAVCOPY_START_DATE"] = args.date_range_start
            os.environ["BHAVCOPY_END_DATE"] = args.date_range_end
            require_success("downloader", bhavcopy_downloader.run())
            after_downloads = len(list((data_path / "downloads").glob("*.zip")))
            download_counts = {
                "new_files": max(after_downloads - before_downloads, 0),
                "existing_files": before_downloads,
                "total_files": after_downloads,
            }
        else:
            stages = {
                "extract": ("extractor", bhavcopy_extractor.run),
                "organize": ("organizer", bhavcopy_organizer.run),
                "extract_schema": ("schema extractor", bhavcopy_schema_extractor.run),
                "validate_schema": ("schema validator", bhavcopy_schema_validator.run),
            }
            name, action = stages[stage]
            require_success(name, action())
        ended = utc_now()
        write_audit(spark, args.target_schema, run_id=args.run_id, task_name=task_name,
                    status="SUCCEEDED", start_time=started, end_time=ended,
                    date_range_start=args.date_range_start, date_range_end=args.date_range_end,
                    message=json.dumps(download_counts) if stage == "download" else "Task completed",
                    input_count=(download_counts.get("total_files") if stage == "download" else None),
                    output_count=(download_counts.get("total_files") if stage == "download" else None),
                    new_count=(download_counts.get("new_files") if stage == "download" else None),
                    existing_count=(download_counts.get("existing_files") if stage == "download" else None))
        task_progress(f"NODE COMPLETE | {stage}")
    except Exception as exc:
        ended = utc_now()
        write_audit(spark, args.target_schema, run_id=args.run_id, task_name=task_name,
                    status="FAILED", start_time=started, end_time=ended,
                    date_range_start=args.date_range_start, date_range_end=args.date_range_end,
                    error_message=str(exc), message="Task failed")
        raise


def publish_tables(data_path: Path, target_schema: str, spark: SparkSession, args: argparse.Namespace) -> None:
    started = utc_now()
    notifier = GoogleChatNotifier(config_from_args(args), spark)
    ensure_audit_table(spark, target_schema)
    write_audit(spark, target_schema, run_id=args.run_id, task_name="publish",
                status="STARTED", start_time=started, date_range_start=args.date_range_start,
                date_range_end=args.date_range_end, message="Task started")
    task_progress("NODE START | publish")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_schema}")
    details = {}
    for index, exchange in enumerate(EXCHANGES, start=1):
        source = data_path / "organized" / exchange
        source_files = list(source.glob("*.csv")) if source.exists() else []
        if not source_files:
            task_progress(
                f"PUBLISH [{index}/{len(EXCHANGES)}] SKIP | {exchange} | no CSV files"
            )
            details[exchange] = {"new_files": 0, "existing_files": 0, "total_files": 0,
                                "new_records": 0, "existing_records": 0, "total_records": 0}
            continue

        table_name = f"{target_schema}.bhavcopy_{exchange}"
        previous = set()
        if spark.catalog.tableExists(table_name):
            previous = {r[0] for r in spark.table(table_name).select("_source_file").distinct().collect()}
        task_progress(
            f"PUBLISH [{index}/{len(EXCHANGES)}] START | {table_name} | "
            f"input_files={len(source_files)}"
        )
        frame = (
            spark.read.option("header", True)
            .option("inferSchema", False)
            .csv(str(source))
            .withColumn("_source_file", col("_metadata.file_path"))
            # The CSV date fields are strings and are not consistent across
            # exchanges. The filename is the canonical trading-date source:
            # YYYYMMDD_EXCHANGE.csv.
            .withColumn(
                "trade_date",
                to_date(
                    regexp_extract(
                        col("_source_file"),
                        r"(\d{8})_[A-Za-z0-9]+\.csv$",
                        1,
                    ),
                    "yyyyMMdd",
                ),
            )
            .withColumn("_ingested_at", current_timestamp())
        )
        source_names = {r[0] for r in frame.select("_source_file").distinct().collect()}
        new_names = source_names - previous
        new_records = frame.where(col("_source_file").isin(list(new_names))).count() if new_names else 0
        total_records = frame.count()
        details[exchange] = {
            "new_files": len(new_names), "existing_files": len(source_names & previous),
            "total_files": len(source_names), "new_records": new_records,
            "existing_records": total_records - new_records, "total_records": total_records,
        }
        if new_names:
            (frame.where(col("_source_file").isin(list(new_names))).write
                .mode("append").option("mergeSchema", "true").saveAsTable(table_name))
        task_progress(
            f"PUBLISH [{index}/{len(EXCHANGES)}] COMPLETE | {table_name}"
        )
    task_progress("NODE COMPLETE | publish")
    ended = utc_now()
    write_audit(spark, target_schema, run_id=args.run_id, task_name="publish",
                status="SUCCEEDED", start_time=started, end_time=ended,
                input_count=sum(v["total_files"] for v in details.values()),
                output_count=sum(v["total_records"] for v in details.values()),
                new_count=sum(v["new_files"] for v in details.values()),
                existing_count=sum(v["existing_files"] for v in details.values()),
                message=json.dumps(details), date_range_start=args.date_range_start,
                date_range_end=args.date_range_end)


def notify_summary(args: argparse.Namespace) -> None:
    spark = SparkSession.builder.getOrCreate()
    notifier = GoogleChatNotifier(config_from_args(args), spark)
    table = ensure_audit_table(spark, args.target_schema)
    all_rows = spark.table(table).where(col("run_id") == args.run_id).collect()
    status_rank = {"STARTED": 0, "RUNNING": 1, "SKIPPED": 2, "FAILED": 3, "SUCCEEDED": 4}
    latest = {}
    for row in all_rows:
        stamp = row.end_time or row.start_time
        key = (stamp, status_rank.get(row.status, -1))
        if row.task_name not in latest or key > latest[row.task_name][0]:
            latest[row.task_name] = (key, row)
    rows = [latest[name][1] for name in sorted(latest)]
    expected = {"download", "extract", "organize", "schema_extraction", "validation", "publish", "archive"}
    overall = "SUCCEEDED" if expected.issubset(latest) and all(
        latest[name][1].status in {"SUCCEEDED", "SKIPPED"} for name in expected
    ) else "FAILED"
    def render_table(headers, values):
        widths = [len(str(h)) for h in headers]
        for row in values:
            widths = [max(width, len(str(value))) for width, value in zip(widths, row)]
        fmt = " | ".join("{{:<{}}}".format(width) for width in widths)
        divider = "-+-".join("-" * width for width in widths)
        return [fmt.format(*headers), divider] + [fmt.format(*row) for row in values]

    publish = latest.get("publish", (None, None))[1]
    details = {}
    if publish and publish.message:
        try:
            parsed = json.loads(publish.message)
            details = parsed if isinstance(parsed, dict) else {}
        except (TypeError, json.JSONDecodeError):
            # Older audit rows may contain a human-readable message rather
            # than the structured publish-count payload.
            details = {}
    table_rows = []
    for exchange in EXCHANGES:
        d = details.get(exchange, {k: 0 for k in ("new_files", "existing_files", "total_files", "new_records", "existing_records", "total_records")})
        table_rows.append([f"bhavcopy_{exchange}", d["new_files"], d["existing_files"], d["total_files"], d["new_records"], d["existing_records"], d["total_records"]])
    archive_row = latest.get("archive", (None, None))[1]
    archive_counts = {}
    if archive_row and archive_row.message:
        try:
            archive_counts = json.loads(archive_row.message)
        except (TypeError, json.JSONDecodeError):
            archive_counts = {}
    zip_total = int(archive_counts.get("downloads", 0))
    download_row = latest.get("download", (None, None))[1]
    zip_new = int(download_row.new_count or 0) if download_row else 0
    zip_existing = max(zip_total - zip_new, 0)
    csv_new = sum(int(details.get(exchange, {}).get("new_files", 0)) for exchange in EXCHANGES)
    csv_total = sum(int(details.get(exchange, {}).get("new_files", 0)) for exchange in EXCHANGES)
    csv_existing = max(csv_total - csv_new, 0)
    task_order = ["download", "extract", "organize", "schema_extraction", "validation", "publish", "archive"]
    task_rows = []
    for name in task_order:
        if name in latest:
            r = latest[name][1]
            task_rows.append([name, r.status, f"{r.duration_seconds or 0:.1f}"])
    slow = [row[0] for row in task_rows if float(row[2]) > 600]
    lines = ["BhavCopy pipeline summary", f"Overall status: {overall}",
             f"Run ID: {args.run_id}", f"Date range: {args.date_range_start} to {args.date_range_end}", "",
             "Files", *render_table(["dataset", "new", "existing", "total"], [["ZIP archives", zip_new, zip_existing, zip_total], ["organized CSV files", csv_new, csv_existing, csv_total]]), "",
             "Delta tables", *render_table(["table_name", "new_files", "existing_files", "total_files", "new_records", "existing_records", "total_records"], table_rows),
             "", "Tasks", *render_table(["task", "status", "duration_seconds"], task_rows), "",
             "Slow tasks over 10 minutes: " + (", ".join(slow) if slow else "None")]
    failures = [f"{r.task_name}: {r.error_message}" for r in rows if r.status == "FAILED"]
    if failures:
        lines.append("Failure details: " + " | ".join(failures))
    notifier.send("```\n" + "\n".join(lines) + "\n```")


def main() -> None:
    args = parse_args()
    resolve_runtime_dates(args)
    data_path = Path(args.data_path).resolve()
    downloads = data_path / "downloads"
    if args.stage == "extract" and (
        not downloads.is_dir() or not any(downloads.glob("*.zip"))
    ):
        raise FileNotFoundError(
            f"No ZIP files found in {downloads}. Upload local downloads first."
        )

    if args.stage == "publish":
        try:
            publish_tables(data_path, args.target_schema, SparkSession.builder.getOrCreate(), args)
        except Exception as exc:
            spark = SparkSession.builder.getOrCreate()
            ended = utc_now()
            write_audit(spark, args.target_schema, run_id=args.run_id, task_name="publish",
                        status="FAILED", end_time=ended, date_range_start=args.date_range_start,
                        date_range_end=args.date_range_end, error_message=str(exc), message="Task failed")
            raise
    elif args.stage == "archive":
        archive_processed(data_path, args.target_schema, SparkSession.builder.getOrCreate(), args)
    elif args.stage == "notify_summary":
        notify_summary(args)
    else:
        run_stage(args.stage, data_path, args)


if __name__ == "__main__":
    main()
