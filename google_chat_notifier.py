"""Non-blocking Google Chat notifications and Delta audit records."""

from __future__ import annotations

import json
import base64
import logging
import os
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import Row, SparkSession
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, StructField, StructType, TimestampType,
)

LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class NotificationConfig:
    enabled: bool
    secret_scope: str
    secret_key: str
    run_id: str
    job_run_id: str
    date_range_start: str
    date_range_end: str
    run_url: str = ""


def config_from_args(args: Any) -> NotificationConfig:
    run_id = getattr(args, "run_id", "unknown")
    job_run_id = getattr(args, "job_run_id", "unknown")
    # Some job runtimes pass unresolved dynamic-value expressions literally;
    # prefer their runtime environment values when that happens.
    if str(run_id).startswith("{{"):
        run_id = os.getenv("DB_JOB_RUN_ID", os.getenv("DATABRICKS_RUN_ID", "unknown"))
    if str(job_run_id).startswith("{{"):
        job_run_id = os.getenv("DB_JOB_ID", os.getenv("DATABRICKS_JOB_ID", "unknown"))
    return NotificationConfig(
        enabled=str(getattr(args, "google_chat_enabled", "false")).lower() == "true",
        secret_scope=getattr(args, "google_chat_webhook_secret_scope", ""),
        secret_key=getattr(args, "google_chat_webhook_secret_key", ""),
        run_id=run_id,
        job_run_id=job_run_id,
        date_range_start=getattr(args, "date_range_start", ""),
        date_range_end=getattr(args, "date_range_end", ""),
        run_url=getattr(args, "run_url", ""),
    )


class GoogleChatNotifier:
    def __init__(self, config: NotificationConfig, spark: SparkSession | None = None):
        self.config = config
        self.spark = spark
        self._webhook: str | None = None

    def _secret(self) -> str | None:
        if not self.config.enabled:
            return None
        if self._webhook is not None:
            return self._webhook
        try:
            if not self.config.secret_scope or not self.config.secret_key:
                raise ValueError("Google Chat secret scope/key is not configured")
            try:
                from databricks.sdk import WorkspaceClient
                secret = WorkspaceClient().secrets.get_secret(
                    scope=self.config.secret_scope, key=self.config.secret_key
                )
                raw = secret.value
                try:
                    decoded = base64.b64decode(raw).decode("utf-8")
                    self._webhook = decoded if decoded.startswith("https://") else raw
                except Exception:
                    self._webhook = raw
                return self._webhook
            except Exception:
                try:
                    from dbruntime.dbutils import DBUtils
                    dbutils = DBUtils()
                except Exception:
                    from pyspark.dbutils import DBUtils
                    spark = self.spark or SparkSession.builder.getOrCreate()
                    dbutils = DBUtils(spark.sparkContext)
            self._webhook = dbutils.secrets.get(
                scope=self.config.secret_scope, key=self.config.secret_key
            )
            return self._webhook
        except Exception as exc:  # notifications must never stop data work
            LOG.warning("Google Chat secret unavailable: %s", exc)
            return None

    def send(self, message: str) -> None:
        webhook = self._secret()
        if not webhook:
            return
        try:
            body = json.dumps({"text": message}).encode("utf-8")
            request = urllib.request.Request(
                webhook, data=body, headers={"Content-Type": "application/json"}, method="POST"
            )
            with urllib.request.urlopen(request, timeout=10) as response:
                if response.status >= 300:
                    raise RuntimeError(f"HTTP {response.status}")
            LOG.info("Google Chat notification delivered")
        except Exception as exc:
            # Never include the URL, secret, or request body in logs. Some
            # HTTP errors echo the request URL, so log only the exception type.
            LOG.warning("Google Chat notification failed (%s)", type(exc).__name__)


AUDIT_TABLE = "pipeline_task_audit"
AUDIT_SCHEMA = StructType([
    StructField("run_id", StringType(), False), StructField("task_name", StringType(), False),
    StructField("status", StringType(), False), StructField("start_time", TimestampType(), True),
    StructField("end_time", TimestampType(), True), StructField("duration_seconds", DoubleType(), True),
    StructField("input_count", IntegerType(), True), StructField("output_count", IntegerType(), True),
    StructField("new_count", IntegerType(), True), StructField("existing_count", IntegerType(), True),
    StructField("skipped_count", IntegerType(), True), StructField("failed_count", IntegerType(), True),
    StructField("message", StringType(), True), StructField("error_message", StringType(), True),
    StructField("date_range_start", StringType(), True), StructField("date_range_end", StringType(), True),
])


def ensure_audit_table(spark: SparkSession, target_schema: str) -> str:
    table = f"{target_schema}.{AUDIT_TABLE}"
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {table} (
      run_id STRING, task_name STRING, status STRING, start_time TIMESTAMP,
      end_time TIMESTAMP, duration_seconds DOUBLE, input_count INT, output_count INT,
      new_count INT, existing_count INT, skipped_count INT, failed_count INT,
      message STRING, error_message STRING, date_range_start STRING, date_range_end STRING
    ) USING DELTA""")
    return table


def write_audit(spark: SparkSession, target_schema: str, *, run_id: str, task_name: str,
                status: str, start_time: datetime | None = None, end_time: datetime | None = None,
                input_count: int | None = None, output_count: int | None = None,
                new_count: int | None = None, existing_count: int | None = None,
                skipped_count: int | None = None, failed_count: int | None = None,
                message: str = "", error_message: str = "", date_range_start: str = "",
                date_range_end: str = "") -> None:
    try:
        ensure_audit_table(spark, target_schema)
        duration = ((end_time - start_time).total_seconds()
                    if start_time and end_time else None)
        row = Row(run_id, task_name, status, start_time, end_time, duration, input_count,
                  output_count, new_count, existing_count, skipped_count, failed_count,
                  message[:4000], error_message[:4000], date_range_start, date_range_end)
        spark.createDataFrame([row], AUDIT_SCHEMA).write.mode("append").saveAsTable(
            f"{target_schema}.{AUDIT_TABLE}"
        )
    except Exception as exc:
        LOG.warning("Audit write failed: %s", exc)


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)
