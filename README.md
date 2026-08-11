# BhavCopy Databricks Pipeline

Production-oriented Databricks Asset Bundle that downloads daily Samco BhavCopy archives, validates and organizes exchange files, and publishes incremental Delta tables in Unity Catalog.

## Pipeline flow

```text
Samco source -> download -> extract -> organize -> schema extraction
             -> validation -> incremental Delta publish -> archive -> summary
```

The source currently provides BhavCopy data from **2016-04-01 onward**. Weekends and exchange holidays may have no archive.

## Databricks resources

| Resource | Value |
|---|---|
| Catalog | `stock_project` |
| Schema | `stock_project.market_data` |
| Volume | `/Volumes/stock_project/market_data/pipeline_files` |
| Job ID | `625809917242574` |
| Schedule | Daily at 7:00 PM |
| Time zone | `Asia/Kolkata` |

Delta tables:

- `stock_project.market_data.bhavcopy_bse`
- `stock_project.market_data.bhavcopy_mcx`
- `stock_project.market_data.bhavcopy_nse`
- `stock_project.market_data.bhavcopy_nsefo`
- `stock_project.market_data.pipeline_task_audit`

## Incremental processing

Scheduled runs use the bundle value `auto`, which resolves to the current date in `Asia/Kolkata`.

- The downloader state prevents duplicate downloads.
- Downstream stages process active/new files only.
- Publish compares `_source_file` with existing Delta data and appends only unseen files.
- Existing Delta records are preserved; daily runs do not overwrite the tables.
- After successful publish, inputs move to:

```text
/Volumes/stock_project/market_data/pipeline_files/processed/downloads
/Volumes/stock_project/market_data/pipeline_files/processed/extracted
/Volumes/stock_project/market_data/pipeline_files/processed/organized
```

Failed runs leave active files in place so they can be retried safely.

## Repository structure

```text
bhavcopy_downloader.py          Samco download, retries, and state handling
bhavcopy_extractor.py           ZIP extraction
bhavcopy_organizer.py           Exchange classification and routing
bhavcopy_schema_extractor.py    Header/sample inspection
bhavcopy_schema_validator.py    Schema validation
databricks_pipeline.py          Task entry point and Delta publishing
google_chat_notifier.py         Secret-backed notifications and audit writes
pipeline_logging.py             Databricks-safe logging helpers
resources/bhavcopy.job.yml      Job graph and schedule
databricks.yml                  Bundle variables and target configuration
```

## Prerequisites

- Databricks CLI configured with the `jagadeeswaran` profile.
- Unity Catalog permissions for the catalog, schema, tables, and volume.
- Databricks compute with Python and Spark.
- Access to the Samco BhavCopy source.

Validate the bundle:

```bash
databricks bundle validate -t dev --profile jagadeeswaran
```

## Deploy

```bash
databricks bundle deploy -t dev --profile jagadeeswaran \
  --var google_chat_enabled=true \
  --var google_chat_webhook_secret_scope=bhavcopy-alerts \
  --var google_chat_webhook_secret_key=google-chat-webhook
```

The deployed job is enabled and scheduled for 7:00 PM Asia/Kolkata.

## Google Chat secret setup

The webhook is read only at runtime from a Databricks secret. Never commit the URL, token, or secret value.

```bash
databricks secrets create-scope bhavcopy-alerts --profile jagadeeswaran
databricks secrets put-secret bhavcopy-alerts google-chat-webhook \
  --string-value '<WEBHOOK_URL>' --profile jagadeeswaran
```

Set `google_chat_enabled=false` to disable notifications. Notification failures are logged without failing the data pipeline.

## Manual runs

Run the production-style current-day range:

```bash
databricks bundle run bhavcopy_processing -t dev --profile jagadeeswaran
```

Run a bounded verification range:

```bash
databricks bundle deploy -t dev --profile jagadeeswaran \
  --var date_range_start=2020-01-01 \
  --var date_range_end=2020-01-01
databricks bundle run bhavcopy_processing -t dev --profile jagadeeswaran
```

Restore automatic daily dates afterward:

```bash
databricks bundle deploy -t dev --profile jagadeeswaran \
  --var date_range_start=auto \
  --var date_range_end=auto
```

## Monitoring and audit

The `notify_run_summary` task uses `ALL_DONE`, so it runs even if an upstream task fails. Operational records are stored in the audit table:

```sql
SELECT run_id, task_name, status, start_time, end_time,
       duration_seconds, input_count, output_count,
       new_count, existing_count, error_message
FROM stock_project.market_data.pipeline_task_audit
ORDER BY start_time DESC;
```

Coverage example:

```sql
SELECT YEAR(trade_date) AS year,
       COUNT(DISTINCT _source_file) AS files,
       COUNT(*) AS records,
       MIN(trade_date) AS first_date,
       MAX(trade_date) AS last_date
FROM stock_project.market_data.bhavcopy_nse
GROUP BY YEAR(trade_date)
ORDER BY year;
```

## Troubleshooting

**No ZIP files found:** Check the download task and volume path. A weekend or exchange holiday may legitimately have no file.

**HTTP download failures:** The downloader retries failed chunks and can split a failed multi-day chunk into smaller ranges. Rerunning the same range is safe because successful dates remain in state.

**Schema validation failure:** Review the schema extraction and validation reports before publishing a changed source format.

**No Chat message:** Verify the secret scope/key and `google_chat_enabled=true`. The data pipeline continues even if the webhook is unavailable.

## Security

- Never commit webhook URLs, tokens, credentials, or exported secret values.
- Store webhooks in Databricks secret scopes.
- Keep raw and operational data in the Unity Catalog Volume, not Git.
- Rotate the webhook if it has ever been exposed in chat, logs, screenshots, or source control.

## Source repository

[Jagadeeswaran-DE/databricks-bhavcopy-pipeline](https://github.com/Jagadeeswaran-DE/databricks-bhavcopy-pipeline)
