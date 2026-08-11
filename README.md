# BhavCopy Databricks pipeline

The Asset Bundle runs the existing download, extract, organize, schema, validation, and Delta publish tasks. It also writes operational records to `stock_project.market_data.pipeline_task_audit` and has one `ALL_DONE` summary task.

## Google Chat notifications

The webhook is read only at runtime from a Databricks secret. Never commit the URL, token, or secret value.

```bash
databricks secrets create-scope bhavcopy-alerts --profile jagadeeswaran
databricks secrets put-secret bhavcopy-alerts google-chat-webhook --string-value '<WEBHOOK_URL>' --profile jagadeeswaran
```

Enable notifications during deployment without putting the webhook in YAML or Git:

```bash
databricks bundle deploy -t dev --profile jagadeeswaran \
  --var google_chat_enabled=true \
  --var google_chat_webhook_secret_scope=bhavcopy-alerts \
  --var google_chat_webhook_secret_key=google-chat-webhook
```

When disabled (the default), the pipeline still runs and writes the audit table. If the webhook is unavailable, notification errors are logged and the data pipeline continues.

## Date ranges

Override the bundle defaults for a safe verification run:

```bash
databricks bundle run bhavcopy_processing -t dev --profile jagadeeswaran \
  --var date_range_start=2020-01-01 --var date_range_end=2020-01-01
```
