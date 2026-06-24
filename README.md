# jira-ingest

Async Jira data pipeline supporting Data Center and Cloud, with pluggable multi-protocol output (local filesystem, S3, Azure Blob, GCS).

## Features

- Dual-mode: Jira Data Center (Bearer PAT + optional mTLS) and Jira Cloud (Basic Auth)
- Async fetching with concurrent project processing, caching, and exponential backoff retry
- Configurable custom fields extraction (generic field ID mapping, no hardcoding)
- PII hashing for assignee/author fields
- Output formats: Parquet, CSV, JSON Lines
- Output destinations: local filesystem, S3, Azure Blob Storage, GCS (via fsspec)
- Optional Redshift loading via COPY from S3
- Click CLI with `run` and `validate` commands
- Pydantic v2 settings and data schemas
- ruff + mypy + pre-commit + GitHub Actions CI

## Quick start

```bash
cp .env.example .env
# edit .env with your Jira URL and credentials
pip install -e ".[dev]"
jira-ingest validate
jira-ingest run
```

## Output layout

```
{sink_uri}/
  issues/issues_{date}.parquet
  projects/projects_{date}.parquet
  releases/releases_{date}.parquet
  boards/boards_{date}.parquet
  transitions/transitions_{date}.parquet
```

## Sink examples

| Destination | JIRA_SINK_URI |
|---|---|
| Local | `./output` |
| S3 | `s3://my-bucket/jira-ingest` |
| Azure Blob | `az://my-container/jira-ingest` |
| GCS | `gs://my-bucket/jira-ingest` |
