# jira-ingest

[![PyPI version](https://img.shields.io/pypi/v/pipewell-jira-ingest.svg)](https://pypi.org/project/pipewell-jira-ingest/)
[![Python versions](https://img.shields.io/pypi/pyversions/pipewell-jira-ingest.svg)](https://pypi.org/project/pipewell-jira-ingest/)
[![Licence: MIT](https://img.shields.io/badge/Licence-MIT-blue.svg)](LICENSE)

Async Jira data pipeline for Data Center and Cloud. Fetches projects, releases, boards, issues, and transitions; writes Parquet, CSV, or JSON Lines to local disk, S3, Azure Blob, or GCS; and optionally loads records into any SQLAlchemy-compatible database.

## Features

- Dual-mode: Jira Data Center (Bearer PAT + optional mTLS) and Jira Cloud (Basic Auth)
- Async fetching with concurrent project processing, in-memory caching, and exponential-backoff retry
- Configurable custom fields extraction -- map any `customfield_XXXXX` to a logical name
- PII hashing for assignee and author fields
- Output formats: Parquet (Snappy), CSV, JSON Lines
- Output destinations: local filesystem, S3, Azure Blob, GCS via [fsspec](https://filesystem-spec.readthedocs.io)
- Pluggable database loader: PostgreSQL, Redshift (with S3 COPY fast path), Snowflake, DuckDB, SQLite
- Click CLI with `run` and `validate` commands
- Pydantic v2 settings and data schemas
- ruff + mypy strict + pre-commit + GitHub Actions CI

## Quick start

```bash
pip install pipewell-jira-ingest
cp .env.example .env   # edit with your Jira URL and credentials
jira-ingest validate   # confirm connectivity
jira-ingest run        # fetch everything and write to ./output
```

For database loading, install the optional extra:

```bash
pip install "pipewell-jira-ingest[database]"   # PostgreSQL, SQLite, etc.
pip install "pipewell-jira-ingest[redshift]"   # Redshift with S3 COPY fast path
```

## Documentation

| Guide | Description |
|---|---|
| [Authentication](docs/authentication.md) | Jira Cloud vs Data Center, PAT vs Basic Auth, mTLS certificates, scoping by project |
| [Output sinks](docs/sinks.md) | Local filesystem, S3, Azure Blob, GCS -- URIs, auth options, output layout |
| [Database loading](docs/database-loading.md) | PostgreSQL, Redshift S3 COPY, Snowflake, DuckDB, SQLite; programmatic API |
| [Custom fields](docs/custom-fields.md) | Mapping `customfield_XXXXX` IDs to logical names, finding field IDs |

## Configuration reference

All settings are read from environment variables (or a `.env` file) with the prefix `JIRA_`.

| Variable | Default | Description |
|---|---|---|
| `JIRA_MODE` | `cloud` | `cloud` or `dc` |
| `JIRA_URL` | required | Jira base URL |
| `JIRA_API_TOKEN` | required | API token (Cloud) or PAT (DC) |
| `JIRA_EMAIL` | required for Cloud | Account email |
| `JIRA_CERT_PEM` | | Base64-encoded PEM for mTLS (DC only) |
| `JIRA_PROJECT_KEYS` | all projects | Comma-separated project keys to scope the run |
| `JIRA_OUTPUT_FORMAT` | `parquet` | `parquet`, `csv`, or `jsonl` |
| `JIRA_SINK_URI` | `./output` | fsspec URI for output destination |
| `JIRA_SINK_OPTIONS` | `{}` | JSON dict of auth options forwarded to fsspec |
| `JIRA_CUSTOM_FIELDS` | `{}` | JSON dict mapping logical name to Jira field ID |
| `JIRA_LOG_LEVEL` | `INFO` | Log verbosity |
| `DATABASE_URL` | | SQLAlchemy URL to load into a database after writing |
| `DATABASE_SCHEMA` | | Target schema (used with `DATABASE_URL`) |
| `REDSHIFT_IAM_ROLE` | | IAM role ARN for Redshift S3 COPY |

## CLI

```
jira-ingest run [OPTIONS]

  --env-file TEXT            Path to .env file  [default: .env]
  --start-date TEXT          Filter issues from date (YYYY-MM-DD)
  --end-date TEXT            Filter issues until date (YYYY-MM-DD)
  --date-suffix TEXT         Output file date suffix  [default: today]
  --database-url TEXT        SQLAlchemy URL to load into a database
  --db-schema TEXT           Target database schema
  --redshift-iam-role TEXT   IAM role ARN for Redshift S3 COPY

jira-ingest validate [OPTIONS]

  --env-file TEXT            Path to .env file  [default: .env]
```

## Output layout

```
{JIRA_SINK_URI}/
  issues/issues_{date}.parquet
  projects/projects_{date}.parquet
  releases/releases_{date}.parquet
  boards/boards_{date}.parquet
  transitions/transitions_{date}.parquet
```

## Development

```bash
pip install -e ".[dev]"
pre-commit install
pytest
```
