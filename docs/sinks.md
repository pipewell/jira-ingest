# Output sinks

jira-ingest writes files via [fsspec](https://filesystem-spec.readthedocs.io), which means the same code path handles every destination. You point `JIRA_SINK_URI` at a URI and supply any authentication in `JIRA_SINK_OPTIONS` as a JSON dict.

## Output layout

Regardless of destination, files are written under a consistent directory tree:

```
{sink_uri}/
  issues/issues_{date}.parquet
  projects/projects_{date}.parquet
  releases/releases_{date}.parquet
  boards/boards_{date}.parquet
  transitions/transitions_{date}.parquet
```

`{date}` defaults to today (`YYYYMMDD`). Override with `--date-suffix`:

```bash
jira-ingest run --date-suffix 20240601
```

## Output formats

Set `JIRA_OUTPUT_FORMAT` to one of:

| Value | Description |
|---|---|
| `parquet` | Snappy-compressed Parquet (default; best for analytics workloads) |
| `csv` | Comma-separated; appends to existing file with header on first write |
| `jsonl` | Newline-delimited JSON; appends to existing file |

---

## Local filesystem

```dotenv
JIRA_SINK_URI=./output
```

Or an absolute path:

```dotenv
JIRA_SINK_URI=/data/jira-exports
```

No credentials needed.

---

## Amazon S3

Install the extra driver if you have not already:

```bash
pip install s3fs
```

**IAM role (recommended for EC2 / ECS / Lambda):**

```dotenv
JIRA_SINK_URI=s3://my-bucket/jira-ingest
# No JIRA_SINK_OPTIONS needed; boto3 picks up the instance role automatically.
```

**Explicit credentials:**

```dotenv
JIRA_SINK_URI=s3://my-bucket/jira-ingest
JIRA_SINK_OPTIONS={"key": "AKIA...", "secret": "wJalrXUt..."}
```

**Named profile:**

```dotenv
JIRA_SINK_URI=s3://my-bucket/jira-ingest
JIRA_SINK_OPTIONS={"profile": "my-aws-profile"}
```

**Custom endpoint (MinIO, LocalStack, etc.):**

```dotenv
JIRA_SINK_URI=s3://my-bucket/jira-ingest
JIRA_SINK_OPTIONS={"endpoint_url": "http://localhost:9000", "key": "minioadmin", "secret": "minioadmin"}
```

---

## Azure Blob Storage

Install the extra driver:

```bash
pip install adlfs
```

**Connection string:**

```dotenv
JIRA_SINK_URI=az://my-container/jira-ingest
JIRA_SINK_OPTIONS={"connection_string": "DefaultEndpointsProtocol=https;AccountName=..."}
```

**Account name + key:**

```dotenv
JIRA_SINK_URI=az://my-container/jira-ingest
JIRA_SINK_OPTIONS={"account_name": "mystorageaccount", "account_key": "abc123..."}
```

**Managed identity (no credentials in config):**

```dotenv
JIRA_SINK_URI=az://my-container/jira-ingest
JIRA_SINK_OPTIONS={"account_name": "mystorageaccount", "anon": false}
```

`abfs://` is also accepted as an alias for `az://`.

---

## Google Cloud Storage

Install the extra driver:

```bash
pip install gcsfs
```

**Application Default Credentials (recommended for GCE / Cloud Run):**

```dotenv
JIRA_SINK_URI=gs://my-bucket/jira-ingest
# No JIRA_SINK_OPTIONS needed; gcsfs picks up ADC automatically.
```

**Service account JSON key:**

```dotenv
JIRA_SINK_URI=gs://my-bucket/jira-ingest
JIRA_SINK_OPTIONS={"token": "/path/to/service-account.json"}
```

---

## Passing sink options on the command line

`JIRA_SINK_OPTIONS` is a JSON string. In a shell you can inline it:

```bash
JIRA_SINK_OPTIONS='{"key":"AKIA...","secret":"wJalrXUt..."}' jira-ingest run
```

Or use an env file:

```bash
jira-ingest run --env-file /etc/jira-ingest/prod.env
```
