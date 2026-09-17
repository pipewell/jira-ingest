# Output sinks

jira-ingest writes files via [fsspec](https://filesystem-spec.readthedocs.io), which means the same code path handles every destination. You point `JIRA_SINK_URI` at a URI and supply any authentication in `JIRA_SINK_OPTIONS` as a JSON dict.

## Output layout

Regardless of destination, each data type is written as a **directory of
part files**, not one single file -- the same pattern Spark/Hive use for
exactly the same reason: every part is independently complete the moment
it's written, so a crash partway through a run leaves everything already
written valid and readable, rather than one held-open file that's unreadable
garbage if the process dies before it's finished:

```
{sink_uri}/
  issues/issues_{date}/part-<uuid>.parquet
             ...more parts as the run produces more batches...
  projects/projects_{date}/part-<uuid>.parquet
  releases/releases_{date}/part-<uuid>.csv
  boards/boards_{date}/part-<uuid>.jsonl
  transitions/transitions_{date}/part-<uuid>.parquet
```

Reading a data type back means reading (or globbing) every part under its
directory -- `pandas.read_parquet()`, `pyarrow.dataset`, and Redshift's
`COPY ... FROM 's3://.../issues_{date}/'` all do this natively; no manual
concatenation needed.

`{date}` defaults to today (`YYYYMMDD`). Override with `--date-suffix`:

```bash
jira-ingest run --date-suffix 20240601
```

By default, running `jira-ingest run` again with the same `--date-suffix`
**replaces** that date's output: existing parts for every enabled data type
are deleted before any new ones are written, so a completed run's output is
exactly and only that run -- not a union of several same-day attempts. Pass
`--append` to skip that and let a new run's parts land alongside whatever's
already there, if you deliberately want to split one day's ingest across
multiple runs that should fold together:

```bash
jira-ingest run --append
```

Accepted tradeoff with the default (replace) behaviour: if a run crashes
partway through, you're left with only the new run's incomplete parts --
the previous, complete run's data for that date is already gone by the time
new parts start landing. This mirrors how an "overwrite" partition write
behaves in Spark, and is still a net improvement over having no
crash-resilience within a run at all.

Each data type's part-file size is capped by `JIRA_PART_FILE_MAX_RECORDS`
(default `10000`, must be a positive integer): records are buffered per
data type as they're fetched and flushed to one or more parts of at most
this many rows each, plus a final flush of whatever's left when the run
finishes. Lower it for smaller, more numerous parts (finer-grained crash
recovery, more per-file overhead) or raise it for fewer, larger parts.

## Output formats

Set `JIRA_OUTPUT_FORMAT` to one of:

| Value | Description |
|---|---|
| `parquet` | Snappy-compressed Parquet (default; best for analytics workloads) |
| `csv` | Comma-separated; each part file has its own header row |
| `jsonl` | Newline-delimited JSON |

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
