# Database loading

After writing files to a sink, jira-ingest can load records directly into a relational database. The loader is built on [SQLAlchemy Core](https://docs.sqlalchemy.org/en/20/core/) and supports any database that has a SQLAlchemy dialect.

## How it works

The `--database-url` flag activates the loader. After all files have been written:

1. `ensure_tables()` runs `CREATE TABLE IF NOT EXISTS` for all five Jira tables.
2. Records are bulk-inserted in batches of 500 (configurable).
3. Duplicate rows are silently skipped via `ON CONFLICT DO NOTHING` (PostgreSQL) or `INSERT OR IGNORE` (SQLite). Redshift has no such clause -- see [Redshift](#redshift) below.

For Redshift with an S3 sink the pipeline automatically switches to the native `COPY ... FROM S3` path, which is far faster than row-by-row insertion for large datasets.

---

## Tables created

| Table | Primary key | Notes |
|---|---|---|
| `jira_projects` | `id` | |
| `jira_releases` | `release_id` | |
| `jira_boards` | `board_id` | |
| `jira_issues` | `id` | `custom_fields` stored as JSON/JSONB (Redshift: [`SUPER`](https://docs.aws.amazon.com/redshift/latest/dg/c_Supported_data_types.html), since Redshift has no JSON type) |
| `jira_transitions` | `(transition_id, issue_id, transition_field)` | Composite unique constraint |

The schema prefix (`--db-schema`) is applied to all table names.

---

## PostgreSQL

Install the driver:

```bash
pip install "jira-ingest[database]" psycopg2-binary
```

Run:

```bash
jira-ingest run \
  --database-url "postgresql+psycopg2://user:pass@localhost:5432/mydb" \
  --db-schema bronze
```

Or via environment variable:

```dotenv
DATABASE_URL=postgresql+psycopg2://user:pass@localhost:5432/mydb
DATABASE_SCHEMA=bronze
```

```bash
jira-ingest run
```

---

## Redshift

Redshift exposes a PostgreSQL-compatible wire protocol, so the standard psycopg2 driver works and the URL keeps the `postgresql+psycopg2` scheme -- there is no need for a Redshift-specific SQLAlchemy dialect. **Always pass `--redshift-iam-role` (or `REDSHIFT_IAM_ROLE`) when the target is Redshift**, even if you don't have an S3 sink configured: this is what tells jira-ingest to treat the target as Redshift and use INSERT statements Redshift actually supports (Redshift's SQL dialect has no `ON CONFLICT` clause, unlike PostgreSQL). Without it, jira-ingest can't distinguish the target from a real PostgreSQL database and will emit `ON CONFLICT`, which Redshift will reject.

**In-memory insert path (no S3 sink):**

```bash
pip install "jira-ingest[database]" psycopg2-binary

jira-ingest run \
  --database-url "postgresql+psycopg2://user:pass@cluster.us-east-1.redshift.amazonaws.com:5439/dev" \
  --db-schema bronze \
  --redshift-iam-role "arn:aws:iam::123456789012:role/RedshiftS3ReadRole"
```

This path does row-by-row INSERTs with no deduplication (Redshift has no upsert clause to fall back to) -- re-running against the same data will duplicate rows. Use it only for small, one-off loads; prefer the S3 COPY path below for anything you'll run repeatedly.

**S3 COPY fast path (S3 sink + IAM role):**

The sink is configured via the `JIRA_SINK_URI` environment variable (or `.env` file), not a CLI flag:

```dotenv
JIRA_SINK_URI=s3://my-bucket/jira-ingest
```

```bash
jira-ingest run \
  --database-url "postgresql+psycopg2://user:pass@cluster.us-east-1.redshift.amazonaws.com:5439/dev" \
  --db-schema bronze \
  --redshift-iam-role "arn:aws:iam::123456789012:role/RedshiftS3ReadRole"
```

When `--redshift-iam-role` is supplied and `JIRA_SINK_URI` starts with `s3://`, the pipeline automatically issues:

```sql
COPY bronze.jira_issues
FROM 's3://my-bucket/jira-ingest/issues/issues_20240601.parquet'
IAM_ROLE 'arn:aws:iam::123456789012:role/RedshiftS3ReadRole'
FORMAT AS PARQUET
SERIALIZETOJSON
COMPUPDATE OFF
STATUPDATE OFF;
```

`COMPUPDATE OFF STATUPDATE OFF` prevents the post-load compression analysis that Redshift runs on new tables by default -- without these flags a large initial load can take hours. `SERIALIZETOJSON` converts Parquet's nested struct columns (`custom_fields`) into the target `SUPER` column -- see AWS's guidance on [COPY from Parquet/ORC into SUPER](https://docs.aws.amazon.com/redshift/latest/dg/copy_json.html).

---

## Snowflake

```bash
pip install "jira-ingest[database]" snowflake-sqlalchemy
```

```bash
jira-ingest run \
  --database-url "snowflake://user:pass@myaccount/mydb/myschema" \
  --db-schema raw
```

Snowflake does not support `ON CONFLICT DO NOTHING`, so duplicate rows are inserted as-is. Run with `--date-suffix` to control partitioning and avoid re-inserting data already loaded.

---

## DuckDB

Useful for local analytics and testing pipelines without a server.

```bash
pip install "jira-ingest[database]" duckdb-engine
```

```bash
# Write to a file on disk
jira-ingest run --database-url "duckdb:///jira.db"

# In-memory (lost after process exits)
jira-ingest run --database-url "duckdb:///:memory:"
```

---

## SQLite

Zero-configuration option for development.

```bash
# SQLite is built into Python -- no driver to install
jira-ingest run --database-url "sqlite:///jira.db"
```

---

## Using the loader programmatically

```python
from jira_ingest.loader import create_loader

loader = create_loader("postgresql+psycopg2://user:pass@localhost/mydb", schema="bronze")
loader.ensure_tables()

records = [
    {"id": 1, "key": "PROJ-1", "summary": "Fix the bug", "issue_type": "Bug"},
]
n = loader.load("issues", records)
print(f"Loaded {n} rows")
```

The `create_loader` factory picks `RedshiftLoader` when you pass a non-empty `iam_role`, or when the URL scheme is one of the `redshift+*` dialects registered by the optional `sqlalchemy-redshift` package (install the `redshift` extra for that); everything else returns `SQLAlchemyLoader`. Both implement the same `BaseLoader` interface, so you can swap databases without changing calling code.

---

## Installing dialect drivers

Only SQLAlchemy itself is included in the `[database]` extra. Install the dialect driver separately:

| Database | Driver package |
|---|---|
| PostgreSQL / Redshift | `psycopg2-binary` or `psycopg[binary]` |
| Snowflake | `snowflake-sqlalchemy` |
| BigQuery | `sqlalchemy-bigquery` |
| DuckDB | `duckdb-engine` |
| SQLite | built into Python |
