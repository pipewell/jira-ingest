"""Click CLI for jira-ingest."""

from __future__ import annotations

import asyncio
import logging
import sys
from datetime import datetime

import click

from jira_ingest.client import create_client
from jira_ingest.config import Settings
from jira_ingest.output.sink import Sink
from jira_ingest.output.writer import create_writer
from jira_ingest.processor import stream_all
from jira_ingest.utils import configure_logging

logger = logging.getLogger(__name__)


@click.group()
def cli() -> None:
    """jira-ingest: async Jira data pipeline for DC and Cloud."""


@cli.command()
@click.option("--env-file", default=".env", help="Path to .env file", show_default=True)
@click.option("--start-date", default=None, help="Filter issues from date (YYYY-MM-DD)")
@click.option("--end-date", default=None, help="Filter issues until date (YYYY-MM-DD)")
@click.option("--date-suffix", default=None, help="Output file date suffix (default: today)")
@click.option(
    "--database-url",
    default=None,
    help="SQLAlchemy URL to load records into a database after writing files",
    envvar="DATABASE_URL",
)
@click.option(
    "--db-schema",
    default=None,
    help="Target database schema (used with --database-url)",
    envvar="DATABASE_SCHEMA",
)
@click.option(
    "--redshift-iam-role",
    default="",
    help="IAM role ARN for Redshift S3 COPY (used with --database-url on Redshift)",
    envvar="REDSHIFT_IAM_ROLE",
)
def run(
    env_file: str,
    start_date: str | None,
    end_date: str | None,
    date_suffix: str | None,
    database_url: str | None,
    db_schema: str | None,
    redshift_iam_role: str,
) -> None:
    """Fetch all Jira data and write to the configured sink."""
    settings = Settings(_env_file=env_file)  # type: ignore[call-arg]
    configure_logging(settings.log_level)

    suffix = date_suffix or datetime.utcnow().strftime("%Y%m%d")
    sink = Sink(settings.sink_uri, settings.sink_options)
    writer = create_writer(settings.output_format)

    logger.info(
        "Starting jira-ingest | mode=%s | format=%s | sink=%s",
        settings.mode,
        settings.output_format,
        settings.sink_uri,
    )

    # Accumulate all records in memory: the file writers need every record for a
    # data type in one call (Parquet has no cheap append), and the database
    # loader below needs the full set too.
    all_records: dict[str, list[dict[str, object]]] = {}

    async def _run() -> dict[str, int]:
        counts: dict[str, int] = {}
        async with create_client(settings) as client:
            async for data_type, records in stream_all(
                client,
                settings,
                start_date=start_date,
                end_date=end_date,
            ):
                counts[data_type] = counts.get(data_type, 0) + len(records)
                all_records.setdefault(data_type, []).extend(records)
        return counts

    counts = asyncio.run(_run())

    for data_type, records in all_records.items():
        writer.write(data_type, records, sink, suffix)

    for data_type, n in sorted(counts.items()):
        click.echo(f"  {data_type}: {n:,} records")

    if database_url:
        _load_database(database_url, db_schema, redshift_iam_role, all_records, settings, suffix)


def _load_database(
    database_url: str,
    schema: str | None,
    iam_role: str,
    records: dict[str, list[dict[str, object]]],
    settings: Settings,
    date_suffix: str,
) -> None:
    from jira_ingest.loader import create_loader
    from jira_ingest.loader.redshift import RedshiftLoader

    loader = create_loader(database_url, schema=schema, iam_role=iam_role)
    loader.ensure_tables()

    # Redshift + S3 sink: use the fast COPY path instead of row-by-row insert
    if isinstance(loader, RedshiftLoader) and settings.sink_uri.startswith("s3://"):
        click.echo("Redshift detected with S3 sink -- using COPY fast path")
        loader.load_all_from_s3(settings.sink_uri, date_suffix)
    else:
        db_counts = loader.load_all(records)
        for table, n in db_counts.items():
            click.echo(f"  DB {table}: {n:,} rows")


@cli.command()
@click.option("--env-file", default=".env", show_default=True)
def validate(env_file: str) -> None:
    """Validate config and connectivity without ingesting data."""
    try:
        settings = Settings(_env_file=env_file)  # type: ignore[call-arg]
    except Exception as exc:
        click.echo(f"Config error: {exc}", err=True)
        sys.exit(1)

    configure_logging("INFO")
    click.echo(f"Mode: {settings.mode}")
    click.echo(f"URL: {settings.url}")
    click.echo(f"Sink: {settings.sink_uri}  (format: {settings.output_format})")
    click.echo(f"Project keys: {settings.project_keys or '(all)'}")
    click.echo(f"Custom fields: {settings.custom_fields or '(none)'}")

    async def _check() -> None:
        async with create_client(settings) as client:
            resp = await client.get("/rest/api/2/serverInfo")
            version = resp.get("version", "?")
            server_title = resp.get("serverTitle", "Jira")
            click.echo(f"Connected to {server_title} v{version}")

    asyncio.run(_check())
