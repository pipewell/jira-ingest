"""Click CLI for jira-ingest."""

from __future__ import annotations

import asyncio
import logging
import sys
from datetime import datetime

import click

from jira_ingest.client import create_client
from jira_ingest.config import RedshiftSettings, Settings
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
    "--load-redshift",
    is_flag=True,
    default=False,
    help="Load output into Redshift after writing (requires S3 sink and Redshift config)",
)
def run(
    env_file: str,
    start_date: str | None,
    end_date: str | None,
    date_suffix: str | None,
    load_redshift: bool,
) -> None:
    """Fetch all Jira data and write to the configured sink."""
    settings = Settings(_env_file=env_file)
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

    async def _run() -> dict[str, int]:
        counts: dict[str, int] = {}
        async with create_client(settings) as client:
            async for data_type, records in stream_all(
                client,
                settings,
                start_date=start_date,
                end_date=end_date,
            ):
                writer.write(data_type, records, sink, suffix)
                counts[data_type] = counts.get(data_type, 0) + len(records)
        return counts

    counts = asyncio.run(_run())

    for data_type, n in sorted(counts.items()):
        click.echo(f"  {data_type}: {n:,} records")

    if load_redshift:
        if not settings.sink_uri.startswith("s3://"):
            click.echo("--load-redshift requires an S3 sink URI (s3://...)", err=True)
            sys.exit(1)
        _run_redshift_load(settings, suffix)


def _run_redshift_load(settings: Settings, date_suffix: str) -> None:
    from jira_ingest.loader.redshift import RedshiftLoader

    rs = RedshiftSettings()
    if not rs.is_configured():
        click.echo(
            "Redshift not configured. Set REDSHIFT_HOST, DATABASE, USER, PASSWORD.", err=True
        )
        sys.exit(1)

    loader = RedshiftLoader(rs)
    counts = loader.load_all(settings.sink_uri, date_suffix)
    for table, n in counts.items():
        click.echo(f"  Redshift {table}: {n:,} rows")


@cli.command()
@click.option("--env-file", default=".env", show_default=True)
def validate(env_file: str) -> None:
    """Validate config and connectivity without ingesting data."""
    try:
        settings = Settings(_env_file=env_file)
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
