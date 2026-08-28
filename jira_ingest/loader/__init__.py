"""Loader factory.

Usage::

    from jira_ingest.loader import create_loader

    # PostgreSQL
    loader = create_loader("postgresql+psycopg2://user:pass@host/db", schema="bronze")

    # Redshift (in-memory insert path -- same as PostgreSQL)
    loader = create_loader("postgresql+psycopg2://user:pass@cluster:5439/db")

    # Redshift with S3 COPY fast path -- Redshift speaks the PostgreSQL wire
    # protocol, so the URL scheme stays "postgresql+psycopg2"; passing an
    # iam_role is what selects RedshiftLoader.
    loader = create_loader(
        "postgresql+psycopg2://user:pass@cluster:5439/db",
        iam_role="arn:aws:iam::123:role/MyRole",
        schema="bronze",
    )
    loader.load_from_s3("issues", "s3://my-bucket/jira/issues/issues_20240601.parquet")

    # DuckDB (local analytics / testing)
    loader = create_loader("duckdb:///jira.db")

    # SQLite (unit tests, zero config)
    loader = create_loader("sqlite:///:memory:")

    # Snowflake
    loader = create_loader("snowflake://user:pass@account/db/schema")
"""

from jira_ingest.loader.base import BaseLoader
from jira_ingest.loader.redshift import RedshiftLoader
from jira_ingest.loader.sqlalchemy_loader import SQLAlchemyLoader

__all__ = ["BaseLoader", "RedshiftLoader", "SQLAlchemyLoader", "create_loader"]

_REDSHIFT_SCHEMES = frozenset({"redshift", "redshift+psycopg2", "redshift+redshift_connector"})


def create_loader(
    database_url: str,
    schema: str | None = None,
    iam_role: str = "",
    **kwargs: object,
) -> BaseLoader:
    """Return the appropriate loader for the given ``database_url``.

    ``RedshiftLoader`` is returned when either:

    - ``iam_role`` is supplied. The S3 COPY fast path only makes sense for
      Redshift, so passing an IAM role is treated as an explicit request for
      it. The URL itself can (and normally should) use the plain
      ``postgresql+psycopg2`` scheme -- Redshift speaks the PostgreSQL wire
      protocol, and ``RedshiftLoader`` never relies on a Redshift-specific
      SQLAlchemy dialect (its COPY path issues raw SQL via ``text()``).
    - the URL scheme is one of the ``redshift`` dialect schemes (``redshift``,
      ``redshift+psycopg2``, ``redshift+redshift_connector``) registered by
      the optional ``sqlalchemy-redshift`` package (install the ``redshift``
      extra to get it).

    Everything else returns a ``SQLAlchemyLoader``.

    Args:
        database_url: SQLAlchemy connection URL.
        schema: Target schema name.
        iam_role: Redshift IAM role ARN. Supplying this selects
            ``RedshiftLoader`` regardless of URL scheme.
        **kwargs: Forwarded to the loader constructor (e.g. ``batch_size``, ``echo``).
    """
    scheme = database_url.split("://")[0].lower()
    if iam_role or scheme in _REDSHIFT_SCHEMES:
        return RedshiftLoader(database_url, iam_role=iam_role, schema=schema, **kwargs)  # type: ignore[arg-type]
    return SQLAlchemyLoader(database_url, schema=schema, **kwargs)  # type: ignore[arg-type]
