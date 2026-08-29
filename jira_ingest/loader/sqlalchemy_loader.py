"""Generic SQLAlchemy Core loader.

Works with any database that has a SQLAlchemy dialect:
  - PostgreSQL / CockroachDB  (psycopg2 or asyncpg)
  - Redshift                  (redshift_connector or psycopg2)
  - Snowflake                 (snowflake-sqlalchemy)
  - BigQuery                  (sqlalchemy-bigquery)
  - DuckDB                    (duckdb-engine)
  - SQLite                    (built-in; useful for testing and local dev)

The ``database_url`` argument is a standard SQLAlchemy connection URL, e.g.:
    ``postgresql+psycopg2://user:pass@host:5432/db``
    ``snowflake://user:pass@account/db/schema``
    ``duckdb:///path/to/file.db``
    ``sqlite:///:memory:``

Bulk-insert strategy by dialect:
  - PostgreSQL:      INSERT ... ON CONFLICT DO NOTHING  (idempotent)
  - SQLite:           INSERT OR IGNORE                   (idempotent)
  - Everything else:  plain INSERT  (caller is responsible for idempotency)

Redshift reports a ``postgresql`` dialect name (see ``RedshiftLoader``) but
does not support ``ON CONFLICT``; ``RedshiftLoader`` overrides ``_pg_upsert``
to use a plain INSERT instead.
"""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from jira_ingest.loader.base import BaseLoader
from jira_ingest.loader.tables import TABLE_NAMES, build_metadata
from jira_ingest.utils import batched

logger = logging.getLogger(__name__)

# Dialects that support ON CONFLICT DO NOTHING
_PG_FAMILY = frozenset({"postgresql", "redshift"})


class SQLAlchemyLoader(BaseLoader):
    """Dialect-agnostic bulk loader built on SQLAlchemy Core.

    Args:
        database_url: SQLAlchemy connection URL.
        schema: Target schema name (``None`` uses the database default).
        batch_size: Number of rows to insert per statement.
        echo: Forward SQL to the Python logger (useful for debugging).
    """

    def __init__(
        self,
        database_url: str,
        schema: str | None = None,
        batch_size: int = 500,
        echo: bool = False,
        redshift: bool = False,
    ) -> None:
        self._engine: Engine = create_engine(database_url, echo=echo)
        self._schema = schema
        self._batch_size = batch_size
        self._metadata = build_metadata(schema, redshift=redshift)

    @property
    def engine(self) -> Engine:
        return self._engine

    # ── Public interface ──────────────────────────────────────────────────────

    def ensure_tables(self) -> None:
        """Create all Jira tables if they do not already exist."""
        self._metadata.create_all(self._engine, checkfirst=True)
        logger.info("Tables ready in schema %r", self._schema)

    def load(self, data_type: str, records: list[dict[str, Any]]) -> int:
        if not records:
            return 0

        table_name = TABLE_NAMES.get(data_type)
        if not table_name:
            raise ValueError(f"Unknown data type: {data_type!r}. Known: {list(TABLE_NAMES)}")

        qualified = f"{self._schema}.{table_name}" if self._schema else table_name
        table = self._metadata.tables[qualified]
        dialect = self._engine.dialect.name

        total = 0
        with self._engine.begin() as conn:
            for batch in batched(records, self._batch_size):
                cleaned = self._prepare_batch(_normalise_batch(batch))
                if dialect in _PG_FAMILY:
                    total += self._pg_upsert(conn, table, cleaned)
                elif dialect == "sqlite":
                    total += self._sqlite_insert(conn, table, cleaned)
                else:
                    conn.execute(table.insert(), cleaned)
                    total += len(cleaned)

        logger.info("Loaded %d rows into %s", total, table_name)
        return total

    # ── Dialect-specific strategies ───────────────────────────────────────────

    def _prepare_batch(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Hook for subclasses to adjust a batch just before it's inserted.

        No-op by default; ``RedshiftLoader`` overrides this to JSON-encode
        ``custom_fields`` since Redshift stores it as ``TEXT``, not JSON.
        """
        return records

    def _pg_upsert(self, conn: Any, table: Any, records: list[dict[str, Any]]) -> int:
        from sqlalchemy.dialects.postgresql import insert as pg_insert

        stmt = pg_insert(table).values(records).on_conflict_do_nothing()
        result = conn.execute(stmt)
        return result.rowcount if result.rowcount >= 0 else len(records)

    def _sqlite_insert(self, conn: Any, table: Any, records: list[dict[str, Any]]) -> int:
        from sqlalchemy.dialects.sqlite import insert as sqlite_insert

        stmt = sqlite_insert(table).values(records).on_conflict_do_nothing()
        result = conn.execute(stmt)
        return result.rowcount if result.rowcount >= 0 else len(records)


# ── Helpers ───────────────────────────────────────────────────────────────────


def _normalise_batch(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Ensure every record in a batch has the same key set.

    SQLAlchemy multi-row INSERT requires all rows to carry identical columns.
    Missing keys are filled with None so the generated VALUES clause is uniform.
    """
    if len(records) == 1:
        return records
    all_keys: set[str] = set().union(*(r.keys() for r in records))
    return [{k: r.get(k) for k in all_keys} for r in records]
