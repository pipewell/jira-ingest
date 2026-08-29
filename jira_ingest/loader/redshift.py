"""Redshift loader with S3 COPY fast path.

Extends ``SQLAlchemyLoader`` with ``load_from_s3``, which issues a Redshift
COPY command directly from an S3 prefix.  Use this when the pipeline has
already written Parquet files to S3 and you want to avoid re-streaming
records through Python.

For row-by-row loads (e.g. small datasets or non-S3 sinks) just call the
inherited ``load(data_type, records)`` method instead. Redshift's wire
protocol reports a ``postgresql`` SQLAlchemy dialect name (since connections
normally use ``postgresql+psycopg2://``), but Redshift's INSERT grammar has
no ``ON CONFLICT`` clause, so the base class's Postgres upsert path is
overridden below to fall back to a plain INSERT for this dialect.

Redshift also lacks the ``SERIAL`` pseudo-type and any native JSON column
type, both of which the generic table metadata otherwise uses when compiled
against the ``postgresql`` dialect. ``build_metadata(redshift=True)`` (used
below) declares plain integer primary keys and a ``SUPER`` column for
``custom_fields`` instead. ``SUPER`` (rather than ``TEXT``) is required
because the S3 COPY path below loads Parquet's nested struct columns via
``COPY ... SERIALIZETOJSON``, which only targets ``SUPER`` -- Redshift COPY
requires the destination type to match what the source produces for nested
data. ``_prepare_batch`` JSON-encodes ``custom_fields`` to a string for the
row-wise path, and ``_pg_upsert`` wraps it in ``JSON_PARSE()`` so Redshift
stores it as parsed ``SUPER``, not a scalar string.

Finally, SQLAlchemy's plain ``postgresql`` dialect probes
``SHOW standard_conforming_strings`` the first time it connects (real
PostgreSQL has supported this GUC since 8.1). Redshift doesn't recognize it
and raises ``UndefinedObject: unrecognized configuration parameter``,
breaking every connection before any query of ours runs. ``__init__``
patches ``_set_backslash_escapes`` on this instance's engine dialect only
(not the ``PGDialect`` class), confirmed against a real Redshift Serverless
workgroup.
"""

from __future__ import annotations

import json
import logging
import types
from typing import Any

import fsspec
from sqlalchemy import bindparam, func, text

from jira_ingest.loader.sqlalchemy_loader import SQLAlchemyLoader
from jira_ingest.loader.tables import TABLE_NAMES

logger = logging.getLogger(__name__)


def _no_backslash_escapes(dialect: Any, connection: Any) -> None:
    dialect._backslash_escapes = False


class RedshiftLoader(SQLAlchemyLoader):
    """SQLAlchemy loader with an additional S3 COPY fast path for Redshift.

    Args:
        database_url: Redshift connection URL, e.g.
            ``postgresql+psycopg2://user:pass@cluster.redshift.amazonaws.com:5439/db``
        iam_role: ARN of the IAM role that has S3 read access, e.g.
            ``arn:aws:iam::123456789012:role/RedshiftS3ReadRole``.
        schema: Target schema (default ``"public"``).
        batch_size: Rows per INSERT batch for the in-memory path.
    """

    def __init__(
        self,
        database_url: str,
        iam_role: str = "",
        schema: str | None = "public",
        batch_size: int = 500,
        echo: bool = False,
    ) -> None:
        super().__init__(
            database_url, schema=schema, batch_size=batch_size, echo=echo, redshift=True
        )
        self._iam_role = iam_role
        if self._engine.dialect.name == "postgresql":
            self._engine.dialect._set_backslash_escapes = types.MethodType(
                _no_backslash_escapes, self._engine.dialect
            )

    def _prepare_batch(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """JSON-encode ``custom_fields`` to a string; ``_pg_upsert`` parses it
        back into ``SUPER`` via ``JSON_PARSE()`` on insert."""
        result = []
        for record in records:
            value = record.get("custom_fields")
            if isinstance(value, dict | list):
                record = {**record, "custom_fields": json.dumps(value)}
            result.append(record)
        return result

    def _pg_upsert(self, conn: Any, table: Any, records: list[dict[str, Any]]) -> int:
        """Plain INSERT: Redshift has no ``ON CONFLICT`` support.

        Unlike the base class's Postgres path, this does not deduplicate.
        Callers loading into Redshift without an S3 sink are responsible for
        their own idempotency (e.g. truncate-and-reload); use
        ``load_from_s3`` for large loads instead.

        ``custom_fields`` (when present) is wrapped in ``JSON_PARSE()`` so
        the JSON string from ``_prepare_batch`` lands as parsed ``SUPER``
        data rather than a quoted scalar string.
        """
        if "custom_fields" in records[0]:
            values = {
                name: func.json_parse(bindparam("custom_fields"))
                if name == "custom_fields"
                else bindparam(name)
                for name in records[0]
            }
            conn.execute(table.insert().values(values), records)
        else:
            conn.execute(table.insert(), records)
        return len(records)

    def load_from_s3(self, data_type: str, s3_prefix: str) -> None:
        """COPY all Parquet files under ``s3_prefix`` into the target table.

        This is substantially faster than the in-memory path for large datasets
        because the data never passes through Python. ``SERIALIZETOJSON``
        converts Parquet's nested struct columns (e.g. ``jira_issues.custom_fields``,
        written as a struct by ``ParquetWriter``) into the target ``SUPER``
        column; without it, COPY rejects a nested source column with no
        matching non-``SUPER`` destination type.
        """
        table_name = TABLE_NAMES.get(data_type)
        if not table_name:
            raise ValueError(f"Unknown data type: {data_type!r}")

        schema_prefix = f"{self._schema}." if self._schema else ""
        qualified = f"{schema_prefix}{table_name}"

        sql = text(f"""
            COPY {qualified}
            FROM :s3_prefix
            IAM_ROLE :iam_role
            FORMAT AS PARQUET
            SERIALIZETOJSON
            COMPUPDATE OFF
            STATUPDATE OFF
        """)

        with self._engine.begin() as conn:
            logger.info("COPY %s <- %s", qualified, s3_prefix)
            conn.execute(sql, {"s3_prefix": s3_prefix, "iam_role": self._iam_role})

    def load_all_from_s3(self, s3_sink_uri: str, date_suffix: str) -> None:
        """COPY all data types from an S3 sink for a given date suffix.

        Data types that produced zero records never get a Parquet file
        written by ``ParquetWriter``, so those are skipped here rather than
        issuing a COPY against a nonexistent key (which would fail and abort
        the whole load).
        """
        for data_type in TABLE_NAMES:
            prefix = f"{s3_sink_uri.rstrip('/')}/{data_type}/{data_type}_{date_suffix}.parquet"
            fs, _ = fsspec.core.url_to_fs(prefix)
            if not fs.exists(prefix):
                logger.info("Skipping COPY for %s: no file at %s", data_type, prefix)
                continue
            self.load_from_s3(data_type, prefix)
