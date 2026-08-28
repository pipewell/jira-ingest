"""Redshift loader with S3 COPY fast path.

Extends ``SQLAlchemyLoader`` with ``load_from_s3``, which issues a Redshift
COPY command directly from an S3 prefix.  Use this when the pipeline has
already written Parquet files to S3 and you want to avoid re-streaming
records through Python.

For row-by-row loads (e.g. small datasets or non-S3 sinks) just call the
inherited ``load(data_type, records)`` method instead -- it uses the standard
PostgreSQL ON CONFLICT DO NOTHING path, which Redshift supports.
"""

from __future__ import annotations

import logging

import fsspec
from sqlalchemy import text

from jira_ingest.loader.sqlalchemy_loader import SQLAlchemyLoader
from jira_ingest.loader.tables import TABLE_NAMES

logger = logging.getLogger(__name__)


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
        super().__init__(database_url, schema=schema, batch_size=batch_size, echo=echo)
        self._iam_role = iam_role

    def load_from_s3(self, data_type: str, s3_prefix: str) -> None:
        """COPY all Parquet files under ``s3_prefix`` into the target table.

        This is substantially faster than the in-memory path for large datasets
        because the data never passes through Python.
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
