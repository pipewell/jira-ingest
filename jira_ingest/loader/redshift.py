"""Load Parquet files from S3 into Redshift via COPY command.

This loader is optional. Requires: pip install jira-ingest[redshift]
"""

from __future__ import annotations

import logging

from jira_ingest.config import RedshiftSettings

logger = logging.getLogger(__name__)

_TABLES: dict[str, str] = {
    "projects": "jira_projects",
    "releases": "jira_releases",
    "boards": "jira_boards",
    "issues": "jira_issues",
    "transitions": "jira_transitions",
}


class RedshiftLoader:
    def __init__(self, settings: RedshiftSettings) -> None:
        self._settings = settings

    def _connect(self):  # type: ignore[no-untyped-def]
        try:
            import redshift_connector
        except ImportError as exc:
            raise ImportError(
                "redshift-connector is required. Install with: pip install jira-ingest[redshift]"
            ) from exc
        s = self._settings
        return redshift_connector.connect(
            host=s.host,
            port=s.port,
            database=s.database,
            user=s.user,
            password=s.password,
        )

    def load(self, data_type: str, s3_prefix: str) -> int:
        """COPY all Parquet files under ``s3_prefix`` into the target table.

        Returns the number of rows loaded.
        """
        s = self._settings
        table = f"{s.schema_name}.{_TABLES.get(data_type, data_type)}"
        iam_role = s.iam_role or ""

        copy_sql = f"""
            COPY {table}
            FROM '{s3_prefix}'
            IAM_ROLE '{iam_role}'
            FORMAT AS PARQUET
            COMPUPDATE OFF
            STATUPDATE OFF;
        """
        count_sql = f"SELECT COUNT(1) FROM {table};"

        with self._connect() as conn, conn.cursor() as cur:
            logger.info("COPY into %s from %s", table, s3_prefix)
            cur.execute(copy_sql)
            cur.execute(count_sql)
            result = cur.fetchone()
            row_count: int = result[0] if result else 0
            conn.commit()

        logger.info("Loaded %d rows into %s", row_count, table)
        return row_count

    def load_all(self, s3_sink_uri: str, date_suffix: str) -> dict[str, int]:
        """Load all data types from ``s3_sink_uri`` for a given date suffix."""
        counts: dict[str, int] = {}
        for data_type in _TABLES:
            prefix = f"{s3_sink_uri.rstrip('/')}/{data_type}/{data_type}_{date_suffix}.parquet"
            counts[data_type] = self.load(data_type, prefix)
        return counts
