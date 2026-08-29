"""SQLAlchemy Core table definitions for all Jira data types.

All tables use a schema prefix so they can be placed in any named schema
(e.g. ``bronze``, ``raw``, ``public``).  Pass ``schema=None`` to use the
database's default schema.
"""

from __future__ import annotations

from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    DateTime,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    UniqueConstraint,
)

# Logical name -> physical table name
TABLE_NAMES: dict[str, str] = {
    "projects": "jira_projects",
    "releases": "jira_releases",
    "boards": "jira_boards",
    "issues": "jira_issues",
    "transitions": "jira_transitions",
}


def build_metadata(schema: str | None = None, redshift: bool = False) -> MetaData:
    """Return a MetaData object containing all Jira tables bound to ``schema``.

    Args:
        schema: Target schema name.
        redshift: Redshift has no ``SERIAL`` pseudo-type and no native JSON
            column type. When ``True``, integer primary keys are declared
            plain (every record already carries its own ``id`` from Jira, so
            no dialect ever relies on DB-generated ids) and
            ``jira_issues.custom_fields`` is declared as ``TEXT`` instead of
            ``JSON`` (see ``RedshiftLoader._prepare_batch`` for the matching
            JSON-encode-on-insert step).
    """
    meta = MetaData(schema=schema)
    custom_fields_type = Text if redshift else JSON

    Table(
        "jira_projects",
        meta,
        Column("id", Integer, primary_key=True, autoincrement=False),
        Column("key", String(32), nullable=False),
        Column("name", String(256)),
        Column("project_type_key", String(64)),
        Column("archived", Boolean, default=False),
        Column("project_category_id", String(32)),
        Column("project_category_name", String(256)),
        Column("project_category_description", String(1024)),
        Column("lead_hash", String(64)),
    )

    Table(
        "jira_releases",
        meta,
        Column("release_id", Integer, primary_key=True, autoincrement=False),
        Column("project_id", Integer, nullable=False),
        Column("project_key", String(32)),
        Column("release_name", String(256)),
        Column("archived", Boolean, default=False),
        Column("released", Boolean, default=False),
        Column("release_date", DateTime(timezone=True)),
        Column("user_release_date", DateTime(timezone=True)),
    )

    Table(
        "jira_boards",
        meta,
        Column("board_id", Integer, primary_key=True, autoincrement=False),
        Column("project_id", Integer, nullable=False),
        Column("name", String(256)),
        Column("board_type", String(32)),
    )

    Table(
        "jira_issues",
        meta,
        Column("id", Integer, primary_key=True, autoincrement=False),
        Column("key", String(32), nullable=False),
        Column("project_id", Integer),
        Column("project_key", String(32)),
        Column("project_name", String(256)),
        Column("issuetype_id", Integer),
        Column("issue_type", String(128)),
        Column("parent_task_id", Integer),
        Column("parent_task_key", String(32)),
        Column("priority", String(64)),
        Column("priority_id", Integer),
        Column("status_id", Integer),
        Column("status_name", String(128)),
        Column("status_category_id", Integer),
        Column("status_category_name", String(128)),
        Column("status_category_colour", String(64)),
        Column("epic_id", Integer),
        Column("epic_key", String(32)),
        Column("epic_name", String(256)),
        Column("epic_summary", String(1024)),
        Column("epic_done", Boolean),
        Column("summary", String(1024)),
        Column("labels", String(1024)),
        Column("created", DateTime(timezone=True)),
        Column("updated", DateTime(timezone=True)),
        Column("custom_fields", custom_fields_type),
    )

    Table(
        "jira_transitions",
        meta,
        Column("transition_id", Integer, nullable=False),
        Column("transition_field", String(128), nullable=False),
        Column("transition_field_type", String(64)),
        Column("issue_id", Integer, nullable=False),
        Column("issue_key", String(32)),
        Column("hashed_author", String(64)),
        Column("created", DateTime(timezone=True)),
        Column("from_id", String(256)),
        Column("from_string", String(1024)),
        Column("to_id", String(256)),
        Column("to_string", String(1024)),
        UniqueConstraint("transition_id", "issue_id", "transition_field", name="uq_transition"),
    )

    return meta
