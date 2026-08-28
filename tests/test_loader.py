"""Tests for the SQLAlchemy loader layer.

All tests use SQLite in-memory so no external server is required.
The SQLite dialect supports ON CONFLICT DO NOTHING via the sqlite insert variant,
which gives us full end-to-end coverage of the upsert path without spinning up
Postgres or Redshift.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import inspect, text

from jira_ingest.loader import create_loader
from jira_ingest.loader.base import BaseLoader
from jira_ingest.loader.redshift import RedshiftLoader
from jira_ingest.loader.sqlalchemy_loader import SQLAlchemyLoader
from jira_ingest.loader.tables import TABLE_NAMES, build_metadata

SQLITE_URL = "sqlite:///:memory:"


@pytest.fixture()
def loader() -> SQLAlchemyLoader:
    ldr = SQLAlchemyLoader(SQLITE_URL)
    ldr.ensure_tables()
    return ldr


# ── Factory ───────────────────────────────────────────────────────────────────


def test_create_loader_returns_sqlalchemy_for_sqlite() -> None:
    ldr = create_loader(SQLITE_URL)
    assert isinstance(ldr, SQLAlchemyLoader)


def test_create_loader_returns_redshift_for_redshift_scheme() -> None:
    with patch("jira_ingest.loader.sqlalchemy_loader.create_engine", return_value=MagicMock()):
        ldr = create_loader("redshift+psycopg2://user:pass@host:5439/db")
    assert isinstance(ldr, RedshiftLoader)


def test_redshift_loader_is_sqlalchemy_subclass() -> None:
    with patch("jira_ingest.loader.sqlalchemy_loader.create_engine", return_value=MagicMock()):
        ldr = create_loader("redshift+psycopg2://user:pass@host:5439/db")
    assert isinstance(ldr, SQLAlchemyLoader)
    assert isinstance(ldr, BaseLoader)


def test_create_loader_returns_redshift_when_iam_role_given_on_postgres_url() -> None:
    # Redshift speaks the PostgreSQL wire protocol, so the documented and
    # recommended way to target it is a plain postgresql+psycopg2 URL --
    # the redshift+* schemes require the optional sqlalchemy-redshift
    # package. Passing an iam_role is what should select RedshiftLoader.
    with patch("jira_ingest.loader.sqlalchemy_loader.create_engine", return_value=MagicMock()):
        ldr = create_loader(
            "postgresql+psycopg2://user:pass@cluster.us-east-1.redshift.amazonaws.com:5439/dev",
            iam_role="arn:aws:iam::123456789012:role/RedshiftS3ReadRole",
        )
    assert isinstance(ldr, RedshiftLoader)


def test_create_loader_returns_sqlalchemy_for_postgres_url_without_iam_role() -> None:
    with patch("jira_ingest.loader.sqlalchemy_loader.create_engine", return_value=MagicMock()):
        ldr = create_loader("postgresql+psycopg2://user:pass@cluster:5439/dev")
    assert isinstance(ldr, SQLAlchemyLoader)
    assert not isinstance(ldr, RedshiftLoader)


# ── Table creation ────────────────────────────────────────────────────────────


def test_ensure_tables_creates_all_tables(loader: SQLAlchemyLoader) -> None:
    db_tables = inspect(loader.engine).get_table_names()
    for physical_name in TABLE_NAMES.values():
        assert physical_name in db_tables, f"Missing table: {physical_name}"


def test_ensure_tables_is_idempotent(loader: SQLAlchemyLoader) -> None:
    loader.ensure_tables()  # second call must not raise
    db_tables = inspect(loader.engine).get_table_names()
    assert len(db_tables) == len(TABLE_NAMES)


# ── Projects ──────────────────────────────────────────────────────────────────


def test_load_projects(loader: SQLAlchemyLoader) -> None:
    records = [
        {"id": 1, "key": "PROJ", "name": "My Project", "project_type_key": "software"},
        {"id": 2, "key": "OPS", "name": "Ops", "archived": False},
    ]
    n = loader.load("projects", records)
    assert n == 2

    with loader.engine.connect() as conn:
        rows = conn.execute(text("SELECT id, key FROM jira_projects ORDER BY id")).fetchall()
    assert [(r[0], r[1]) for r in rows] == [(1, "PROJ"), (2, "OPS")]


def test_load_projects_idempotent(loader: SQLAlchemyLoader) -> None:
    record = {"id": 1, "key": "PROJ", "name": "My Project"}
    loader.load("projects", [record])
    loader.load("projects", [record])  # duplicate -- must be silently skipped

    with loader.engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM jira_projects")).scalar()
    assert count == 1


def test_load_empty_returns_zero(loader: SQLAlchemyLoader) -> None:
    assert loader.load("projects", []) == 0


# ── Issues ────────────────────────────────────────────────────────────────────


def test_load_issues(loader: SQLAlchemyLoader) -> None:
    records = [
        {
            "id": 100,
            "key": "PROJ-1",
            "project_id": 1,
            "project_key": "PROJ",
            "issue_type": "Story",
            "summary": "Do the thing",
            "custom_fields": {"story_points": "5"},
            "created": datetime(2024, 1, 1, tzinfo=UTC),
        },
    ]
    n = loader.load("issues", records)
    assert n == 1

    with loader.engine.connect() as conn:
        row = conn.execute(text("SELECT key, summary FROM jira_issues")).fetchone()
    assert row is not None
    assert row[0] == "PROJ-1"
    assert row[1] == "Do the thing"


def test_load_issues_custom_fields_stored_as_json(loader: SQLAlchemyLoader) -> None:
    records = [
        {
            "id": 200,
            "key": "PROJ-2",
            "custom_fields": {"sprint": "Sprint 12", "story_points": "3"},
        }
    ]
    loader.load("issues", records)

    with loader.engine.connect() as conn:
        raw = conn.execute(text("SELECT custom_fields FROM jira_issues WHERE id = 200")).scalar()
    # SQLite stores JSON as text; SQLAlchemy deserialises on read
    assert raw is not None


# ── Transitions ───────────────────────────────────────────────────────────────


def test_load_transitions(loader: SQLAlchemyLoader) -> None:
    records = [
        {
            "transition_id": 10,
            "transition_field": "status",
            "issue_id": 100,
            "issue_key": "PROJ-1",
            "from_string": "To Do",
            "to_string": "In Progress",
        },
    ]
    n = loader.load("transitions", records)
    assert n == 1


def test_load_transitions_duplicate_silently_skipped(loader: SQLAlchemyLoader) -> None:
    record: dict[str, Any] = {
        "transition_id": 10,
        "transition_field": "status",
        "issue_id": 100,
    }
    loader.load("transitions", [record])
    loader.load("transitions", [record])  # same unique key

    with loader.engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM jira_transitions")).scalar()
    assert count == 1


# ── Boards and Releases ───────────────────────────────────────────────────────


def test_load_boards(loader: SQLAlchemyLoader) -> None:
    records = [{"board_id": 1, "project_id": 1, "name": "Sprint Board", "board_type": "scrum"}]
    assert loader.load("boards", records) == 1


def test_load_releases(loader: SQLAlchemyLoader) -> None:
    records = [
        {
            "release_id": 1,
            "project_id": 1,
            "project_key": "PROJ",
            "release_name": "v1.0",
            "released": True,
        }
    ]
    assert loader.load("releases", records) == 1


# ── load_all ──────────────────────────────────────────────────────────────────


def test_load_all(loader: SQLAlchemyLoader) -> None:
    batches: dict[str, list[dict[str, Any]]] = {
        "projects": [{"id": 1, "key": "A"}],
        "boards": [{"board_id": 1, "project_id": 1, "name": "B", "board_type": "kanban"}],
    }
    counts = loader.load_all(batches)
    assert counts == {"projects": 1, "boards": 1}


# ── Error handling ────────────────────────────────────────────────────────────


def test_load_unknown_data_type_raises(loader: SQLAlchemyLoader) -> None:
    with pytest.raises(ValueError, match="Unknown data type"):
        loader.load("dragons", [{"id": 1}])


# ── Metadata / tables module ──────────────────────────────────────────────────


def test_build_metadata_no_schema() -> None:
    meta = build_metadata(schema=None)
    # Table names should be unqualified
    for name in meta.tables:
        assert "." not in name


def test_build_metadata_with_schema() -> None:
    meta = build_metadata(schema="bronze")
    for name in meta.tables:
        assert name.startswith("bronze.")


def test_table_names_covers_all_data_types() -> None:
    expected = {"projects", "releases", "boards", "issues", "transitions"}
    assert set(TABLE_NAMES.keys()) == expected


# ── Redshift S3 COPY ─────────────────────────────────────────────────────────


def _redshift_loader() -> RedshiftLoader:
    with patch("jira_ingest.loader.sqlalchemy_loader.create_engine", return_value=MagicMock()):
        ldr = create_loader("redshift+psycopg2://user:pass@host:5439/db")
    assert isinstance(ldr, RedshiftLoader)
    return ldr


def test_redshift_loader_pg_upsert_uses_plain_insert_not_on_conflict() -> None:
    """Redshift reports a "postgresql" dialect name but its SQL grammar has no
    ON CONFLICT clause, so RedshiftLoader must not use the base class's
    postgres upsert path."""
    ldr = _redshift_loader()
    mock_conn = MagicMock()
    mock_table = MagicMock()
    records = [{"id": 1}, {"id": 2}]

    result = ldr._pg_upsert(mock_conn, mock_table, records)

    mock_table.insert.assert_called_once_with()
    mock_conn.execute.assert_called_once_with(mock_table.insert.return_value, records)
    assert result == len(records)


def test_load_all_from_s3_skips_data_types_with_no_file(tmp_path: Path) -> None:
    """Regression test for #2: a COPY against a data type that produced zero
    records (so ParquetWriter never wrote a file) must not crash the run."""
    (tmp_path / "projects").mkdir()
    (tmp_path / "projects" / "projects_20240601.parquet").write_bytes(b"fake-parquet")
    # No file written for any other data type.

    ldr = _redshift_loader()
    with patch.object(ldr, "load_from_s3") as mock_load_from_s3:
        ldr.load_all_from_s3(str(tmp_path), "20240601")

    called_types = {call.args[0] for call in mock_load_from_s3.call_args_list}
    assert called_types == {"projects"}


def test_load_all_from_s3_calls_nothing_when_no_files_exist(tmp_path: Path) -> None:
    ldr = _redshift_loader()
    with patch.object(ldr, "load_from_s3") as mock_load_from_s3:
        ldr.load_all_from_s3(str(tmp_path), "20240601")

    mock_load_from_s3.assert_not_called()
