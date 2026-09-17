"""Tests for the CLI's write orchestration (regression coverage for #1)."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pandas as pd
from click.testing import CliRunner

from jira_ingest.cli import cli
from jira_ingest.config import Settings


def _settings(sink_uri: str, output_format: str = "parquet") -> Settings:
    return Settings.model_validate(
        {
            "url": "https://jira.example.com",
            "api_token": "tok",
            "mode": "cloud",
            "email": "user@example.com",
            "sink_uri": sink_uri,
            "output_format": output_format,
        }
    )


async def _fake_stream_all(
    *_args: Any, **_kwargs: Any
) -> AsyncGenerator[tuple[str, list[dict[str, Any]]], None]:
    """Simulate multiple boards/projects each yielding a separate batch."""
    yield "projects", [{"id": 1, "key": "PROJ-1"}]
    yield "projects", [{"id": 2, "key": "PROJ-2"}]
    yield "issues", [{"id": 10, "key": "PROJ-1-1"}]


class _FakeClient:
    async def __aenter__(self) -> _FakeClient:
        return self

    async def __aexit__(self, *_args: Any) -> None:
        return None


def test_parquet_output_contains_records_from_every_batch(tmp_path: Path) -> None:
    """Regression test for #1: ParquetWriter used to overwrite per batch,
    so only the last board/project's records survived on disk. Each
    data_type is now a directory of part files (see #27); pandas reads a
    directory of parquet parts as a single dataset."""
    settings = _settings(str(tmp_path))
    runner = CliRunner()

    with (
        patch("jira_ingest.cli.Settings", return_value=settings),
        patch("jira_ingest.cli.create_client", return_value=_FakeClient()),
        patch("jira_ingest.cli.stream_all", side_effect=_fake_stream_all),
    ):
        result = runner.invoke(cli, ["run", "--date-suffix", "20240601"])

    assert result.exit_code == 0, result.output

    df = pd.read_parquet(tmp_path / "projects" / "projects_20240601")
    assert set(df["key"]) == {"PROJ-1", "PROJ-2"}

    issues_df = pd.read_parquet(tmp_path / "issues" / "issues_20240601")
    assert set(issues_df["key"]) == {"PROJ-1-1"}


def test_rerun_replaces_previous_output_by_default(tmp_path: Path) -> None:
    """A second run with the same --date-suffix clears the first run's
    parts before writing new ones, rather than accumulating a union of
    both runs -- matching the pre-#27 Parquet overwrite behaviour."""
    settings = _settings(str(tmp_path))
    runner = CliRunner()

    async def _first_run(*_a: Any, **_k: Any) -> AsyncGenerator[Any, None]:
        yield "projects", [{"id": 1, "key": "FIRST-RUN"}]

    async def _second_run(*_a: Any, **_k: Any) -> AsyncGenerator[Any, None]:
        yield "projects", [{"id": 2, "key": "SECOND-RUN"}]

    with (
        patch("jira_ingest.cli.Settings", return_value=settings),
        patch("jira_ingest.cli.create_client", return_value=_FakeClient()),
        patch("jira_ingest.cli.stream_all", side_effect=_first_run),
    ):
        runner.invoke(cli, ["run", "--date-suffix", "20240601"])

    with (
        patch("jira_ingest.cli.Settings", return_value=settings),
        patch("jira_ingest.cli.create_client", return_value=_FakeClient()),
        patch("jira_ingest.cli.stream_all", side_effect=_second_run),
    ):
        result = runner.invoke(cli, ["run", "--date-suffix", "20240601"])

    assert result.exit_code == 0, result.output
    df = pd.read_parquet(tmp_path / "projects" / "projects_20240601")
    assert set(df["key"]) == {"SECOND-RUN"}


def test_append_flag_accumulates_instead_of_replacing(tmp_path: Path) -> None:
    settings = _settings(str(tmp_path))
    runner = CliRunner()

    async def _first_run(*_a: Any, **_k: Any) -> AsyncGenerator[Any, None]:
        yield "projects", [{"id": 1, "key": "FIRST-RUN"}]

    async def _second_run(*_a: Any, **_k: Any) -> AsyncGenerator[Any, None]:
        yield "projects", [{"id": 2, "key": "SECOND-RUN"}]

    with (
        patch("jira_ingest.cli.Settings", return_value=settings),
        patch("jira_ingest.cli.create_client", return_value=_FakeClient()),
        patch("jira_ingest.cli.stream_all", side_effect=_first_run),
    ):
        runner.invoke(cli, ["run", "--date-suffix", "20240601", "--append"])

    with (
        patch("jira_ingest.cli.Settings", return_value=settings),
        patch("jira_ingest.cli.create_client", return_value=_FakeClient()),
        patch("jira_ingest.cli.stream_all", side_effect=_second_run),
    ):
        result = runner.invoke(cli, ["run", "--date-suffix", "20240601", "--append"])

    assert result.exit_code == 0, result.output
    df = pd.read_parquet(tmp_path / "projects" / "projects_20240601")
    assert set(df["key"]) == {"FIRST-RUN", "SECOND-RUN"}
