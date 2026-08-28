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
    so only the last board/project's records survived on disk."""
    settings = _settings(str(tmp_path))
    runner = CliRunner()

    with (
        patch("jira_ingest.cli.Settings", return_value=settings),
        patch("jira_ingest.cli.create_client", return_value=_FakeClient()),
        patch("jira_ingest.cli.stream_all", side_effect=_fake_stream_all),
    ):
        result = runner.invoke(cli, ["run", "--date-suffix", "20240601"])

    assert result.exit_code == 0, result.output

    df = pd.read_parquet(tmp_path / "projects" / "projects_20240601.parquet")
    assert set(df["key"]) == {"PROJ-1", "PROJ-2"}

    issues_df = pd.read_parquet(tmp_path / "issues" / "issues_20240601.parquet")
    assert set(issues_df["key"]) == {"PROJ-1-1"}
