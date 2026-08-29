"""Tests for the output writer layer (Sink + format writers)."""

from __future__ import annotations

import csv
import json
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from jira_ingest.output.sink import Sink
from jira_ingest.output.writer import CsvWriter, JsonLinesWriter, ParquetWriter, create_writer

SAMPLE_RECORDS = [
    {"id": 1, "key": "PROJ-1", "summary": "Alpha", "labels": "backend"},
    {"id": 2, "key": "PROJ-2", "summary": "Beta", "labels": None},
]


class TestSink:
    def test_full_path_joins_uri_and_relative(self, tmp_path: Path) -> None:
        sink = Sink(str(tmp_path))
        expected = f"{tmp_path}/issues/issues_20240601.csv"
        assert sink.full_path("issues/issues_20240601.csv") == expected

    def test_trailing_slash_on_uri_is_normalised(self, tmp_path: Path) -> None:
        sink = Sink(str(tmp_path) + "/")
        assert not sink.uri.endswith("/")

    def test_exists_returns_false_for_missing(self, tmp_path: Path) -> None:
        sink = Sink(str(tmp_path))
        assert not sink.exists("nonexistent.csv")

    def test_open_creates_file(self, tmp_path: Path) -> None:
        sink = Sink(str(tmp_path))
        with sink.open("test.txt", "wb") as f:
            f.write(b"hello")
        assert (tmp_path / "test.txt").read_bytes() == b"hello"

    def test_exists_returns_true_after_write(self, tmp_path: Path) -> None:
        sink = Sink(str(tmp_path))
        with sink.open("present.txt", "wb") as f:
            f.write(b"x")
        assert sink.exists("present.txt")


class TestCsvWriter:
    def test_writes_records_with_header(self, tmp_path: Path) -> None:
        writer = CsvWriter()
        sink = Sink(str(tmp_path))
        writer.write("issues", SAMPLE_RECORDS, sink, "20240601")

        path = tmp_path / "issues" / "issues_20240601.csv"
        assert path.exists()
        rows = list(csv.DictReader(path.open()))
        assert len(rows) == 2
        assert rows[0]["key"] == "PROJ-1"

    def test_empty_records_writes_nothing(self, tmp_path: Path) -> None:
        writer = CsvWriter()
        sink = Sink(str(tmp_path))
        writer.write("issues", [], sink, "20240601")
        assert not (tmp_path / "issues" / "issues_20240601.csv").exists()

    def test_appends_on_second_call(self, tmp_path: Path) -> None:
        writer = CsvWriter()
        sink = Sink(str(tmp_path))
        writer.write("issues", SAMPLE_RECORDS[:1], sink, "20240601")
        writer.write("issues", SAMPLE_RECORDS[1:], sink, "20240601")

        path = tmp_path / "issues" / "issues_20240601.csv"
        rows = list(csv.DictReader(path.open()))
        assert len(rows) == 2


class TestParquetWriter:
    def test_writes_valid_parquet(self, tmp_path: Path) -> None:
        writer = ParquetWriter()
        sink = Sink(str(tmp_path))
        writer.write("projects", SAMPLE_RECORDS, sink, "20240601")

        path = tmp_path / "projects" / "projects_20240601.parquet"
        assert path.exists()
        df = pd.read_parquet(path)
        assert len(df) == 2
        assert "key" in df.columns

    def test_empty_records_writes_nothing(self, tmp_path: Path) -> None:
        writer = ParquetWriter()
        sink = Sink(str(tmp_path))
        writer.write("projects", [], sink, "20240601")
        assert not (tmp_path / "projects" / "projects_20240601.parquet").exists()

    def test_all_null_optional_columns_get_typed_not_null_type(self, tmp_path: Path) -> None:
        """Redshift's COPY ... FORMAT AS PARQUET does strict column-type
        matching and rejects an all-null Arrow `null`-typed source column
        against any real target column type (confirmed against a real
        Redshift Serverless workgroup). A batch where every issue lacks an
        epic, a parent, or a created date -- entirely plausible in a real
        Jira project -- must not produce `null`-typed Parquet columns for
        those fields."""
        records = [
            {
                "id": i,
                "key": f"PROJ-{i}",
                "project_id": 1,
                "project_key": "PROJ",
                "project_name": "Project",
                "epic_id": None,
                "epic_done": None,
                "created": None,
                "labels": None,
                "custom_fields": {},
            }
            for i in range(1, 3)
        ]
        writer = ParquetWriter()
        sink = Sink(str(tmp_path))
        writer.write("issues", records, sink, "20240601")

        schema = pq.read_schema(tmp_path / "issues" / "issues_20240601.parquet")
        assert not pa.types.is_null(schema.field("epic_id").type)
        assert pa.types.is_integer(schema.field("epic_id").type)
        assert not pa.types.is_null(schema.field("epic_done").type)
        assert pa.types.is_boolean(schema.field("epic_done").type)
        assert not pa.types.is_null(schema.field("created").type)
        assert pa.types.is_timestamp(schema.field("created").type)
        assert not pa.types.is_null(schema.field("labels").type)
        assert pa.types.is_string(schema.field("labels").type)

    def test_populated_columns_unaffected_by_null_type_fix(self, tmp_path: Path) -> None:
        writer = ParquetWriter()
        sink = Sink(str(tmp_path))
        writer.write("projects", SAMPLE_RECORDS, sink, "20240601")

        df = pd.read_parquet(tmp_path / "projects" / "projects_20240601.parquet")
        assert list(df["key"]) == ["PROJ-1", "PROJ-2"]


class TestJsonLinesWriter:
    def test_writes_ndjson(self, tmp_path: Path) -> None:
        writer = JsonLinesWriter()
        sink = Sink(str(tmp_path))
        writer.write("boards", SAMPLE_RECORDS, sink, "20240601")

        path = tmp_path / "boards" / "boards_20240601.jsonl"
        assert path.exists()
        lines = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
        assert len(lines) == 2
        assert lines[0]["key"] == "PROJ-1"

    def test_appends_on_second_call(self, tmp_path: Path) -> None:
        writer = JsonLinesWriter()
        sink = Sink(str(tmp_path))
        writer.write("boards", SAMPLE_RECORDS[:1], sink, "20240601")
        writer.write("boards", SAMPLE_RECORDS[1:], sink, "20240601")

        path = tmp_path / "boards" / "boards_20240601.jsonl"
        lines = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
        assert len(lines) == 2


class TestCreateWriter:
    def test_csv(self) -> None:
        assert isinstance(create_writer("csv"), CsvWriter)

    def test_parquet(self) -> None:
        assert isinstance(create_writer("parquet"), ParquetWriter)

    def test_jsonl(self) -> None:
        assert isinstance(create_writer("jsonl"), JsonLinesWriter)

    def test_unknown_format_raises(self) -> None:
        with pytest.raises(ValueError, match="Unsupported"):
            create_writer("excel")  # type: ignore[arg-type]
