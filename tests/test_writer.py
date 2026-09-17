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
from jira_ingest.output.writer import (
    BatchWriter,
    CsvWriter,
    JsonLinesWriter,
    ParquetWriter,
    create_writer,
)

SAMPLE_RECORDS = [
    {"id": 1, "key": "PROJ-1", "summary": "Alpha", "labels": "backend"},
    {"id": 2, "key": "PROJ-2", "summary": "Beta", "labels": None},
]


def _parts(tmp_path: Path, data_type: str, date_suffix: str, extension: str) -> list[Path]:
    directory = tmp_path / data_type / f"{data_type}_{date_suffix}"
    if not directory.exists():
        return []
    return sorted(directory.glob(f"part-*.{extension}"))


def _read_all_csv_rows(parts: list[Path]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for part in parts:
        rows.extend(csv.DictReader(part.open()))
    return rows


def _read_all_jsonl_lines(parts: list[Path]) -> list[dict[str, object]]:
    lines: list[dict[str, object]] = []
    for part in parts:
        lines.extend(json.loads(line) for line in part.read_text().splitlines() if line.strip())
    return lines


class TestSink:
    def test_full_path_joins_uri_and_relative(self, tmp_path: Path) -> None:
        sink = Sink(str(tmp_path))
        expected = f"{tmp_path}/issues/issues_20240601/part-abc.csv"
        assert sink.full_path("issues/issues_20240601/part-abc.csv") == expected

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

    def test_clear_directory_is_noop_when_nothing_exists(self, tmp_path: Path) -> None:
        sink = Sink(str(tmp_path))
        sink.clear_directory("issues/issues_20240601")  # must not raise

    def test_clear_directory_removes_existing_files(self, tmp_path: Path) -> None:
        sink = Sink(str(tmp_path))
        with sink.open("issues/issues_20240601/part-1.csv") as f:
            f.write(b"a")
        with sink.open("issues/issues_20240601/part-2.csv") as f:
            f.write(b"b")

        sink.clear_directory("issues/issues_20240601")

        assert not (tmp_path / "issues" / "issues_20240601" / "part-1.csv").exists()
        assert not (tmp_path / "issues" / "issues_20240601" / "part-2.csv").exists()

    def test_clear_directory_does_not_touch_other_date_suffixes(self, tmp_path: Path) -> None:
        sink = Sink(str(tmp_path))
        with sink.open("issues/issues_20240601/part-1.csv") as f:
            f.write(b"old-run")
        with sink.open("issues/issues_20240602/part-1.csv") as f:
            f.write(b"different-day")

        sink.clear_directory("issues/issues_20240601")

        assert not (tmp_path / "issues" / "issues_20240601" / "part-1.csv").exists()
        assert (tmp_path / "issues" / "issues_20240602" / "part-1.csv").exists()


class TestCsvWriter:
    def test_writes_one_part_with_header(self, tmp_path: Path) -> None:
        writer = CsvWriter()
        sink = Sink(str(tmp_path))
        writer.write("issues", SAMPLE_RECORDS, sink, "20240601")

        parts = _parts(tmp_path, "issues", "20240601", "csv")
        assert len(parts) == 1
        rows = _read_all_csv_rows(parts)
        assert len(rows) == 2
        assert rows[0]["key"] == "PROJ-1"

    def test_empty_records_writes_nothing(self, tmp_path: Path) -> None:
        writer = CsvWriter()
        sink = Sink(str(tmp_path))
        writer.write("issues", [], sink, "20240601")
        assert _parts(tmp_path, "issues", "20240601", "csv") == []

    def test_each_call_writes_a_new_independent_part(self, tmp_path: Path) -> None:
        """Each part is self-contained (own header), unlike the old
        single-file-append design -- readers concatenate multiple
        independently-readable CSVs, the same way Spark/Hive parts work."""
        writer = CsvWriter()
        sink = Sink(str(tmp_path))
        writer.write("issues", SAMPLE_RECORDS[:1], sink, "20240601")
        writer.write("issues", SAMPLE_RECORDS[1:], sink, "20240601")

        parts = _parts(tmp_path, "issues", "20240601", "csv")
        assert len(parts) == 2
        for part in parts:
            assert part.read_text().startswith("id,key,summary,labels")
        assert _read_all_csv_rows(parts) and len(_read_all_csv_rows(parts)) == 2

    def test_part_names_are_unique(self, tmp_path: Path) -> None:
        writer = CsvWriter()
        sink = Sink(str(tmp_path))
        writer.write("issues", SAMPLE_RECORDS, sink, "20240601")
        writer.write("issues", SAMPLE_RECORDS, sink, "20240601")

        parts = _parts(tmp_path, "issues", "20240601", "csv")
        assert len(parts) == 2
        assert parts[0].name != parts[1].name


class TestParquetWriter:
    def test_writes_one_part(self, tmp_path: Path) -> None:
        writer = ParquetWriter()
        sink = Sink(str(tmp_path))
        writer.write("projects", SAMPLE_RECORDS, sink, "20240601")

        parts = _parts(tmp_path, "projects", "20240601", "parquet")
        assert len(parts) == 1
        df = pd.read_parquet(parts[0])
        assert len(df) == 2
        assert "key" in df.columns

    def test_empty_records_writes_nothing(self, tmp_path: Path) -> None:
        writer = ParquetWriter()
        sink = Sink(str(tmp_path))
        writer.write("projects", [], sink, "20240601")
        assert _parts(tmp_path, "projects", "20240601", "parquet") == []

    def test_each_call_writes_a_new_independent_part(self, tmp_path: Path) -> None:
        writer = ParquetWriter()
        sink = Sink(str(tmp_path))
        writer.write("projects", SAMPLE_RECORDS[:1], sink, "20240601")
        writer.write("projects", SAMPLE_RECORDS[1:], sink, "20240601")

        parts = _parts(tmp_path, "projects", "20240601", "parquet")
        assert len(parts) == 2
        total_rows = sum(len(pd.read_parquet(p)) for p in parts)
        assert total_rows == 2

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

        parts = _parts(tmp_path, "issues", "20240601", "parquet")
        schema = pq.read_schema(parts[0])
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

        parts = _parts(tmp_path, "projects", "20240601", "parquet")
        df = pd.read_parquet(parts[0])
        assert list(df["key"]) == ["PROJ-1", "PROJ-2"]

    def test_empty_custom_fields_round_trips_as_null_not_fabricated_data(
        self, tmp_path: Path
    ) -> None:
        """custom_fields defaults to {} when no custom fields are configured
        -- the common case, not an edge case. PyArrow can't write a
        zero-field struct<> to Parquet at all, but the placeholder used to
        work around that must not fabricate a value: {"_empty": null} would
        persist through Parquet and into Redshift's SUPER column via COPY
        ... SERIALIZETOJSON, so consumers would see a field that was never
        actually in custom_fields instead of the original {}."""
        records = [{"id": i, "key": f"PROJ-{i}", "custom_fields": {}} for i in range(1, 3)]
        writer = ParquetWriter()
        sink = Sink(str(tmp_path))
        writer.write("issues", records, sink, "20240601")

        parts = _parts(tmp_path, "issues", "20240601", "parquet")
        table = pq.read_table(parts[0])
        assert pa.types.is_struct(table.schema.field("custom_fields").type)
        assert table.column("custom_fields").to_pylist() == [None, None]


class TestJsonLinesWriter:
    def test_writes_one_part(self, tmp_path: Path) -> None:
        writer = JsonLinesWriter()
        sink = Sink(str(tmp_path))
        writer.write("boards", SAMPLE_RECORDS, sink, "20240601")

        parts = _parts(tmp_path, "boards", "20240601", "jsonl")
        assert len(parts) == 1
        lines = _read_all_jsonl_lines(parts)
        assert len(lines) == 2
        assert lines[0]["key"] == "PROJ-1"

    def test_each_call_writes_a_new_independent_part(self, tmp_path: Path) -> None:
        writer = JsonLinesWriter()
        sink = Sink(str(tmp_path))
        writer.write("boards", SAMPLE_RECORDS[:1], sink, "20240601")
        writer.write("boards", SAMPLE_RECORDS[1:], sink, "20240601")

        parts = _parts(tmp_path, "boards", "20240601", "jsonl")
        assert len(parts) == 2
        assert len(_read_all_jsonl_lines(parts)) == 2


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


class TestBatchWriter:
    def test_buffers_below_threshold_until_flush_all(self, tmp_path: Path) -> None:
        writer = CsvWriter()
        sink = Sink(str(tmp_path))
        bw = BatchWriter(writer, sink, "20240601", max_records=10)

        bw.add("issues", SAMPLE_RECORDS)
        assert _parts(tmp_path, "issues", "20240601", "csv") == []

        bw.flush_all()
        parts = _parts(tmp_path, "issues", "20240601", "csv")
        assert len(parts) == 1
        assert len(_read_all_csv_rows(parts)) == 2

    def test_flushes_automatically_once_threshold_reached(self, tmp_path: Path) -> None:
        writer = CsvWriter()
        sink = Sink(str(tmp_path))
        bw = BatchWriter(writer, sink, "20240601", max_records=2)

        bw.add("issues", SAMPLE_RECORDS)  # exactly at threshold -> flush immediately
        parts = _parts(tmp_path, "issues", "20240601", "csv")
        assert len(parts) == 1
        assert len(_read_all_csv_rows(parts)) == 2

    def test_many_small_yields_accumulate_into_one_part_not_many(self, tmp_path: Path) -> None:
        """stream_all yields exactly one record at a time for e.g. 'projects'
        and 'boards' -- flushing on every add() would produce mostly
        one-row part files. Buffering up to the threshold avoids that."""
        writer = CsvWriter()
        sink = Sink(str(tmp_path))
        bw = BatchWriter(writer, sink, "20240601", max_records=100)

        for record in SAMPLE_RECORDS * 10:  # 20 single-record adds
            bw.add("projects", [record])
        bw.flush_all()

        parts = _parts(tmp_path, "projects", "20240601", "csv")
        assert len(parts) == 1
        assert len(_read_all_csv_rows(parts)) == 20

    def test_flush_all_only_touches_data_types_with_buffered_records(self, tmp_path: Path) -> None:
        writer = CsvWriter()
        sink = Sink(str(tmp_path))
        bw = BatchWriter(writer, sink, "20240601", max_records=100)

        bw.add("issues", SAMPLE_RECORDS)
        bw.flush_all()

        assert _parts(tmp_path, "issues", "20240601", "csv") != []
        assert _parts(tmp_path, "boards", "20240601", "csv") == []

    def test_clear_previous_removes_prior_parts_for_enabled_data_types_only(
        self, tmp_path: Path
    ) -> None:
        writer = CsvWriter()
        sink = Sink(str(tmp_path))
        with sink.open("issues/issues_20240601/part-old.csv") as f:
            f.write(b"stale-run")
        with sink.open("boards/boards_20240601/part-old.csv") as f:
            f.write(b"unrelated-data-type")

        bw = BatchWriter(writer, sink, "20240601", max_records=100)
        bw.clear_previous(["issues"])

        assert not (tmp_path / "issues" / "issues_20240601" / "part-old.csv").exists()
        assert (tmp_path / "boards" / "boards_20240601" / "part-old.csv").exists()

    def test_add_with_empty_records_does_not_create_a_buffer(self, tmp_path: Path) -> None:
        writer = CsvWriter()
        sink = Sink(str(tmp_path))
        bw = BatchWriter(writer, sink, "20240601", max_records=10)

        bw.add("issues", [])
        bw.flush_all()

        assert _parts(tmp_path, "issues", "20240601", "csv") == []

    def test_a_single_oversized_batch_is_chunked_not_written_as_one_part(
        self, tmp_path: Path
    ) -> None:
        """A single yielded batch larger than max_records (e.g. one board's
        full page of issues) must not land as one oversized part -- that
        would make JIRA_PART_FILE_MAX_RECORDS a misleading name, since it's
        documented as a cap on part size, not just a flush trigger."""
        writer = CsvWriter()
        sink = Sink(str(tmp_path))
        bw = BatchWriter(writer, sink, "20240601", max_records=10)

        big_batch = [{"id": i, "key": f"PROJ-{i}"} for i in range(25)]
        bw.add("issues", big_batch)  # single add() call, no threshold crossing in between

        parts = _parts(tmp_path, "issues", "20240601", "csv")
        assert len(parts) == 3  # 10 + 10 + 5
        row_counts = sorted(len(_read_all_csv_rows([p])) for p in parts)
        assert row_counts == [5, 10, 10]
        assert sum(row_counts) == 25
