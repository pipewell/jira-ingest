"""Format writers: CSV, Parquet, JSON Lines.

Each writer receives a ``Sink`` (which handles the destination protocol) and
streams records into the appropriate format. Writers do not care whether the
sink points at a local path, S3, Azure Blob, GCS, or anything else.

Usage::

    sink = Sink("s3://my-bucket/jira-ingest", storage_options={"anon": False})
    writer = ParquetWriter()
    writer.write("issues", records, sink, date_suffix="20240601")
"""

from __future__ import annotations

import csv
import io
import json
import logging
from abc import ABC, abstractmethod
from typing import Any, Literal

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from jira_ingest.output.sink import Sink

logger = logging.getLogger(__name__)

OutputFormat = Literal["csv", "parquet", "jsonl"]


class BaseWriter(ABC):
    """Abstract base: subclasses implement ``write`` for a specific format."""

    @abstractmethod
    def write(
        self,
        data_type: str,
        records: list[dict[str, Any]],
        sink: Sink,
        date_suffix: str,
    ) -> None:
        ...

    def _path(self, data_type: str, date_suffix: str, extension: str) -> str:
        return f"{data_type}/{data_type}_{date_suffix}.{extension}"


class CsvWriter(BaseWriter):
    """Append-friendly CSV writer.

    Each call appends to the target file (creating it with a header row on
    first write). Thread-safe at the file level via the sink's atomic open.
    """

    def write(
        self,
        data_type: str,
        records: list[dict[str, Any]],
        sink: Sink,
        date_suffix: str,
    ) -> None:
        if not records:
            return

        path = self._path(data_type, date_suffix, "csv")
        file_exists = sink.exists(path)

        buf = io.StringIO()
        writer = csv.DictWriter(
            buf,
            fieldnames=list(records[0].keys()),
            quoting=csv.QUOTE_MINIMAL,
            extrasaction="ignore",
        )
        if not file_exists:
            writer.writeheader()
        writer.writerows(records)

        mode = "ab" if file_exists else "wb"
        with sink.open(path, mode) as f:
            f.write(buf.getvalue().encode("utf-8"))

        logger.info("CSV: wrote %d rows -> %s", len(records), sink.full_path(path))


class ParquetWriter(BaseWriter):
    """Parquet writer using PyArrow with Snappy compression.

    Each call overwrites the target file. For incremental/partitioned output
    call with distinct ``date_suffix`` values or add a partition column.
    """

    def write(
        self,
        data_type: str,
        records: list[dict[str, Any]],
        sink: Sink,
        date_suffix: str,
    ) -> None:
        if not records:
            return

        path = self._path(data_type, date_suffix, "parquet")

        df = pd.DataFrame(records)
        table = pa.Table.from_pandas(df, preserve_index=False)

        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")  # type: ignore[no-untyped-call]
        buf.seek(0)

        with sink.open(path, "wb") as f:
            f.write(buf.read())

        logger.info("Parquet: wrote %d rows -> %s", len(records), sink.full_path(path))


class JsonLinesWriter(BaseWriter):
    """JSON Lines (NDJSON) writer. One JSON object per line, UTF-8 encoded."""

    def write(
        self,
        data_type: str,
        records: list[dict[str, Any]],
        sink: Sink,
        date_suffix: str,
    ) -> None:
        if not records:
            return

        path = self._path(data_type, date_suffix, "jsonl")
        file_exists = sink.exists(path)

        lines = "\n".join(json.dumps(r, default=str) for r in records) + "\n"

        mode = "ab" if file_exists else "wb"
        with sink.open(path, mode) as f:
            f.write(lines.encode("utf-8"))

        logger.info("JSONL: wrote %d rows -> %s", len(records), sink.full_path(path))


def create_writer(output_format: OutputFormat) -> BaseWriter:
    """Factory: return a writer for the given format string."""
    writers: dict[OutputFormat, type[BaseWriter]] = {
        "csv": CsvWriter,
        "parquet": ParquetWriter,
        "jsonl": JsonLinesWriter,
    }
    if output_format not in writers:
        raise ValueError(
            f"Unsupported output format: {output_format!r}. Choose from {list(writers)}"
        )
    return writers[output_format]()
