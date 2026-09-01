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
import types
import typing
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Literal

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import BaseModel

from jira_ingest.output.sink import Sink
from jira_ingest.schemas import (
    BoardRecord,
    IssueRecord,
    ProjectRecord,
    ReleaseRecord,
    TransitionRecord,
)

logger = logging.getLogger(__name__)

OutputFormat = Literal["csv", "parquet", "jsonl"]

_SCHEMA_MODELS: dict[str, type[BaseModel]] = {
    "projects": ProjectRecord,
    "releases": ReleaseRecord,
    "boards": BoardRecord,
    "issues": IssueRecord,
    "transitions": TransitionRecord,
}


def _arrow_type_for(annotation: Any) -> pa.DataType | None:
    """Map a Pydantic field annotation to a concrete PyArrow type, or ``None``
    if it's not one we know how to type explicitly (e.g. ``dict[str, Any]``)."""
    origin = typing.get_origin(annotation)
    if origin in (typing.Union, types.UnionType):
        args = [a for a in typing.get_args(annotation) if a is not type(None)]
        if len(args) != 1:
            return None
        annotation = args[0]
    if annotation is int:
        return pa.int64()
    if annotation is str:
        return pa.string()
    if annotation is bool:
        return pa.bool_()
    if annotation is datetime:
        return pa.timestamp("us", tz="UTC")
    return None


def _fix_null_typed_columns(table: pa.Table, data_type: str) -> pa.Table:
    """Replace any column PyArrow inferred as the untyped ``null`` type
    (every value ``None`` across the whole batch) with its real declared
    type, still all-null.

    Redshift's ``COPY ... FORMAT AS PARQUET`` does strict column-type
    matching and rejects an all-null ``null``-typed source column against
    any real target column type -- confirmed against a real Redshift
    Serverless workgroup with a batch where e.g. every issue lacked an
    epic. Other sinks (S3 data lake consumers, re-reading the file with
    pandas) are unaffected either way, so this is safe to apply generally.
    """
    model = _SCHEMA_MODELS.get(data_type)
    if model is None:
        return table

    for index, field in enumerate(table.schema):
        if not pa.types.is_null(field.type):
            continue
        field_info = model.model_fields.get(field.name)
        arrow_type = _arrow_type_for(field_info.annotation) if field_info else None
        if arrow_type is None:
            continue
        table = table.set_column(
            index, pa.field(field.name, arrow_type), pa.nulls(table.num_rows, type=arrow_type)
        )
    return table


def _fix_empty_struct_columns(table: pa.Table) -> pa.Table:
    """PyArrow cannot write a struct type with zero child fields to Parquet
    at all ("Cannot write struct type ... with no child field"). It infers
    exactly this degenerate ``struct<>`` for ``custom_fields`` whenever every
    record in the batch has none configured -- ``custom_fields: dict = {}``
    is the default, so this is the common case for anyone not using custom
    fields, not a rare edge case. Confirmed independently of Redshift: this
    crashes ``pq.write_table`` for every sink.

    Adding a placeholder field keeps the column a struct (so a later batch
    that does have data uses the same nested-struct-then-SERIALIZETOJSON
    path already verified against a real Redshift Serverless workgroup)
    while letting an all-empty batch round-trip through Parquet at all.
    """
    for index, field in enumerate(table.schema):
        if pa.types.is_struct(field.type) and field.type.num_fields == 0:
            placeholder_type = pa.struct([pa.field("_empty", pa.bool_())])
            values = pa.array([{"_empty": None}] * table.num_rows, type=placeholder_type)
            table = table.set_column(index, pa.field(field.name, placeholder_type), values)
    return table


class BaseWriter(ABC):
    """Abstract base: subclasses implement ``write`` for a specific format."""

    @abstractmethod
    def write(
        self,
        data_type: str,
        records: list[dict[str, Any]],
        sink: Sink,
        date_suffix: str,
    ) -> None: ...

    def _path(self, data_type: str, date_suffix: str, extension: str) -> str:
        return f"{data_type}/{data_type}_{date_suffix}.{extension}"


class CsvWriter(BaseWriter):
    """Append-friendly CSV writer.

    Each call appends to the target file (creating it with a header row on
    first write). Thread-safe at the file level via the sink's atomic open.

    Appending works correctly even on backends with no real append
    primitive (e.g. GCS -- see ``Sink.write_or_append``): the writer only
    decides whether to emit a header row, and delegates the actual
    append-or-fallback logic to the sink.
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

        sink.write_or_append(path, buf.getvalue().encode("utf-8"), file_exists)

        logger.info("CSV: wrote %d rows -> %s", len(records), sink.full_path(path))


class ParquetWriter(BaseWriter):
    """Parquet writer using PyArrow with Snappy compression.

    Each call overwrites the target file, so callers must pass every record
    for a given ``(data_type, date_suffix)`` in a single ``write()`` call
    (the CLI accumulates records in memory across the run for this reason).
    For genuinely incremental/partitioned output, use distinct ``date_suffix``
    values or add a partition column instead.
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
        table = _fix_null_typed_columns(table, data_type)
        table = _fix_empty_struct_columns(table)

        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
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

        sink.write_or_append(path, lines.encode("utf-8"), file_exists)

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
