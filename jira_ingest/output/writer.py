"""Format writers: CSV, Parquet, JSON Lines.

Each writer receives a ``Sink`` (which handles the destination protocol) and
writes one batch of records as one self-contained "part" file per ``write()``
call -- ``{data_type}/{data_type}_{date_suffix}/part-<uuid>.{ext}``, never a
single file appended or overwritten across calls. Writers do not care
whether the sink points at a local path, S3, Azure Blob, GCS, or anything
else.

Each part is fully finalized (schema/footer, or header + rows, written) the
moment its single write call returns -- there is no held-open state, so a
crash between two ``write()`` calls leaves every previously-written part
valid and readable; only the batch that hadn't been written yet is lost.
This is why multiple parts, not one appended/overwritten file: appending to
Parquet isn't really possible without holding a writer open across the
whole run (and an interrupted stream has no footer, so it's unreadable
garbage, not partial-but-useful data), and GCS has no cheap append
primitive at all (see the removed ``Sink.write_or_append``) -- but every
backend, including GCS, trivially supports writing one more small,
independent file.

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
import uuid
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
from jira_ingest.utils import batched

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

    A placeholder field keeps the column a struct (so a later batch that
    does have data uses the same nested-struct-then-SERIALIZETOJSON path
    already verified against a real Redshift Serverless workgroup) while
    letting an all-empty batch round-trip through Parquet at all -- but
    every row's value must be a top-level NULL, not an instantiated struct
    like ``{"_empty": null}``. The latter is fabricated data: it would
    persist through Parquet and into Redshift's SUPER column via
    SERIALIZETOJSON, so consumers would see a field that was never actually
    present in ``custom_fields`` instead of the original ``{}``.
    """
    for index, field in enumerate(table.schema):
        if pa.types.is_struct(field.type) and field.type.num_fields == 0:
            placeholder_type = pa.struct([pa.field("_empty", pa.bool_())])
            values = pa.array([None] * table.num_rows, type=placeholder_type)
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

    def _directory(self, data_type: str, date_suffix: str) -> str:
        return f"{data_type}/{data_type}_{date_suffix}"

    def _part_path(self, data_type: str, date_suffix: str, extension: str) -> str:
        return f"{self._directory(data_type, date_suffix)}/part-{uuid.uuid4().hex}.{extension}"


class CsvWriter(BaseWriter):
    """CSV writer. Each call writes one new, independent part file with its
    own header row -- readers concatenating multiple parts read each with
    its own header, the same way Spark/Hive CSV part-files work."""

    def write(
        self,
        data_type: str,
        records: list[dict[str, Any]],
        sink: Sink,
        date_suffix: str,
    ) -> None:
        if not records:
            return

        path = self._part_path(data_type, date_suffix, "csv")

        buf = io.StringIO()
        writer = csv.DictWriter(
            buf,
            fieldnames=list(records[0].keys()),
            quoting=csv.QUOTE_MINIMAL,
            extrasaction="ignore",
        )
        writer.writeheader()
        writer.writerows(records)

        with sink.open(path, "wb") as f:
            f.write(buf.getvalue().encode("utf-8"))

        logger.info("CSV: wrote %d rows -> %s", len(records), sink.full_path(path))


class ParquetWriter(BaseWriter):
    """Parquet writer using PyArrow with Snappy compression. Each call
    writes one new, independent part file -- a fresh, self-contained
    schema and footer every time, so a part is either fully valid the
    moment ``write()`` returns or doesn't exist at all."""

    def write(
        self,
        data_type: str,
        records: list[dict[str, Any]],
        sink: Sink,
        date_suffix: str,
    ) -> None:
        if not records:
            return

        path = self._part_path(data_type, date_suffix, "parquet")

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
    """JSON Lines (NDJSON) writer. Each call writes one new, independent
    part file; NDJSON has no header, so parts concatenate trivially."""

    def write(
        self,
        data_type: str,
        records: list[dict[str, Any]],
        sink: Sink,
        date_suffix: str,
    ) -> None:
        if not records:
            return

        path = self._part_path(data_type, date_suffix, "jsonl")

        lines = "\n".join(json.dumps(r, default=str) for r in records) + "\n"

        with sink.open(path, "wb") as f:
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


class BatchWriter:
    """Buffers records per data type across many small yields (e.g. from
    ``processor.stream_all``, where "projects" and "boards" each yield
    exactly one record at a time) and flushes to one or more part files of
    at most ``max_records`` each once a data type's buffer reaches that
    threshold, so output is neither one tiny part per yield nor one
    unbounded part regardless of how large a single incoming batch is.

    A crash between flushes loses at most one data type's current,
    below-threshold buffer -- everything already flushed is on disk as
    valid, independent part files.

    Usage::

        bw = BatchWriter(writer, sink, date_suffix, max_records=10_000)
        bw.clear_previous(settings.data_types)  # replace-on-rerun
        async for data_type, records in stream_all(...):
            bw.add(data_type, records)
        bw.flush_all()  # any remainder below threshold
    """

    def __init__(
        self,
        writer: BaseWriter,
        sink: Sink,
        date_suffix: str,
        max_records: int,
    ) -> None:
        self._writer = writer
        self._sink = sink
        self._date_suffix = date_suffix
        self._max_records = max_records
        self._buffers: dict[str, list[dict[str, Any]]] = {}

    def clear_previous(self, data_types: list[str]) -> None:
        """Delete any existing parts for each enabled data type's
        ``{date_suffix}`` directory before writing new ones.

        Re-running with the same ``date_suffix`` replaces that run's output
        rather than accumulating a union of multiple attempts -- matching
        the sink-agnostic writers' previous single-file-overwrite semantics
        (Parquet), rather than their previous single-file-append semantics
        (CSV/JSONL). Accepted tradeoff: a re-run that crashes partway
        through leaves only the new, incomplete parts -- the previous
        completed run's data for that data type is already gone by the
        time new parts start landing.
        """
        for data_type in data_types:
            directory = self._writer._directory(data_type, self._date_suffix)
            self._sink.clear_directory(directory)

    def add(self, data_type: str, records: list[dict[str, Any]]) -> None:
        if not records:
            return
        buffer = self._buffers.setdefault(data_type, [])
        buffer.extend(records)
        if len(buffer) >= self._max_records:
            self._flush(data_type)

    def flush_all(self) -> None:
        for data_type in list(self._buffers):
            self._flush(data_type)

    def _flush(self, data_type: str) -> None:
        """Write the buffer as one or more parts of at most ``max_records``
        each, not one part however large the buffer has grown.

        ``add()`` only triggers a flush once the buffer *reaches*
        ``max_records``, but a single incoming batch can itself be larger
        than that (e.g. one board's full page of issues) -- in that case
        the buffer jumps straight past the threshold in one ``add()`` call,
        and this method has to split it rather than writing it as a single
        oversized part.
        """
        buffer = self._buffers.get(data_type)
        if not buffer:
            return
        for chunk in batched(buffer, self._max_records):
            self._writer.write(data_type, chunk, self._sink, self._date_suffix)
        self._buffers[data_type] = []
