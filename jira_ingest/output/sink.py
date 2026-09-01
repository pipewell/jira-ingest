"""Protocol-agnostic sink built on fsspec.

A ``Sink`` wraps an fsspec URI and exposes ``open()`` for writing files.
The protocol is inferred from the URI scheme:

    ``./output``               -- local filesystem
    ``s3://bucket/prefix``     -- AWS S3  (requires s3fs)
    ``az://container/prefix``  -- Azure Blob Storage  (requires adlfs)
    ``gs://bucket/prefix``     -- Google Cloud Storage  (requires gcsfs)
    ``abfs://...``             -- Azure Data Lake Gen2  (requires adlfs)

``storage_options`` are forwarded directly to fsspec and carry auth credentials
for the chosen protocol (IAM role, account keys, service principal, etc.).
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import IO, Any

import fsspec

# Protocols whose fsspec implementation genuinely appends bytes to an
# existing object rather than silently overwriting it: local disk (native
# OS append), S3 (s3fs re-buffers small files or uses UploadPartCopy for
# large ones), Azure (adlfs uses native Append Blobs). Notably absent: GCS
# -- gcsfs has no append primitive and silently rewrites "ab" to "wb" with
# only a warnings.warn(), discarding whatever was already there. Anything
# not in this list (including GCS, and any backend we haven't verified)
# gets the safe read-then-rewrite fallback in Sink.write_or_append instead
# of being trusted to append correctly.
_NATIVE_APPEND_PROTOCOLS = frozenset({"file", "local", "s3", "s3a", "abfs", "az", "abfss"})


class Sink:
    def __init__(self, uri: str, storage_options: dict[str, Any] | None = None) -> None:
        self._uri = uri.rstrip("/")
        self._storage_options = storage_options or {}

    @property
    def uri(self) -> str:
        return self._uri

    def full_path(self, relative_path: str) -> str:
        return f"{self._uri}/{relative_path.lstrip('/')}"

    @contextmanager
    def open(self, relative_path: str, mode: str = "wb") -> Iterator[IO[bytes]]:
        """Open a file at ``relative_path`` under this sink for writing."""
        path = self.full_path(relative_path)
        with fsspec.open(path, mode, **self._storage_options) as f:
            yield f

    def exists(self, relative_path: str) -> bool:
        path = self.full_path(relative_path)
        fs, _ = fsspec.core.url_to_fs(path, **self._storage_options)
        return bool(fs.exists(path))

    def _supports_native_append(self, relative_path: str) -> bool:
        path = self.full_path(relative_path)
        fs, _ = fsspec.core.url_to_fs(path, **self._storage_options)
        protocol = fs.protocol
        protocols = {protocol} if isinstance(protocol, str) else set(protocol)
        return bool(protocols & _NATIVE_APPEND_PROTOCOLS)

    def write_or_append(self, relative_path: str, data: bytes, file_exists: bool) -> None:
        """Write ``data``, appending to an existing file rather than
        overwriting it, working correctly even on backends with no real
        append primitive (see ``_NATIVE_APPEND_PROTOCOLS`` above).

        ``file_exists`` is taken from the caller rather than re-checked here
        since callers (CsvWriter, JsonLinesWriter) already need to know it
        to decide whether to write a header row.
        """
        if not file_exists or self._supports_native_append(relative_path):
            mode = "ab" if file_exists else "wb"
            with self.open(relative_path, mode) as f:
                f.write(data)
            return

        path = self.full_path(relative_path)
        fs, _ = fsspec.core.url_to_fs(path, **self._storage_options)
        existing = fs.cat(path)
        with self.open(relative_path, "wb") as f:
            f.write(existing + data)

    def makedirs(self, relative_path: str) -> None:
        """Create intermediate directories (no-op for object stores)."""
        path = self.full_path(relative_path)
        fs, _ = fsspec.core.url_to_fs(path, **self._storage_options)
        if hasattr(fs, "makedirs"):
            fs.makedirs(path, exist_ok=True)

    def __repr__(self) -> str:
        return f"Sink({self._uri!r})"
