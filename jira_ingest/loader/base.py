"""Abstract loader interface."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class BaseLoader(ABC):
    """Write Jira records into a target database.

    Subclasses implement ``load`` for a specific database engine or bulk-load
    protocol. The ``load_all`` convenience method drives the stream produced
    by ``processor.stream_all``.
    """

    @abstractmethod
    def load(self, data_type: str, records: list[dict[str, Any]]) -> int:
        """Persist ``records`` for the given ``data_type``.

        Returns the number of rows written.
        """
        ...

    @abstractmethod
    def ensure_tables(self) -> None:
        """Create tables in the target database if they do not already exist."""
        ...

    def load_all(self, batches: dict[str, list[dict[str, Any]]]) -> dict[str, int]:
        """Load multiple data types in one call.

        ``batches`` maps data_type -> list of records.  Returns a summary of
        row counts keyed by data_type.
        """
        return {dtype: self.load(dtype, records) for dtype, records in batches.items()}
