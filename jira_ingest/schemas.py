"""Pydantic v2 schemas for Jira data types."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, field_validator


class ProjectRecord(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    id: int
    key: str
    name: str
    project_type_key: str
    archived: bool
    project_category_id: str | None = None
    project_category_name: str | None = None
    project_category_description: str | None = None
    lead_hash: str | None = None


class ReleaseRecord(BaseModel):
    release_id: int
    release_name: str
    project_id: int
    project_key: str
    archived: bool
    released: bool
    release_date: datetime | None = None
    user_release_date: datetime | None = None

    @field_validator("release_date", "user_release_date", mode="before")
    @classmethod
    def parse_date(cls, v: Any) -> Any:
        if not v:
            return None
        for fmt in ("%Y-%m-%d", "%d/%b/%y %H:%M", "%d/%b/%y"):
            try:
                return datetime.strptime(v, fmt)
            except (ValueError, TypeError):
                continue
        raise ValueError(f"Unrecognised date format: {v!r}")


class BoardRecord(BaseModel):
    board_id: int
    project_id: int
    name: str
    board_type: str


class IssueRecord(BaseModel):
    id: int
    key: str
    project_id: int
    project_key: str
    project_name: str
    issuetype_id: int | None = None
    issue_type: str | None = None
    parent_task_id: int | None = None
    parent_task_key: str | None = None
    priority: str | None = None
    priority_id: int | None = None
    status_id: int | None = None
    status_name: str | None = None
    status_category_id: int | None = None
    status_category_name: str | None = None
    status_category_colour: str | None = None
    epic_id: int | None = None
    epic_key: str | None = None
    epic_name: str | None = None
    epic_summary: str | None = None
    epic_done: bool | None = None
    summary: str | None = None
    labels: str | None = None
    created: datetime | None = None
    updated: datetime | None = None
    # Populated from user-configured custom_fields mapping
    custom_fields: dict[str, Any] = {}

    @field_validator("created", "updated", mode="before")
    @classmethod
    def parse_datetime(cls, v: Any) -> Any:
        if not v:
            return None
        # Jira returns ISO 8601 with timezone offset e.g. "2024-01-15T10:30:00.000+0000"
        for fmt in ("%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z"):
            try:
                return datetime.strptime(v, fmt)
            except (ValueError, TypeError):
                continue
        return None

    @field_validator(
        "parent_task_id",
        "epic_id",
        "status_id",
        "issuetype_id",
        "priority_id",
        "status_category_id",
        mode="before",
    )
    @classmethod
    def coerce_int(cls, v: Any) -> Any:
        if v in (None, "", "None"):
            return None
        try:
            return int(v)
        except (ValueError, TypeError):
            return None


class TransitionRecord(BaseModel):
    transition_id: int
    transition_field: str
    transition_field_type: str
    issue_id: int
    issue_key: str
    hashed_author: str
    created: datetime | None = None
    from_id: str | None = None
    from_string: str | None = None
    to_id: str | None = None
    to_string: str | None = None

    @field_validator("created", mode="before")
    @classmethod
    def parse_datetime(cls, v: Any) -> Any:
        if not v:
            return None
        for fmt in ("%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z"):
            try:
                return datetime.strptime(v, fmt)
            except (ValueError, TypeError):
                continue
        return None
