"""Tests for Pydantic v2 schemas."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from jira_ingest.schemas import IssueRecord, ProjectRecord, ReleaseRecord, TransitionRecord


class TestProjectRecord:
    def test_valid(self) -> None:
        p = ProjectRecord(
            id=10001,
            key="PROJ",
            name="Test Project",
            project_type_key="software",
            archived=False,
        )
        assert p.key == "PROJ"

    def test_missing_required_field(self) -> None:
        with pytest.raises(ValidationError):
            ProjectRecord(key="PROJ", name="x", project_type_key="software", archived=False)  # type: ignore[call-arg]


class TestReleaseRecord:
    def test_release_date_formats(self) -> None:
        r = ReleaseRecord(
            release_id=1,
            release_name="v1.0",
            project_id=10001,
            project_key="PROJ",
            archived=False,
            released=True,
            release_date="2024-03-01",
        )
        assert r.release_date is not None
        assert r.release_date.year == 2024

    def test_slash_date_format(self) -> None:
        r = ReleaseRecord(
            release_id=2,
            release_name="v2",
            project_id=10001,
            project_key="PROJ",
            archived=False,
            released=False,
            user_release_date="15/Mar/24",
        )
        assert r.user_release_date is not None
        assert r.user_release_date.month == 3

    def test_invalid_date_raises(self) -> None:
        with pytest.raises(ValidationError):
            ReleaseRecord(
                release_id=3,
                release_name="v3",
                project_id=10001,
                project_key="PROJ",
                archived=False,
                released=False,
                release_date="not-a-date",
            )


class TestIssueRecord:
    def test_datetime_parsing(self) -> None:
        issue = IssueRecord(
            id=1001,
            key="PROJ-1",
            project_id=10001,
            project_key="PROJ",
            project_name="Test",
            created="2024-01-15T09:00:00.000+0000",
            updated="2024-03-01T14:30:00.000+0000",
        )
        assert issue.created is not None
        assert issue.created.year == 2024

    def test_none_string_ids_coerced(self) -> None:
        issue = IssueRecord(
            id=1001,
            key="PROJ-1",
            project_id=10001,
            project_key="PROJ",
            project_name="Test",
            status_id="None",
            priority_id="",
        )
        assert issue.status_id is None
        assert issue.priority_id is None

    def test_custom_fields_stored(self) -> None:
        issue = IssueRecord(
            id=1,
            key="X-1",
            project_id=1,
            project_key="X",
            project_name="X",
            custom_fields={"type_of_work": "feature"},
        )
        assert issue.custom_fields["type_of_work"] == "feature"


class TestTransitionRecord:
    def test_valid_transition(self) -> None:
        t = TransitionRecord(
            transition_id=100,
            transition_field="status",
            transition_field_type="jira",
            issue_id=1001,
            issue_key="PROJ-1",
            hashed_author="abc123",
            created="2024-02-01T10:00:00.000+0000",
            from_id="1",
            from_string="To Do",
            to_id="3",
            to_string="In Progress",
        )
        assert t.to_string == "In Progress"
        assert t.created is not None
