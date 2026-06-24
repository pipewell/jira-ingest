"""Tests for the processor: extraction and transformation logic."""

from __future__ import annotations

from jira_ingest.processor import _extract_issue, _extract_transitions
from jira_ingest.schemas import ProjectRecord
from tests.conftest import make_issue


def _project() -> ProjectRecord:
    return ProjectRecord(
        id=10001,
        key="PROJ",
        name="Test Project",
        project_type_key="software",
        archived=False,
    )


class TestExtractIssue:
    def test_extracts_basic_fields(self) -> None:
        issue = make_issue(issue_id="1001", key="PROJ-1", summary="Fix bug")
        rec, _transitions = _extract_issue(issue, issue["fields"], _project(), {})
        assert rec is not None
        assert rec["id"] == 1001
        assert rec["key"] == "PROJ-1"
        assert rec["summary"] == "Fix bug"

    def test_labels_joined_as_string(self) -> None:
        issue = make_issue(labels=["backend", "urgent"])
        rec, _ = _extract_issue(issue, issue["fields"], _project(), {})
        assert rec is not None
        assert "backend" in rec["labels"]
        assert "urgent" in rec["labels"]

    def test_custom_fields_extracted(self) -> None:
        issue = make_issue()
        issue["fields"]["customfield_10100"] = {"value": "feature"}
        rec, _ = _extract_issue(
            issue, issue["fields"], _project(), {"type_of_work": "customfield_10100"}
        )
        assert rec is not None
        assert rec["custom_fields"]["type_of_work"] == "feature"

    def test_custom_field_scalar(self) -> None:
        issue = make_issue()
        issue["fields"]["customfield_10001"] = "Platform"
        rec, _ = _extract_issue(
            issue, issue["fields"], _project(), {"team": "customfield_10001"}
        )
        assert rec is not None
        assert rec["custom_fields"]["team"] == "Platform"

    def test_invalid_id_returns_none(self) -> None:
        issue = {"id": "not-an-int", "key": "PROJ-9", "fields": {}, "changelog": {"histories": []}}
        rec, _ = _extract_issue(issue, issue["fields"], _project(), {})
        assert rec is None


class TestExtractTransitions:
    def test_status_transition_extracted(self) -> None:
        transition_history = [
            {
                "id": "100",
                "created": "2024-02-01T10:00:00.000+0000",
                "author": {"displayName": "Alice", "key": "alice"},
                "items": [
                    {
                        "field": "status",
                        "fieldtype": "jira",
                        "from": "1",
                        "fromString": "To Do",
                        "to": "3",
                        "toString": "In Progress",
                    }
                ],
            }
        ]
        issue = make_issue(transitions=transition_history)
        transitions = _extract_transitions(issue, 1001, "PROJ-1")
        assert len(transitions) == 1
        assert transitions[0]["transition_field"] == "status"
        assert transitions[0]["to_string"] == "In Progress"

    def test_assignee_pii_hashed(self) -> None:
        history = [
            {
                "id": "101",
                "created": "2024-02-01T10:00:00.000+0000",
                "author": {"displayName": "Bob", "key": "bob"},
                "items": [
                    {
                        "field": "assignee",
                        "fieldtype": "jira",
                        "from": "alice-key",
                        "fromString": "Alice Smith",
                        "to": "bob-key",
                        "toString": "Bob Jones",
                    }
                ],
            }
        ]
        issue = make_issue(transitions=history)
        transitions = _extract_transitions(issue, 1001, "PROJ-1")
        assert len(transitions) == 1
        assert "Alice" not in transitions[0].get("from_string", "")
        assert "Bob" not in transitions[0].get("to_string", "")
        # Hashes are hex strings of length 64
        assert len(transitions[0]["from_string"]) == 64
        assert len(transitions[0]["to_string"]) == 64

    def test_unknown_fields_excluded(self) -> None:
        history = [
            {
                "id": "102",
                "created": "2024-02-01T10:00:00.000+0000",
                "author": {"displayName": "Carol", "key": "carol"},
                "items": [
                    {
                        "field": "Attachment",  # not in _TRANSITION_FIELDS
                        "fieldtype": "jira",
                        "from": None,
                        "fromString": None,
                        "to": "12345",
                        "toString": "file.pdf",
                    }
                ],
            }
        ]
        issue = make_issue(transitions=history)
        transitions = _extract_transitions(issue, 1001, "PROJ-1")
        assert len(transitions) == 0

    def test_empty_changelog(self) -> None:
        issue = make_issue(transitions=[])
        transitions = _extract_transitions(issue, 1001, "PROJ-1")
        assert transitions == []
