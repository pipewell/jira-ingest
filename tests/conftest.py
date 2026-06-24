"""Shared fixtures for jira-ingest tests."""

from __future__ import annotations


def make_issue(
    issue_id: str = "1001",
    key: str = "PROJ-1",
    summary: str = "Test issue",
    status_name: str = "In Progress",
    status_id: str = "3",
    status_category: dict | None = None,
    labels: list[str] | None = None,
    transitions: list[dict] | None = None,
) -> dict:
    return {
        "id": issue_id,
        "key": key,
        "fields": {
            "summary": summary,
            "labels": labels or [],
            "created": "2024-01-15T09:00:00.000+0000",
            "updated": "2024-03-01T14:30:00.000+0000",
            "status": {
                "id": status_id,
                "name": status_name,
                "statusCategory": status_category
                or {
                    "id": "4",
                    "name": "In Progress",
                    "colorName": "yellow",
                },
            },
            "issuetype": {"id": "1", "name": "Story"},
            "priority": {"id": "2", "name": "Medium"},
            "parent": {},
            "epic": {},
        },
        "changelog": {"histories": transitions or []},
    }


def make_project(project_id: str = "10001", key: str = "PROJ") -> dict:
    return {
        "id": project_id,
        "key": key,
        "name": "Test Project",
        "projectTypeKey": "software",
        "archived": False,
        "self": f"https://jira.example.com/rest/api/2/project/{project_id}",
    }


def make_project_details(project_id: str = "10001", versions: list | None = None) -> dict:
    return {
        "id": project_id,
        "versions": versions or [],
        "lead": {"displayName": "Jane Doe", "key": "jdoe"},
    }


def make_board(board_id: int = 1, project_id: str = "10001") -> dict:
    return {
        "id": board_id,
        "name": "PROJ board",
        "type": "scrum",
        "location": {"projectId": project_id},
    }
