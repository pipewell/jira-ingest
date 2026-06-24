"""High-level Jira data fetching functions built on top of JiraClient."""

from __future__ import annotations

import logging
from typing import Any

from jira_ingest.client import JiraClient

logger = logging.getLogger(__name__)

# ── Projects ──────────────────────────────────────────────────────────────────


async def fetch_projects(
    client: JiraClient, project_keys: list[str] | None = None
) -> list[dict[str, Any]]:
    """Fetch all accessible projects, optionally filtered to ``project_keys``."""
    projects: list[dict[str, Any]] = await client.get("/rest/api/2/project")
    if project_keys:
        keys = set(project_keys)
        projects = [p for p in projects if p.get("key") in keys]
        logger.info("Filtered to %d projects: %s", len(projects), sorted(keys))
    else:
        logger.info("Fetched %d projects", len(projects))
    return projects


async def fetch_project_details(client: JiraClient, project_self_url: str) -> dict[str, Any]:
    """Fetch full project details (includes versions/releases and lead) via self URL."""
    # The self URL is absolute; extract the path portion
    from urllib.parse import urlparse

    path = urlparse(project_self_url).path
    result: dict[str, Any] = await client.get(path)
    return result


# ── Boards ────────────────────────────────────────────────────────────────────


async def fetch_boards(client: JiraClient, project_key: str) -> list[dict[str, Any]]:
    """Fetch all boards for a project."""
    return await client.get_paginated(
        "/rest/agile/1.0/board",
        results_key="values",
        params={"projectKeyOrId": project_key},
    )


# ── Issues ────────────────────────────────────────────────────────────────────


async def fetch_issues_for_board(
    client: JiraClient,
    board_id: int,
    start_date: str | None = None,
    end_date: str | None = None,
) -> list[dict[str, Any]]:
    """Fetch all issues for a board, with optional date-range JQL filter.

    Uses ``expand=changelog`` so transition history is included in every issue.
    """
    jql_parts: list[str] = []
    if start_date:
        jql_parts.append(f"(created >= '{start_date}' OR updated >= '{start_date}')")
    if end_date:
        jql_parts.append(f"(created <= '{end_date}' OR updated <= '{end_date}')")
    jql = " AND ".join(jql_parts) if jql_parts else None

    params: dict[str, Any] = {"expand": "changelog"}
    if jql:
        params["jql"] = jql

    issues = await client.get_paginated(
        f"/rest/agile/1.0/board/{board_id}/issue",
        results_key="issues",
        params=params,
        page_size=150,
    )
    logger.debug("Board %d: fetched %d issues", board_id, len(issues))
    return issues
