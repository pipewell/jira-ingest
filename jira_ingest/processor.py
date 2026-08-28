"""Transform raw Jira API responses into validated records.

Each function returns an async generator of ``(data_type, list[dict])`` tuples
so the pipeline can stream records to the writer without buffering everything
in memory first.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncGenerator
from itertools import islice
from typing import Any

from pydantic import ValidationError

from jira_ingest.client import JiraClient
from jira_ingest.config import Settings
from jira_ingest.fetcher import (
    fetch_boards,
    fetch_issues_for_board,
    fetch_project_details,
    fetch_projects,
)
from jira_ingest.schemas import (
    BoardRecord,
    IssueRecord,
    ProjectRecord,
    ReleaseRecord,
    TransitionRecord,
)
from jira_ingest.utils import clean_whitespace, hash_pii, safe_int, safe_str

logger = logging.getLogger(__name__)

# Transition fields we care about (everything else is ignored)
_TRANSITION_FIELDS = frozenset(
    {
        "status",
        "Sprint",
        "resolution",
        "Story Points",
        "Epic Link",
        "Rank",
        "issuetype",
        "labels",
        "priority",
        "Link",
        "summary",
        "assignee",
    }
)

DataStream = AsyncGenerator[tuple[str, list[dict[str, Any]]], None]


async def stream_all(
    client: JiraClient,
    settings: Settings,
    start_date: str | None = None,
    end_date: str | None = None,
    batch_size: int = 10,
) -> DataStream:
    """Top-level generator: fetch and yield all configured data types.

    Yields ``(data_type, records)`` tuples where ``records`` is a list of
    plain dicts ready to be written by the output layer.
    """
    projects = await fetch_projects(client, settings.project_keys or None)

    def _batches(items: list[Any], n: int) -> list[list[Any]]:
        it = iter(items)
        return [list(islice(it, n)) for _ in range(0, len(items), n) if True]

    batches = list(_batches(projects, batch_size))
    for batch in batches:
        tasks = [
            asyncio.create_task(
                _collect(_process_project(client, settings, project, start_date, end_date))
            )
            for project in batch
        ]
        for coro in asyncio.as_completed(tasks):
            for dtype, records in await coro:
                if records:
                    yield dtype, records


async def _collect(gen: DataStream) -> list[tuple[str, list[dict[str, Any]]]]:
    return [item async for item in gen]


async def _process_project(
    client: JiraClient,
    settings: Settings,
    project: dict[str, Any],
    start_date: str | None,
    end_date: str | None,
) -> DataStream:
    project = clean_whitespace(project)
    project_self = project.get("self", "")

    try:
        details = await fetch_project_details(client, project_self)
    except Exception:
        logger.warning("Could not fetch details for project %s", project.get("key"))
        return

    if not details:
        return

    # ── Project ────────────────────────────────────────────────────────────────
    category = project.get("projectCategory") or {}
    lead = details.get("lead") or {}
    lead_hash = hash_pii(safe_str(lead.get("displayName")), safe_str(lead.get("key")))

    try:
        proj_record = ProjectRecord(
            id=int(project["id"]),
            key=project["key"],
            name=project["name"],
            project_type_key=project.get("projectTypeKey", ""),
            archived=project.get("archived", False),
            project_category_id=safe_str(category.get("id")) or None,
            project_category_name=category.get("name"),
            project_category_description=category.get("description"),
            lead_hash=lead_hash,
        )
        yield "projects", [proj_record.model_dump()]
    except (ValidationError, KeyError, ValueError) as exc:
        logger.error("Project validation failed for %s: %s", project.get("key"), exc)
        return

    # ── Releases ───────────────────────────────────────────────────────────────
    releases: list[dict[str, Any]] = []
    for v in details.get("versions", []):
        try:
            rec = ReleaseRecord(
                release_id=int(v["id"]),
                release_name=v.get("name", ""),
                project_id=proj_record.id,
                project_key=proj_record.key,
                archived=v.get("archived", False),
                released=v.get("released", False),
                release_date=v.get("releaseDate") or None,
                user_release_date=v.get("userReleaseDate") or None,
            )
            releases.append(rec.model_dump())
        except (ValidationError, KeyError) as exc:
            logger.debug("Skipping release %s: %s", v.get("id"), exc)
    if releases:
        yield "releases", releases

    # ── Boards + Issues + Transitions ──────────────────────────────────────────
    boards_raw = await fetch_boards(client, proj_record.key)
    seen_issues: set[str] = set()

    for board in boards_raw:
        try:
            board_id = int(board["id"])
            board_rec = BoardRecord(
                board_id=board_id,
                project_id=proj_record.id,
                name=board.get("name", ""),
                board_type=board.get("type", ""),
            )
            yield "boards", [board_rec.model_dump()]
        except (ValidationError, KeyError, ValueError) as exc:
            logger.debug("Skipping board %s: %s", board.get("id"), exc)
            continue

        issues_raw = await fetch_issues_for_board(client, board_id, start_date, end_date)
        issue_records: list[dict[str, Any]] = []
        transition_records: list[dict[str, Any]] = []

        for issue in issues_raw:
            issue_id = str(issue.get("id", ""))
            if not issue_id or issue_id in seen_issues:
                continue
            seen_issues.add(issue_id)

            fields = issue.get("fields") or {}
            issue_rec, transitions = _extract_issue(
                issue, fields, proj_record, settings.custom_fields
            )
            if issue_rec:
                issue_records.append(issue_rec)
            transition_records.extend(transitions)

        if issue_records:
            yield "issues", issue_records
        if transition_records:
            yield "transitions", transition_records


def _extract_issue(
    issue: dict[str, Any],
    fields: dict[str, Any],
    project: ProjectRecord,
    custom_field_map: dict[str, str],
) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    labels = fields.get("labels") or []
    labels_str = ", ".join(str(lb) for lb in labels if lb)

    status = fields.get("status") or {}
    status_cat = status.get("statusCategory") or {}
    issuetype = fields.get("issuetype") or {}
    priority = fields.get("priority") or {}
    parent = fields.get("parent") or {}
    epic = fields.get("epic") or {}

    # Resolve user-configured custom fields generically
    resolved_custom: dict[str, Any] = {}
    for logical_name, field_id in custom_field_map.items():
        raw = fields.get(field_id)
        if isinstance(raw, dict):
            resolved_custom[logical_name] = raw.get("value") or raw.get("name") or str(raw)
        else:
            resolved_custom[logical_name] = raw

    try:
        rec = IssueRecord(
            id=int(issue["id"]),  # raises ValueError on non-numeric ID -> caught below
            key=issue["key"],
            project_id=project.id,
            project_key=project.key,
            project_name=project.name,
            issuetype_id=safe_int(issuetype.get("id")),
            issue_type=issuetype.get("name"),
            parent_task_id=safe_int(parent.get("id")),
            parent_task_key=parent.get("key"),
            priority=priority.get("name"),
            priority_id=safe_int(priority.get("id")),
            status_id=safe_int(status.get("id")),
            status_name=status.get("name"),
            status_category_id=safe_int(status_cat.get("id")),
            status_category_name=status_cat.get("name"),
            status_category_colour=status_cat.get("colorName"),
            epic_id=safe_int(epic.get("id")),
            epic_key=epic.get("key"),
            epic_name=epic.get("name"),
            epic_summary=safe_str(epic.get("summary")) or None,
            epic_done=epic.get("done") if epic else None,
            summary=fields.get("summary"),
            labels=labels_str or None,
            created=fields.get("created"),
            updated=fields.get("updated"),
            custom_fields=resolved_custom,
        )
    except (ValidationError, KeyError, ValueError) as exc:
        logger.warning("Issue %s validation failed: %s", issue.get("key"), exc)
        return None, []

    transitions = _extract_transitions(issue, rec.id, rec.key)
    return rec.model_dump(), transitions


def _extract_transitions(
    issue: dict[str, Any], issue_id: int, issue_key: str
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for history in (issue.get("changelog") or {}).get("histories", []):
        author = history.get("author") or {}
        account_id = author.get("key") or author.get("accountId")
        hashed = hash_pii(safe_str(author.get("displayName")), safe_str(account_id))
        created = history.get("created")
        transition_id = safe_int(history.get("id"))
        if transition_id is None:
            logger.debug("Skipping changelog history with missing id for issue %s", issue_key)
            continue

        for change in history.get("items", []):
            field = change.get("field")
            if field not in _TRANSITION_FIELDS:
                continue

            if field == "assignee":
                from_str = hash_pii(
                    safe_str(change.get("fromString")), safe_str(change.get("from", ""))
                )
                to_str = hash_pii(safe_str(change.get("toString")), safe_str(change.get("to", "")))
                from_id = to_id = ""
            else:
                from_id = safe_str(change.get("from"))
                from_str = safe_str(change.get("fromString"))
                to_id = safe_str(change.get("to"))
                to_str = safe_str(change.get("toString"))

            try:
                rec = TransitionRecord(
                    transition_id=transition_id,
                    transition_field=field,
                    transition_field_type=change.get("fieldtype", ""),
                    issue_id=issue_id,
                    issue_key=issue_key,
                    hashed_author=hashed,
                    created=created,
                    from_id=from_id or None,
                    from_string=from_str or None,
                    to_id=to_id or None,
                    to_string=to_str or None,
                )
                records.append(rec.model_dump())
            except ValidationError as exc:
                logger.debug("Transition validation failed: %s", exc)

    return records
