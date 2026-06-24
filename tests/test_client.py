"""Tests for JiraClient: headers, pagination, caching."""

from __future__ import annotations

import base64
from unittest.mock import AsyncMock, patch

import pytest

from jira_ingest.client import JiraClient
from jira_ingest.config import Settings


def cloud_settings(**overrides: object) -> Settings:
    return Settings.model_validate(
        {
            "url": "https://jira.example.com",
            "api_token": "mytoken",
            "mode": "cloud",
            "email": "user@example.com",
            **overrides,
        }
    )


def dc_settings(**overrides: object) -> Settings:
    return Settings.model_validate(
        {
            "url": "https://jira.internal.com",
            "api_token": "dctoken",
            "mode": "dc",
            **overrides,
        }
    )


class TestHeaders:
    def test_cloud_basic_auth(self) -> None:
        s = cloud_settings()
        client = JiraClient(s)
        headers = client._build_headers()
        assert "Basic" in headers["Authorization"]
        expected = base64.b64encode(b"user@example.com:mytoken").decode()
        assert headers["Authorization"] == f"Basic {expected}"

    def test_dc_bearer_auth(self) -> None:
        s = dc_settings()
        client = JiraClient(s)
        headers = client._build_headers()
        assert headers["Authorization"] == "Bearer dctoken"


class TestPagination:
    def _make_page(self, items: list, total: int, start_at: int = 0) -> dict:
        return {
            "issues": items,
            "total": total,
            "startAt": start_at,
            "maxResults": len(items),
        }

    @pytest.mark.asyncio
    async def test_single_page(self) -> None:
        s = cloud_settings()
        async with JiraClient(s) as client:
            with patch.object(
                client,
                "get",
                new=AsyncMock(return_value=self._make_page([{"id": 1}, {"id": 2}], total=2)),
            ):
                results = await client.get_paginated("/rest/api/2/search", "issues")
        assert len(results) == 2

    @pytest.mark.asyncio
    async def test_multi_page_fetches_all(self) -> None:
        s = cloud_settings()
        pages = [
            self._make_page([{"id": 1}, {"id": 2}], total=3),
            self._make_page([{"id": 3}], total=3, start_at=2),
        ]

        async with JiraClient(s) as client:
            call_count = 0

            async def mock_get(endpoint: str, params: dict | None = None) -> dict:
                nonlocal call_count
                result = pages[call_count]
                call_count += 1
                return result

            with patch.object(client, "get", new=mock_get):
                results = await client.get_paginated("/rest/api/2/search", "issues", page_size=2)

        assert len(results) == 3
        assert call_count == 2

    @pytest.mark.asyncio
    async def test_is_last_flag_stops_pagination(self) -> None:
        s = cloud_settings()

        async with JiraClient(s) as client:
            with patch.object(
                client,
                "get",
                new=AsyncMock(return_value={"values": [{"id": "board-1"}], "isLast": True}),
            ):
                results = await client.get_paginated("/rest/agile/1.0/board", "values")

        assert len(results) == 1

    @pytest.mark.asyncio
    async def test_empty_response_stops_pagination(self) -> None:
        s = cloud_settings()

        async with JiraClient(s) as client:
            with patch.object(client, "get", new=AsyncMock(return_value={})):
                results = await client.get_paginated("/rest/api/2/search", "issues")

        assert results == []
