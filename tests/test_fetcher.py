"""Tests for high-level Jira fetching functions."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from jira_ingest.fetcher import fetch_issues_for_board


class TestFetchIssuesForBoardDateValidation:
    @pytest.mark.asyncio
    async def test_valid_dates_build_jql(self) -> None:
        client = MagicMock()
        client.get_paginated = AsyncMock(return_value=[])

        await fetch_issues_for_board(client, 1, start_date="2024-01-01", end_date="2024-02-01")

        _endpoint, kwargs = client.get_paginated.call_args
        jql = kwargs["params"]["jql"]
        assert "2024-01-01" in jql
        assert "2024-02-01" in jql

    @pytest.mark.asyncio
    async def test_no_dates_omits_jql(self) -> None:
        client = MagicMock()
        client.get_paginated = AsyncMock(return_value=[])

        await fetch_issues_for_board(client, 1)

        _endpoint, kwargs = client.get_paginated.call_args
        assert "jql" not in kwargs["params"]

    @pytest.mark.asyncio
    async def test_malformed_start_date_raises_before_calling_client(self) -> None:
        client = MagicMock()
        client.get_paginated = AsyncMock(return_value=[])

        with pytest.raises(ValueError, match="start_date"):
            await fetch_issues_for_board(client, 1, start_date="2024-01-01' OR 1=1--")

        client.get_paginated.assert_not_called()

    @pytest.mark.asyncio
    async def test_malformed_end_date_raises_before_calling_client(self) -> None:
        client = MagicMock()
        client.get_paginated = AsyncMock(return_value=[])

        with pytest.raises(ValueError, match="end_date"):
            await fetch_issues_for_board(client, 1, end_date="not-a-date")

        client.get_paginated.assert_not_called()
