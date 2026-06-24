"""Async Jira HTTP client supporting Data Center (Bearer + mTLS) and Cloud (Basic Auth)."""

from __future__ import annotations

import asyncio
import base64
import logging
import ssl
import time
from typing import Any
from urllib.parse import urljoin

import aiohttp
from aiocache import SimpleMemoryCache
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from jira_ingest.config import Settings

logger = logging.getLogger(__name__)

_RETRYABLE = (
    aiohttp.ClientError,
    aiohttp.ServerConnectionError,
    asyncio.TimeoutError,
    ConnectionError,
)


class JiraClient:
    """Unified async Jira client.

    Supports both Jira Data Center (Bearer PAT, optional mTLS) and
    Jira Cloud (Basic Auth with email + API token). The mode is determined
    by ``settings.mode``.

    Usage::

        async with JiraClient(settings) as client:
            projects = await client.get("/rest/api/2/project")
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._base_url = settings.url
        self._semaphore = asyncio.Semaphore(settings.max_concurrent_requests)
        self._timeout = aiohttp.ClientTimeout(total=settings.request_timeout_seconds)
        self._cache: SimpleMemoryCache = SimpleMemoryCache()
        self._pem_path: str | None = settings.resolve_pem_path()
        self._session: aiohttp.ClientSession | None = None

    async def __aenter__(self) -> JiraClient:
        self._session = aiohttp.ClientSession(
            headers=self._build_headers(),
            timeout=self._timeout,
        )
        return self

    async def __aexit__(self, *_: Any) -> None:
        if self._session:
            await self._session.close()
        await self._cache.clear()

    async def get(self, endpoint: str, params: dict[str, Any] | None = None) -> Any:
        """GET ``endpoint`` with caching and exponential-backoff retry."""
        cache_key = f"GET:{endpoint}:{params}"
        cached = await self._cache.get(cache_key)
        if cached is not None:
            return cached

        result = await self._get_with_retry(endpoint, params or {})
        await self._cache.set(cache_key, result, ttl=self._settings.cache_ttl_seconds)
        return result

    async def get_paginated(
        self,
        endpoint: str,
        results_key: str,
        params: dict[str, Any] | None = None,
        page_size: int = 100,
    ) -> list[Any]:
        """Fetch all pages of a paginated Jira endpoint.

        Handles both ``startAt``/``total`` style (Jira REST API) and
        ``isLast`` style (Agile API boards).
        """
        all_results: list[Any] = []
        start_at = 0
        base_params = dict(params or {})

        while True:
            page_params = {**base_params, "startAt": start_at, "maxResults": page_size}
            response = await self.get(endpoint, page_params)

            if not response:
                break

            page_results = response.get(results_key, [])
            all_results.extend(page_results)

            total = response.get("total")
            is_last = response.get("isLast", False)

            if is_last or not page_results:
                break
            if total is not None and start_at + len(page_results) >= total:
                break

            start_at += len(page_results)

        return all_results

    def _build_headers(self) -> dict[str, str]:
        if self._settings.mode == "cloud":
            encoded = base64.b64encode(
                f"{self._settings.email}:{self._settings.api_token}".encode()
            ).decode()
            return {"Authorization": f"Basic {encoded}", "Accept": "application/json"}
        return {
            "Authorization": f"Bearer {self._settings.api_token}",
            "Accept": "application/json",
        }

    def _build_ssl_context(self) -> ssl.SSLContext | bool:
        if self._settings.mode == "dc" and self._pem_path:
            ctx = ssl.create_default_context()
            ctx.load_cert_chain(certfile=self._pem_path)
            return ctx
        return True

    async def _get_with_retry(self, endpoint: str, params: dict[str, Any]) -> Any:
        settings = self._settings

        @retry(
            wait=wait_exponential(multiplier=1, min=4, max=300),
            stop=stop_after_attempt(settings.max_retry_attempts),
            retry=retry_if_exception_type(_RETRYABLE),
            before_sleep=before_sleep_log(logger, logging.WARNING),
        )
        async def _do() -> Any:
            assert self._session is not None, "Call within async context manager"
            url = urljoin(self._base_url + "/", endpoint.lstrip("/"))
            ssl_ctx = self._build_ssl_context()

            async with self._semaphore:
                t0 = time.monotonic()
                async with self._session.get(url, params=params, ssl=ssl_ctx) as resp:
                    elapsed = time.monotonic() - t0
                    logger.debug("GET %s -> %d (%.2fs)", url, resp.status, elapsed)
                    resp.raise_for_status()
                    return await resp.json()

        return await _do()


def create_client(settings: Settings) -> JiraClient:
    """Factory: returns a ``JiraClient`` configured for the given settings."""
    return JiraClient(settings)
