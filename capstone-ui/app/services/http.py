from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)


class UpstreamServiceError(Exception):
    """Raised when a call to DataPipeline/AiDiagnosis fails after all retries."""


async def request_json(
    *,
    method: str,
    url: str,
    headers: dict[str, str] | None = None,
    json_body: dict[str, Any] | None = None,
    params: dict[str, Any] | None = None,
    timeout: float,
    max_attempts: int,
    retry_statuses: set[int] = frozenset({502, 503, 504}),
) -> httpx.Response:
    """POST/GET with retry on transient failures (5xx / timeout / connection errors).

    Non-retryable statuses (e.g. 400/404) are returned as-is on the first attempt
    so callers can branch on them without this helper swallowing the response.
    """
    last_error: Exception | None = None
    async with httpx.AsyncClient(timeout=timeout) as client:
        for attempt in range(1, max_attempts + 1):
            try:
                response = await client.request(
                    method, url, headers=headers, json=json_body, params=params
                )
                if response.status_code not in retry_statuses:
                    return response
                last_error = UpstreamServiceError(
                    f"{url} returned {response.status_code}"
                )
            except (httpx.TimeoutException, httpx.TransportError) as error:
                last_error = error

            if attempt < max_attempts:
                backoff = min(2 ** (attempt - 1), 30)
                logger.warning(
                    "Retrying %s %s (attempt %s/%s) after %ss: %s",
                    method,
                    url,
                    attempt,
                    max_attempts,
                    backoff,
                    last_error,
                )
                await asyncio.sleep(backoff)

    raise UpstreamServiceError(str(last_error)) from last_error
