from __future__ import annotations

import os

from app.services.http import UpstreamServiceError, request_json


def _base_url() -> str:
    url = os.getenv("DATA_PIPELINE_BASE_URL", "").strip().rstrip("/")
    if not url:
        raise UpstreamServiceError("DATA_PIPELINE_BASE_URL is not configured")
    return url


def _timeout() -> float:
    return float(os.getenv("FASTAPI_TIMEOUT", "60"))


async def start_pipeline_run(tenant_id: str, pipeline_type: str = "") -> dict:
    """Calls DataPipeline's already-async POST /api/v1/pipeline/run; returns immediately
    with a run_id while the real Spark job keeps running in the background."""
    response = await request_json(
        method="POST",
        url=f"{_base_url()}/api/v1/pipeline/run",
        headers={"X-Tenant-ID": tenant_id},
        json_body={"pipeline_type": pipeline_type},
        timeout=_timeout(),
        max_attempts=3,
    )
    if response.status_code != 200:
        raise UpstreamServiceError(
            f"pipeline/run failed: {response.status_code} {response.text}"
        )
    return response.json()


async def get_pipeline_status(tenant_id: str, run_id: str) -> dict | None:
    """Returns None if no status record exists yet (pipeline still running)."""
    response = await request_json(
        method="GET",
        url=f"{_base_url()}/api/v1/pipeline/status",
        headers={"X-Tenant-ID": tenant_id},
        params={"run_id": run_id},
        timeout=_timeout(),
        max_attempts=3,
    )
    if response.status_code == 404:
        return None
    if response.status_code != 200:
        raise UpstreamServiceError(
            f"pipeline/status failed: {response.status_code} {response.text}"
        )
    return response.json()
