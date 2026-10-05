from __future__ import annotations

import os
from typing import Any

from app.services.http import UpstreamServiceError, request_json


def _base_url() -> str:
    url = os.getenv("AI_DIAGNOSIS_BASE_URL", "").strip().rstrip("/")
    if not url:
        raise UpstreamServiceError("AI_DIAGNOSIS_BASE_URL is not configured")
    return url


def _timeout() -> float:
    return float(os.getenv("FASTAPI_TIMEOUT", "60"))


async def diagnose(
    *,
    tenant_id: str,
    incident_id: str,
    adapter: str,
    pipeline: str,
    status: str | None = None,
    failed_stage: str | None = None,
    failed_task: str | None = None,
    metadata: dict[str, Any] | None = None,
    details: dict[str, Any] | None = None,
) -> dict:
    """Calls AiDiagnosis's combined Diagnosis+Remediation LangGraph (Agent 2 + 3 engine).

    AiDiagnosis already does RCA grounded in evidence/logs/Chroma history and proposes
    (but never executes) remediation actions - reused here as-is per the agreed plan.
    """
    response = await request_json(
        method="POST",
        url=f"{_base_url()}/api/v1/incidents/ai-diagnosis",
        json_body={
            "tenant_id": tenant_id,
            "incident_id": incident_id,
            "adapter": adapter,
            "pipeline": pipeline,
            "status": status,
            "failed_stage": failed_stage,
            "failed_task": failed_task,
            "metadata": metadata or {},
            "details": details or {},
        },
        timeout=_timeout(),
        max_attempts=2,
    )
    if response.status_code != 200:
        raise UpstreamServiceError(
            f"ai-diagnosis failed: {response.status_code} {response.text}"
        )
    return response.json()
