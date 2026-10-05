"""Exposes the latest retail-spark-pipeline run status, read from its S3 log records,
and starts new runs of the existing retail-spark-pipeline CLI job."""

import json
import queue
import re
import subprocess
import threading
import time

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from fastapi import APIRouter, Depends, Header, HTTPException, Query, status
from pydantic import BaseModel

from app.config import settings

router = APIRouter(prefix="/api/v1/pipeline", tags=["pipeline"])

_RUN_ID_PATTERN = re.compile(r"^\d{8}_\d{6}_[0-9a-f]{6}$")


class PipelineRunRequest(BaseModel):
    pipeline_type: str = ""


def require_tenant_header(
    x_tenant_id: str | None = Header(None, alias="X-Tenant-ID"),
) -> str:
    """Pipeline run/status only need a non-blank tenant id for S3 paths.

    Unlike the retail-data endpoints, this does not require membership in the
    retail mock's specific seeded tenant list -- any caller-supplied tenant
    identifier is valid here (matching how AI Diagnosis's IncidentTrigger
    accepts any non-blank tenant_id).
    """
    tenant_id = (x_tenant_id or "").strip()
    if not tenant_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": {"code": "MISSING_TENANT_ID", "message": "X-Tenant-ID header is required"}},
        )
    return tenant_id


def _latest_run_id(s3, tenant_id: str) -> str:
    """Find the most recent run_id under logs/pipeline/tenant_id=<tenant>/ (run_id is timestamp-sortable)."""
    prefix = f"logs/pipeline/tenant_id={tenant_id}/"
    response = s3.list_objects_v2(Bucket=settings.S3_BUCKET, Prefix=prefix, Delimiter="/")
    run_prefixes = [p["Prefix"] for p in response.get("CommonPrefixes", [])]
    if not run_prefixes:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": {"code": "NO_PIPELINE_RUNS", "message": f"No pipeline runs found for tenant {tenant_id}"}},
        )
    latest_prefix = sorted(run_prefixes)[-1]
    return latest_prefix[len(prefix):].rstrip("/").removeprefix("run_id=")


def _latest_status_key(s3, tenant_id: str, run_id: str) -> str:
    prefix = f"logs/pipeline/tenant_id={tenant_id}/run_id={run_id}/pipeline_status_"
    response = s3.list_objects_v2(Bucket=settings.S3_BUCKET, Prefix=prefix)
    keys = [obj["Key"] for obj in response.get("Contents", [])]
    if not keys:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": {
                    "code": "PIPELINE_STATUS_NOT_FOUND",
                    "message": f"No pipeline_status record found for tenant {tenant_id}, run_id {run_id}",
                }
            },
        )
    return sorted(keys)[-1]


@router.get("/status")
async def get_pipeline_status(
    tenant_id: str = Depends(require_tenant_header),
    run_id: str = Query(None, description="Specific run_id to look up; defaults to the most recent run"),
):
    """Return the overall SUCCESS/FAILED status of the latest (or a specific) pipeline run."""
    if not settings.S3_BUCKET:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"error": {"code": "S3_NOT_CONFIGURED", "message": "S3_BUCKET is not configured"}},
        )

    s3 = boto3.client("s3", region_name=settings.AWS_REGION)
    try:
        effective_run_id = run_id or _latest_run_id(s3, tenant_id)
        status_key = _latest_status_key(s3, tenant_id, effective_run_id)
        obj = s3.get_object(Bucket=settings.S3_BUCKET, Key=status_key)
        record = json.loads(obj["Body"].read())
    except HTTPException:
        raise
    except (BotoCoreError, ClientError) as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail={"error": {"code": "S3_READ_FAILURE", "message": str(exc)}},
        )

    return {
        "tenant_id": record.get("tenant_id", tenant_id),
        "run_id": record.get("run_id", effective_run_id),
        "status": record.get("overall_status"),
    }


def _launch_pipeline_run(tenant_id: str) -> str:
    """Start the existing retail-spark-pipeline CLI job and return its self-generated run_id.

    The job only writes its pipeline_status record to S3 once it finishes, so this
    just waits long enough to observe the run_id it prints on startup, then lets the
    job continue in the background; GET /status is the source of truth for outcome.
    """
    try:
        process = subprocess.Popen(
            [
                settings.RETAIL_PIPELINE_PYTHON,
                "run_pipeline.py",
                "--tenant-id",
                tenant_id,
                "--mode",
                "full",
            ],
            cwd=settings.RETAIL_PIPELINE_DIR,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
    except OSError as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail={"error": {"code": "PIPELINE_LAUNCH_FAILED", "message": str(exc)}},
        )

    lines: "queue.Queue[str]" = queue.Queue()

    def _read_stdout() -> None:
        try:
            for line in process.stdout:
                lines.put(line)
        except Exception:  # noqa: BLE001 - best-effort background drain
            pass

    threading.Thread(target=_read_stdout, daemon=True).start()

    run_id = ""
    saw_run_id_label = False
    deadline = time.monotonic() + settings.PIPELINE_RUN_ID_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        remaining = max(0.1, deadline - time.monotonic())
        try:
            line = lines.get(timeout=remaining).strip()
        except queue.Empty:
            if process.poll() is not None:
                break
            continue
        if line == "Run ID:":
            saw_run_id_label = True
            continue
        if saw_run_id_label and _RUN_ID_PATTERN.fullmatch(line):
            run_id = line
            break

    if not run_id:
        process.kill()
        raise HTTPException(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            detail={
                "error": {
                    "code": "PIPELINE_RUN_ID_TIMEOUT",
                    "message": "Timed out waiting for the pipeline job to report its run_id",
                }
            },
        )
    return run_id


@router.post("/run")
def start_pipeline_run(
    body: PipelineRunRequest,
    tenant_id: str = Depends(require_tenant_header),
):
    """Start a real retail-spark-pipeline run for this tenant; outcome is read via GET /status."""
    run_id = _launch_pipeline_run(tenant_id)
    return {
        "tenant_id": tenant_id,
        "run_id": run_id,
        "pipeline_type": body.pipeline_type,
        "status": "RUNNING",
        "accepted": True,
    }
