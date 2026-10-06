"""Agent 1 (Pipeline Agent): starts DataPipeline runs asynchronously, polls for
PASS/FAIL without blocking any FastAPI request, and on FAILED hands off to the
AiDiagnosis service (which plays the combined Agent 2 + Agent 3 role for this MVP).

All progress is persisted to workflow_runs/agent_events on every tick, so a crash
or restart only costs the current poll interval - reconcile_on_startup() resumes
every row that was still in-flight.
"""
from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from langsmith import traceable

from app.database import incident_repository, workflow_repository
from app.database.connection import SessionLocal
from app.models_db import WorkflowRunDB
from app.services import diagnosis_client, pipeline_client
from app.services.http import UpstreamServiceError

logger = logging.getLogger(__name__)

# workflow_run_id -> running asyncio task, so we never spawn two pollers for the same run.
_RUNNING_TASKS: dict[str, asyncio.Task] = {}

# DataPipeline always runs the same retail-spark-pipeline job no matter which
# platform (spark/synapse) is selected in the UI - that job's own New Relic
# telemetry is tagged with a fixed PIPELINE_ID (see retail-spark-pipeline/.env),
# so AiDiagnosis's log lookup must use that SAME constant regardless of the
# UI's pipeline-type label, or it will never find matching logs.
_KNOWN_ADAPTERS = {"spark", "synapse"}


def _telemetry_pipeline_id() -> str:
    return os.getenv("PIPELINE_ID", "retail_data_pipeline")


def _resolve_adapter(platform_id: str) -> str:
    candidate = (platform_id or "").strip().lower()
    return candidate if candidate in _KNOWN_ADAPTERS else "spark"


@dataclass(frozen=True)
class _RowSnapshot:
    """Plain copy of the fields the poller needs, taken while the DB session was
    still open - the ORM row itself can't be read after its session is closed."""

    workflow_run_id: str
    tenant_id: str
    platform_id: str
    pipeline: str
    run_id: str | None
    state: str
    attempt_count: int
    poll_interval_seconds: int
    next_poll_at: datetime | None
    deadline_at: datetime | None
    completed_at: datetime | None
    incident_id: str | None


def _snapshot(row: WorkflowRunDB) -> _RowSnapshot:
    return _RowSnapshot(
        workflow_run_id=row.workflow_run_id,
        tenant_id=row.tenant_id,
        platform_id=row.platform_id,
        pipeline=row.pipeline,
        run_id=row.run_id,
        state=row.state,
        attempt_count=row.attempt_count,
        poll_interval_seconds=row.poll_interval_seconds,
        next_poll_at=row.next_poll_at,
        deadline_at=row.deadline_at,
        completed_at=row.completed_at,
        incident_id=row.incident_id,
    )


def _deterministic_incident_id(workflow_run_id: str) -> str:
    """Stable per-workflow-run id so a crash/restart re-processing the same
    terminal status upserts the same incident instead of creating a duplicate."""
    return f"INC-{workflow_run_id[:8].upper()}"


def _is_mock_mode() -> bool:
    return os.getenv("PIPELINE_AGENT_MOCK", "false").lower() == "true"


def _poll_interval_seconds() -> int:
    if _is_mock_mode():
        return 2
    return int(os.getenv("PIPELINE_POLL_INTERVAL_SECONDS", "30"))


def _max_wait_minutes() -> int:
    if _is_mock_mode():
        return 2
    return int(os.getenv("PIPELINE_MAX_WAIT_MINUTES", "40"))


def start_workflow(
    *,
    tenant_id: str,
    platform_id: str,
    pipeline: str,
    idempotency_key: str | None,
) -> tuple[_RowSnapshot, bool]:
    """Create (or return the existing, if idempotency_key matches) workflow_run row
    and launch its background poller. Returns (row, created)."""
    db = SessionLocal()
    try:
        row, created = workflow_repository.create_workflow_run(
            db,
            tenant_id=tenant_id,
            platform_id=platform_id,
            pipeline=pipeline,
            idempotency_key=idempotency_key,
            poll_interval_seconds=_poll_interval_seconds(),
            deadline_at=datetime.now(timezone.utc) + timedelta(minutes=_max_wait_minutes()),
        )
        snapshot = _snapshot(row)
    finally:
        db.close()

    if created:
        _spawn(snapshot.workflow_run_id)
    return snapshot, created


def reconcile_on_startup() -> int:
    """Resume every workflow_run that was still in-flight when the process last stopped."""
    db = SessionLocal()
    try:
        rows = workflow_repository.list_incomplete_workflow_runs(db)
        workflow_run_ids = [row.workflow_run_id for row in rows]
    finally:
        db.close()

    for workflow_run_id in workflow_run_ids:
        _spawn(workflow_run_id)
    if workflow_run_ids:
        logger.info("Resumed %s in-flight workflow run(s) after restart", len(workflow_run_ids))
    return len(workflow_run_ids)


def _spawn(workflow_run_id: str) -> None:
    if workflow_run_id in _RUNNING_TASKS and not _RUNNING_TASKS[workflow_run_id].done():
        return
    task = asyncio.create_task(_run(workflow_run_id))
    _RUNNING_TASKS[workflow_run_id] = task


async def _run(workflow_run_id: str) -> None:
    try:
        while True:
            row = await asyncio.to_thread(_load_row, workflow_run_id)
            if row is None or row.completed_at is not None:
                return

            if row.state == "STARTING":
                row = await _do_start(workflow_run_id)
                if row is None:
                    return

            now = datetime.now(timezone.utc)
            next_poll_at = row.next_poll_at or now
            delay = max((next_poll_at - now).total_seconds(), 0)
            if delay > 0:
                await asyncio.sleep(delay)

            row = await asyncio.to_thread(_load_row, workflow_run_id)
            if row is None or row.completed_at is not None:
                return

            if row.deadline_at and datetime.now(timezone.utc) >= row.deadline_at:
                diagnosis_args = await asyncio.to_thread(_handle_timeout, workflow_run_id)
                if diagnosis_args is not None:
                    incident_id, payload = diagnosis_args
                    asyncio.create_task(
                        _run_diagnosis_and_remediation(workflow_run_id, incident_id, row, payload)
                    )
                return

            done = await _do_poll_tick(workflow_run_id)
            if done:
                return
    except Exception:  # noqa: BLE001 - last-resort guard so one bad run doesn't kill the loop forever
        logger.exception("Pipeline Agent crashed for workflow_run_id=%s", workflow_run_id)
        await asyncio.to_thread(_mark_error, workflow_run_id, "Unhandled agent error")
    finally:
        _RUNNING_TASKS.pop(workflow_run_id, None)


def _load_row(workflow_run_id: str) -> _RowSnapshot | None:
    db = SessionLocal()
    try:
        row = workflow_repository.get_workflow_run(db, workflow_run_id)
        return _snapshot(row) if row is not None else None
    finally:
        db.close()


@traceable(name="pipeline_agent.start", run_type="chain")
async def _do_start(workflow_run_id: str) -> _RowSnapshot | None:
    row = await asyncio.to_thread(_load_row, workflow_run_id)
    if row is None:
        return None
    try:
        result = await pipeline_client.start_pipeline_run(row.tenant_id)
    except UpstreamServiceError as error:
        diagnosis_args = await asyncio.to_thread(
            _fail_to_start, workflow_run_id, row.tenant_id, row.platform_id, row.pipeline, str(error)
        )
        if diagnosis_args is not None:
            incident_id, payload = diagnosis_args
            asyncio.create_task(
                _run_diagnosis_and_remediation(workflow_run_id, incident_id, row, payload)
            )
        return None

    def _apply() -> _RowSnapshot | None:
        db = SessionLocal()
        try:
            now = datetime.now(timezone.utc)
            updated = workflow_repository.update_workflow_run(
                db,
                workflow_run_id,
                run_id=result.get("run_id"),
                state="RUNNING",
                current_agent="PipelineAgent",
                attempt_count=0,
                next_poll_at=now + timedelta(seconds=row.poll_interval_seconds),
            )
            workflow_repository.record_event(
                db,
                workflow_run_id=workflow_run_id,
                tenant_id=row.tenant_id,
                agent="PipelineAgent",
                event_type="PIPELINE_STARTED",
                payload=result,
            )
            return _snapshot(updated) if updated is not None else None
        finally:
            db.close()

    return await asyncio.to_thread(_apply)


def _fail_to_start(
    workflow_run_id: str, tenant_id: str, platform_id: str, pipeline: str, error_detail: str
) -> tuple[str, dict[str, Any]] | None:
    """Returns (incident_id, payload) to trigger Agent 2/3, or None if this launch
    failure was already processed (restart/duplicate-tick safe)."""
    db = SessionLocal()
    try:
        incident_id = _deterministic_incident_id(workflow_run_id)
        incident_repository.upsert_pipeline_run(
            db,
            incident_id=incident_id,
            tenant_id=tenant_id,
            platform_id=platform_id,
            pipeline=pipeline,
            outcome="FAILED",
            details=f"Pipeline failed to start: {error_detail}",
        )
        workflow_repository.update_workflow_run(
            db,
            workflow_run_id,
            state="FAILED",
            incident_id=incident_id,
            current_agent="DiagnosisAgent",
            error_detail=error_detail,
        )
        payload = {"failure_reason": "pipeline_failed_to_start", "error": error_detail}
        event = workflow_repository.record_event(
            db,
            workflow_run_id=workflow_run_id,
            tenant_id=tenant_id,
            agent="PipelineAgent",
            event_type="PIPELINE_START_FAILED",
            payload=payload,
            incident_id=incident_id,
        )
        return (incident_id, payload) if event is not None else None
    finally:
        db.close()


@traceable(name="pipeline_agent.poll_tick", run_type="chain")
async def _do_poll_tick(workflow_run_id: str) -> bool:
    """Returns True when the workflow has reached a terminal state (caller should stop)."""
    row = await asyncio.to_thread(_load_row, workflow_run_id)
    if row is None or not row.run_id:
        return False

    try:
        status_payload = await pipeline_client.get_pipeline_status(row.tenant_id, row.run_id)
    except UpstreamServiceError as error:
        await asyncio.to_thread(_bump_attempt, workflow_run_id, row, str(error))
        return False

    if status_payload is None:
        await asyncio.to_thread(_bump_attempt, workflow_run_id, row, None)
        return False

    overall_status = status_payload.get("overall_status") or status_payload.get("status")
    if overall_status == "SUCCESS" or overall_status == "PASSED":
        await asyncio.to_thread(_handle_passed, workflow_run_id, row, status_payload)
        return True
    if overall_status == "FAILED":
        diagnosis_args = await asyncio.to_thread(_handle_failed, workflow_run_id, row, status_payload)
        if diagnosis_args is not None:
            incident_id, payload = diagnosis_args
            asyncio.create_task(
                _run_diagnosis_and_remediation(workflow_run_id, incident_id, row, payload)
            )
        return True

    # Unknown/partial payload shape - keep polling until deadline.
    await asyncio.to_thread(_bump_attempt, workflow_run_id, row, None)
    return False


def _bump_attempt(workflow_run_id: str, row: _RowSnapshot, error_detail: str | None) -> None:
    db = SessionLocal()
    try:
        backoff = min(row.poll_interval_seconds * (2 ** min(row.attempt_count, 4)), 60)
        workflow_repository.update_workflow_run(
            db,
            workflow_run_id,
            attempt_count=row.attempt_count + 1,
            next_poll_at=datetime.now(timezone.utc) + timedelta(seconds=backoff),
            error_detail=error_detail,
        )
    finally:
        db.close()


def _handle_timeout(workflow_run_id: str) -> tuple[str, dict[str, Any]] | None:
    """Returns (incident_id, payload) to trigger Agent 2/3, or None if this timeout
    was already processed."""
    db = SessionLocal()
    try:
        row = workflow_repository.get_workflow_run(db, workflow_run_id)
        if row is None:
            return None
        incident_id = _deterministic_incident_id(workflow_run_id)
        incident_repository.upsert_pipeline_run(
            db,
            incident_id=incident_id,
            tenant_id=row.tenant_id,
            platform_id=row.platform_id,
            pipeline=row.pipeline,
            outcome="FAILED",
            details=f"Pipeline run {row.run_id} did not report status within the deadline",
        )
        workflow_repository.update_workflow_run(
            db,
            workflow_run_id,
            state="FAILED",
            incident_id=incident_id,
            current_agent="DiagnosisAgent",
        )
        payload = {"failure_reason": "pipeline_timed_out", "run_id": row.run_id}
        event = workflow_repository.record_event(
            db,
            workflow_run_id=workflow_run_id,
            tenant_id=row.tenant_id,
            agent="PipelineAgent",
            event_type="PIPELINE_TIMED_OUT",
            payload=payload,
            incident_id=incident_id,
        )
        return (incident_id, payload) if event is not None else None
    finally:
        db.close()


def _mark_error(workflow_run_id: str, error_detail: str) -> None:
    db = SessionLocal()
    try:
        workflow_repository.update_workflow_run(
            db,
            workflow_run_id,
            state="ERROR",
            error_detail=error_detail,
            completed_at=datetime.now(timezone.utc),
        )
    finally:
        db.close()


def _handle_passed(workflow_run_id: str, row: _RowSnapshot, status_payload: dict[str, Any]) -> None:
    db = SessionLocal()
    try:
        incident_id = _deterministic_incident_id(workflow_run_id)
        incident_repository.upsert_pipeline_run(
            db,
            incident_id=incident_id,
            tenant_id=row.tenant_id,
            platform_id=row.platform_id,
            pipeline=row.pipeline,
            outcome="PASSED",
            details="Pipeline run succeeded",
        )
        workflow_repository.update_workflow_run(
            db,
            workflow_run_id,
            state="PASSED",
            incident_id=incident_id,
            completed_at=datetime.now(timezone.utc),
        )
        workflow_repository.record_event(
            db,
            workflow_run_id=workflow_run_id,
            tenant_id=row.tenant_id,
            agent="PipelineAgent",
            event_type="PIPELINE_PASSED",
            payload=status_payload,
            incident_id=incident_id,
        )
    finally:
        db.close()


def _handle_failed(
    workflow_run_id: str, row: _RowSnapshot, status_payload: dict[str, Any]
) -> tuple[str, dict[str, Any]] | None:
    """Returns (incident_id, payload) to trigger Agent 2/3, or None if this failure
    was already processed (idempotency gate via agent_events)."""
    db = SessionLocal()
    try:
        incident_id = _deterministic_incident_id(workflow_run_id)
        incident_repository.upsert_pipeline_run(
            db,
            incident_id=incident_id,
            tenant_id=row.tenant_id,
            platform_id=row.platform_id,
            pipeline=row.pipeline,
            outcome="FAILED",
            details=str(status_payload),
        )
        workflow_repository.update_workflow_run(
            db,
            workflow_run_id,
            state="FAILED",
            incident_id=incident_id,
            current_agent="DiagnosisAgent",
        )
        # Idempotency gate: record_event returns None if PIPELINE_FAILED was already
        # recorded for this workflow_run_id (e.g. a previous tick before a restart).
        event = workflow_repository.record_event(
            db,
            workflow_run_id=workflow_run_id,
            tenant_id=row.tenant_id,
            agent="PipelineAgent",
            event_type="PIPELINE_FAILED",
            payload=status_payload,
            incident_id=incident_id,
        )
        return (incident_id, status_payload) if event is not None else None
    finally:
        db.close()


@traceable(name="pipeline_agent.diagnosis_and_remediation", run_type="chain")
async def _run_diagnosis_and_remediation(
    workflow_run_id: str,
    incident_id: str,
    row: _RowSnapshot,
    status_payload: dict[str, Any],
) -> None:
    """Agent 2 (Diagnosis) + Agent 3 (Remediation): one HTTP call to AiDiagnosis's
    existing combined LangGraph, split into two DB rows + two sequential events so the
    audit trail and UI can treat them as distinct agent handoffs.

    Decorated with @traceable so this handoff shows up in LangSmith alongside the
    AiDiagnosis graph trace it triggers, correlated by tenant_id/incident_id (both
    captured automatically as traced function inputs)."""
    try:
        response = await diagnosis_client.diagnose(
            tenant_id=row.tenant_id,
            incident_id=incident_id,
            adapter=_resolve_adapter(row.platform_id),
            pipeline=_telemetry_pipeline_id(),
            status="FAILED",
            failed_stage=status_payload.get("failed_stage"),
            details=status_payload,
        )
    except UpstreamServiceError as error:
        await asyncio.to_thread(_mark_diagnosis_failed, workflow_run_id, incident_id, row.tenant_id, str(error))
        return

    await asyncio.to_thread(
        _persist_diagnosis_and_remediation, workflow_run_id, incident_id, row.tenant_id, response
    )


def _mark_diagnosis_failed(workflow_run_id: str, incident_id: str, tenant_id: str, error_detail: str) -> None:
    db = SessionLocal()
    try:
        workflow_repository.update_workflow_run(
            db,
            workflow_run_id,
            state="ERROR",
            error_detail=error_detail,
            completed_at=datetime.now(timezone.utc),
        )
        workflow_repository.record_event(
            db,
            workflow_run_id=workflow_run_id,
            tenant_id=tenant_id,
            agent="DiagnosisAgent",
            event_type="DIAGNOSIS_FAILED",
            payload={"error": error_detail},
            incident_id=incident_id,
        )
    finally:
        db.close()


def _persist_diagnosis_and_remediation(
    workflow_run_id: str, incident_id: str, tenant_id: str, response: dict[str, Any]
) -> None:
    db = SessionLocal()
    try:
        workflow_repository.create_diagnosis(
            db,
            workflow_run_id=workflow_run_id,
            incident_id=incident_id,
            tenant_id=tenant_id,
            failure_location=response.get("failure_location", ""),
            root_cause=response.get("root_cause", ""),
            confidence=response.get("confidence", 0.0),
            evidence=response.get("evidence", []),
            reasoning_summary=response.get("message_for_ui", ""),
            similar_incidents=response.get("similar_incidents", []),
        )
        incident_repository.attach_diagnosis_result(db, incident_id=incident_id, diagnosis_payload=response)
        workflow_repository.record_event(
            db,
            workflow_run_id=workflow_run_id,
            tenant_id=tenant_id,
            agent="DiagnosisAgent",
            event_type="DIAGNOSIS_COMPLETED",
            payload={"root_cause": response.get("root_cause", ""), "confidence": response.get("confidence", 0.0)},
            incident_id=incident_id,
        )

        workflow_repository.create_remediation_plan(
            db,
            workflow_run_id=workflow_run_id,
            incident_id=incident_id,
            tenant_id=tenant_id,
            actions=response.get("remedies", []),
            # AiDiagnosis's safety-validation step runs internally but isn't surfaced
            # over HTTP today; recorded empty here rather than fabricated.
            validation_result={},
        )
        incident_repository.mark_awaiting_approval(db, incident_id)
        workflow_repository.record_event(
            db,
            workflow_run_id=workflow_run_id,
            tenant_id=tenant_id,
            agent="RemediationAgent",
            event_type="REMEDIATION_PLAN_READY",
            payload={"actions": response.get("remedies", [])},
            incident_id=incident_id,
        )

        workflow_repository.update_workflow_run(
            db,
            workflow_run_id,
            state="AWAITING_APPROVAL",
            current_agent="Human",
            completed_at=datetime.now(timezone.utc),
        )
    finally:
        db.close()
