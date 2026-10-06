from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from sqlalchemy.orm import Session

from app.models_db import AgentEventDB, DiagnosisDB, RemediationPlanDB, WorkflowRunDB


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def create_workflow_run(
    db: Session,
    *,
    tenant_id: str,
    platform_id: str,
    pipeline: str,
    idempotency_key: str | None,
    poll_interval_seconds: int,
    deadline_at: datetime,
) -> tuple[WorkflowRunDB, bool]:
    """Return (row, created). If idempotency_key already exists, returns the existing row."""
    if idempotency_key:
        existing = (
            db.query(WorkflowRunDB)
            .filter(WorkflowRunDB.idempotency_key == idempotency_key)
            .one_or_none()
        )
        if existing is not None:
            return existing, False

    now = _now_utc()
    row = WorkflowRunDB(
        workflow_run_id=str(uuid4()),
        tenant_id=tenant_id,
        platform_id=platform_id,
        pipeline=pipeline,
        state="STARTING",
        current_agent="PipelineAgent",
        attempt_count=0,
        poll_interval_seconds=poll_interval_seconds,
        next_poll_at=now,
        deadline_at=deadline_at,
        idempotency_key=idempotency_key,
        started_at=now,
        updated_at=now,
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row, True


def get_workflow_run(db: Session, workflow_run_id: str) -> WorkflowRunDB | None:
    return db.get(WorkflowRunDB, workflow_run_id)


def list_incomplete_workflow_runs(db: Session) -> list[WorkflowRunDB]:
    """Used by the startup reconciliation hook to resume orphaned runs after a restart."""
    return db.query(WorkflowRunDB).filter(WorkflowRunDB.completed_at.is_(None)).all()


def update_workflow_run(
    db: Session,
    workflow_run_id: str,
    **fields: Any,
) -> WorkflowRunDB | None:
    row = db.get(WorkflowRunDB, workflow_run_id)
    if row is None:
        return None
    for key, value in fields.items():
        setattr(row, key, value)
    row.updated_at = _now_utc()
    db.commit()
    db.refresh(row)
    return row


def record_event(
    db: Session,
    *,
    workflow_run_id: str,
    tenant_id: str,
    agent: str,
    event_type: str,
    payload: dict[str, Any] | None = None,
    incident_id: str | None = None,
) -> AgentEventDB | None:
    """Append an audit event. Returns None (no-op) if this event_type was already
    recorded for this workflow_run_id, so duplicate poll ticks never double-trigger
    a downstream agent."""
    existing = (
        db.query(AgentEventDB)
        .filter(
            AgentEventDB.workflow_run_id == workflow_run_id,
            AgentEventDB.event_type == event_type,
        )
        .one_or_none()
    )
    if existing is not None:
        return None

    row = AgentEventDB(
        event_id=str(uuid4()),
        workflow_run_id=workflow_run_id,
        incident_id=incident_id,
        tenant_id=tenant_id,
        agent=agent,
        event_type=event_type,
        payload=payload or {},
        created_at=_now_utc(),
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


def list_events(db: Session, workflow_run_id: str) -> list[AgentEventDB]:
    return (
        db.query(AgentEventDB)
        .filter(AgentEventDB.workflow_run_id == workflow_run_id)
        .order_by(AgentEventDB.created_at.asc())
        .all()
    )


def list_events_for_incident(db: Session, incident_id: str) -> list[AgentEventDB]:
    return (
        db.query(AgentEventDB)
        .filter(AgentEventDB.incident_id == incident_id)
        .order_by(AgentEventDB.created_at.asc())
        .all()
    )


def create_diagnosis(
    db: Session,
    *,
    workflow_run_id: str,
    incident_id: str,
    tenant_id: str,
    failure_location: str,
    root_cause: str,
    confidence: float,
    evidence: list[str],
    reasoning_summary: str,
    similar_incidents: list[dict[str, Any]],
) -> DiagnosisDB:
    row = DiagnosisDB(
        diagnosis_id=str(uuid4()),
        workflow_run_id=workflow_run_id,
        incident_id=incident_id,
        tenant_id=tenant_id,
        failure_location=failure_location,
        root_cause=root_cause,
        confidence=confidence,
        evidence=evidence,
        reasoning_summary=reasoning_summary,
        similar_incidents=similar_incidents,
        created_at=_now_utc(),
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


def get_latest_diagnosis(db: Session, incident_id: str) -> DiagnosisDB | None:
    return (
        db.query(DiagnosisDB)
        .filter(DiagnosisDB.incident_id == incident_id)
        .order_by(DiagnosisDB.created_at.desc())
        .first()
    )


def create_remediation_plan(
    db: Session,
    *,
    workflow_run_id: str,
    incident_id: str,
    tenant_id: str,
    actions: list[dict[str, Any]],
    validation_result: dict[str, Any],
) -> RemediationPlanDB:
    row = RemediationPlanDB(
        plan_id=str(uuid4()),
        workflow_run_id=workflow_run_id,
        incident_id=incident_id,
        tenant_id=tenant_id,
        actions=actions,
        validation_result=validation_result,
        approval_status="PENDING",
        executed=False,
        created_at=_now_utc(),
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


def get_latest_remediation_plan(db: Session, incident_id: str) -> RemediationPlanDB | None:
    return (
        db.query(RemediationPlanDB)
        .filter(RemediationPlanDB.incident_id == incident_id)
        .order_by(RemediationPlanDB.created_at.desc())
        .first()
    )


def set_remediation_decision(
    db: Session,
    *,
    incident_id: str,
    approved: bool,
    decided_by: str,
) -> RemediationPlanDB | None:
    row = get_latest_remediation_plan(db, incident_id)
    if row is None:
        return None
    row.approval_status = "APPROVED" if approved else "REJECTED"
    row.approved_by = decided_by
    row.approved_at = _now_utc()
    db.commit()
    db.refresh(row)
    return row


def history_for_pipeline(
    db: Session,
    *,
    tenant_id: str,
    pipeline: str,
    limit: int = 5,
) -> list[dict[str, Any]]:
    """'What happened the last N times this pipeline failed?' - joins diagnoses +
    remediation_plans via the workflow_runs that failed for this tenant/pipeline."""
    runs = (
        db.query(WorkflowRunDB)
        .filter(
            WorkflowRunDB.tenant_id == tenant_id,
            WorkflowRunDB.pipeline == pipeline,
            WorkflowRunDB.state.in_(
                ["DIAGNOSED", "AWAITING_APPROVAL", "APPROVED", "REJECTED"]
            ),
        )
        .order_by(WorkflowRunDB.started_at.desc())
        .limit(limit)
        .all()
    )
    results: list[dict[str, Any]] = []
    for run in runs:
        diagnosis = (
            db.query(DiagnosisDB)
            .filter(DiagnosisDB.workflow_run_id == run.workflow_run_id)
            .order_by(DiagnosisDB.created_at.desc())
            .first()
        )
        plan = (
            db.query(RemediationPlanDB)
            .filter(RemediationPlanDB.workflow_run_id == run.workflow_run_id)
            .order_by(RemediationPlanDB.created_at.desc())
            .first()
        )
        results.append(
            {
                "workflow_run_id": run.workflow_run_id,
                "incident_id": run.incident_id,
                "started_at": run.started_at.isoformat(),
                "state": run.state,
                "root_cause": diagnosis.root_cause if diagnosis else "",
                "confidence": diagnosis.confidence if diagnosis else 0.0,
                "remediation_actions": plan.actions if plan else [],
                "approval_status": plan.approval_status if plan else "",
            }
        )
    return results
