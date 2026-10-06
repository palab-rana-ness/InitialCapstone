from fastapi import APIRouter, Depends, Header, HTTPException
from sqlalchemy.orm import Session

from app.agents import pipeline_agent
from app.database import incident_repository, workflow_repository
from app.database.connection import get_db
from app.incident_workflow import (
    get_incident_row,
    ingest_failure,
    list_dashboard_incidents,
    mark_agent_invoked,
    process_agent_result,
)
from app.services.langgraph_service import langgraph_service
from app.services.notification_service import notification_service
from app.workflow_schemas import (
    AgentInvokeRequest,
    AgentResultRequest,
    DiagnosisResultRequest,
    FailureIngestRequest,
    PipelineRunIngestRequest,
    PipelineRunStartRequest,
    RemediationDecisionRequest,
    StandardApiResponse,
    response_ok,
)

router = APIRouter(prefix="/api/v1/incidents", tags=["IncidentFlow"])


def _require_incident(
    db: Session,
    incident_id: str,
    tenant_id: str | None = None,
    platform_id: str | None = None,
):
    row = get_incident_row(db, incident_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Incident not found")
    if tenant_id and row.tenant_id != tenant_id:
        raise HTTPException(status_code=404, detail="Incident not found")
    if platform_id and row.platform_id != platform_id:
        raise HTTPException(status_code=404, detail="Incident not found")
    return row


def _incident_payload(db: Session, incident_id: str) -> dict:
    return incident_repository.payload_map_by_incident_ids(db, [incident_id]).get(
        incident_id, {}
    )


def _incident_data(row) -> dict:
    return {
        "incident_id": row.incident_id,
        "tenant_id": row.tenant_id,
        "platform_id": row.platform_id,
        "pipeline": row.pipeline,
        "severity": row.severity,
        "status": row.status,
        "problem": row.problem,
        "source": row.source,
        "created_at": row.created_at.isoformat(),
        "updated_at": row.updated_at.isoformat(),
    }


@router.post("/failures", response_model=StandardApiResponse, status_code=202)
def receive_new_relic_failure(
    body: FailureIngestRequest,
    db: Session = Depends(get_db),
):
    row, created = ingest_failure(db, body)
    notification = notification_service.notify_human(
        incident_id=row.incident_id,
        title="Incident detected",
        message="A new failure was stored and requires human review.",
        context={
            "status": row.status,
            "severity": row.severity,
            "source": row.source,
        },
    )
    return response_ok(
        "Failure received from New Relic and stored in PostgreSQL.",
        {
            "incident_id": row.incident_id,
            "created": created,
            "status": row.status,
            "notification": notification,
        },
    )


@router.get("", response_model=StandardApiResponse)
def get_dashboard_incidents(
    tenant_id: str | None = None,
    platform_id: str | None = None,
    status: str | None = None,
    severity: str | None = None,
    limit: int = 50,
    offset: int = 0,
    db: Session = Depends(get_db),
):
    items = list_dashboard_incidents(
        db,
        tenant_id=tenant_id,
        platform_id=platform_id,
        status=status,
        severity=severity,
        limit=limit,
        offset=offset,
    )
    return response_ok(
        "Dashboard incidents fetched from PostgreSQL.",
        {
            "items": items,
            "count": len(items),
            "filters": {
                "tenant_id": tenant_id,
                "platform_id": platform_id,
                "status": status,
                "severity": severity,
                "limit": limit,
                "offset": offset,
            },
        },
    )


@router.post("/pipeline-run/start", response_model=StandardApiResponse, status_code=202)
async def start_pipeline_run(
    body: PipelineRunStartRequest,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
):
    """Agent 1 (Pipeline Agent): starts the DataPipeline run and returns immediately.

    The Reflex UI calls this instead of DataPipeline directly - all orchestration
    (polling, failure handoff to Agent 2/3) happens server-side in this backend.

    Must run on the event loop thread (not FastAPI's worker threadpool) because it
    schedules the background poller via asyncio.create_task.
    """
    row, created = pipeline_agent.start_workflow(
        tenant_id=body.tenant_id,
        platform_id=body.platform_id,
        pipeline=body.pipeline,
        idempotency_key=idempotency_key,
    )
    return response_ok(
        "Pipeline run accepted; polling continues in the background."
        if created
        else "Duplicate request - returning the existing in-flight workflow run.",
        {
            "workflow_run_id": row.workflow_run_id,
            "status": row.state,
            "created": created,
        },
    )


@router.get("/pipeline-run/{workflow_run_id}/status", response_model=StandardApiResponse)
def get_pipeline_run_status(
    workflow_run_id: str,
    db: Session = Depends(get_db),
):
    """Fast local-DB-only read the Reflex UI polls; never calls DataPipeline/AiDiagnosis directly."""
    row = workflow_repository.get_workflow_run(db, workflow_run_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Workflow run not found")
    return response_ok(
        "Workflow run status fetched.",
        {
            "workflow_run_id": row.workflow_run_id,
            "tenant_id": row.tenant_id,
            "platform_id": row.platform_id,
            "pipeline": row.pipeline,
            "run_id": row.run_id,
            "incident_id": row.incident_id,
            "state": row.state,
            "current_agent": row.current_agent,
            "attempt_count": row.attempt_count,
            "started_at": row.started_at.isoformat(),
            "updated_at": row.updated_at.isoformat(),
            "completed_at": row.completed_at.isoformat() if row.completed_at else None,
            "error_detail": row.error_detail,
        },
    )


@router.get("/{incident_id}", response_model=StandardApiResponse)
def get_incident_detail(
    incident_id: str,
    tenant_id: str | None = None,
    platform_id: str | None = None,
    db: Session = Depends(get_db),
):
    row = _require_incident(db, incident_id, tenant_id, platform_id)
    return response_ok(
        "Incident detail fetched.",
        {
            "tenant_id": row.tenant_id,
            "platform_id": row.platform_id,
            "incident": _incident_data(row),
            "sections": {},
            "approval_required": False,
            "execution_policy": "Manual review",
            "revision": 0,
            "capabilities": [],
        },
    )


@router.get("/{incident_id}/timeline", response_model=StandardApiResponse)
def get_incident_timeline(
    incident_id: str,
    tenant_id: str | None = None,
    platform_id: str | None = None,
    db: Session = Depends(get_db),
):
    row = _require_incident(db, incident_id, tenant_id, platform_id)
    events = [
        {
            "label": "Detected",
            "message": row.problem or "Failure detected",
            "timestamp": row.created_at.isoformat(),
        },
        {
            "label": "Current status",
            "message": row.status,
            "timestamp": row.updated_at.isoformat(),
        },
    ]
    agent_events = workflow_repository.list_events_for_incident(db, incident_id)
    events.extend(
        {
            "label": f"{event.agent}: {event.event_type}",
            "message": "",
            "timestamp": event.created_at.isoformat(),
        }
        for event in agent_events
    )
    return response_ok("Incident timeline fetched.", {"timeline": events})


@router.get("/{incident_id}/logs", response_model=StandardApiResponse)
def get_incident_logs(
    incident_id: str,
    tenant_id: str | None = None,
    platform_id: str | None = None,
    db: Session = Depends(get_db),
):
    row = _require_incident(db, incident_id, tenant_id, platform_id)
    payload = _incident_payload(db, incident_id)
    fields = payload if isinstance(payload, dict) else {}
    return response_ok(
        "Incident logs fetched.",
        {
            "logs": [
                {
                    "timestamp": row.updated_at.isoformat(),
                    "level": "INFO",
                    "source": row.source,
                    "message": row.problem,
                    "fields": fields,
                }
            ]
        },
    )


@router.post("/{incident_id}/logs/refresh", response_model=StandardApiResponse)
def refresh_incident_logs(
    incident_id: str,
    tenant_id: str | None = None,
    platform_id: str | None = None,
    db: Session = Depends(get_db),
):
    _require_incident(db, incident_id, tenant_id, platform_id)
    return get_incident_logs(incident_id, tenant_id, platform_id, db)


@router.get("/{incident_id}/diagnosis", response_model=StandardApiResponse)
def get_incident_diagnosis(
    incident_id: str,
    tenant_id: str | None = None,
    platform_id: str | None = None,
    db: Session = Depends(get_db),
):
    row = _require_incident(db, incident_id, tenant_id, platform_id)
    diagnosis_ready = row.status in {
        "DIAGNOSED",
        "REMEDIATION_PROPOSED",
        "AWAITING_APPROVAL",
        "REMEDIATING",
        "VALIDATING",
        "RESOLVED",
        "REJECTED",
        "ESCALATED",
    }
    diagnosis = workflow_repository.get_latest_diagnosis(db, incident_id)
    if diagnosis is not None:
        return response_ok(
            "Incident diagnosis fetched.",
            {
                "diagnosis": {
                    "available": True,
                    "summary": diagnosis.root_cause,
                    "failure_location": diagnosis.failure_location,
                    "confidence": diagnosis.confidence,
                    "evidence": diagnosis.evidence,
                    "similar_incidents": diagnosis.similar_incidents,
                    "status": row.status,
                }
            },
        )
    return response_ok(
        "Incident diagnosis fetched.",
        {
            "diagnosis": {
                "available": diagnosis_ready,
                "summary": row.problem if diagnosis_ready else "",
                "status": row.status,
            }
        },
    )


@router.get("/{incident_id}/history", response_model=StandardApiResponse)
def get_incident_history(
    incident_id: str,
    tenant_id: str | None = None,
    platform_id: str | None = None,
    db: Session = Depends(get_db),
):
    row = _require_incident(db, incident_id, tenant_id, platform_id)
    items = list_dashboard_incidents(
        db,
        tenant_id=row.tenant_id,
        platform_id=row.platform_id,
        limit=5,
    )
    similar = [
        {
            "incident_id": item["incident_id"],
            "summary": item.get("problem", ""),
            "detected_at": item.get("created_at", ""),
        }
        for item in items
        if item.get("incident_id") != incident_id
    ]
    return response_ok(
        "Incident history fetched.",
        {"history": {"incidents": similar, "status": "READY"}},
    )


@router.get("/{incident_id}/remediation", response_model=StandardApiResponse)
def get_incident_remediation(
    incident_id: str,
    tenant_id: str | None = None,
    platform_id: str | None = None,
    db: Session = Depends(get_db),
):
    row = _require_incident(db, incident_id, tenant_id, platform_id)
    plan = workflow_repository.get_latest_remediation_plan(db, incident_id)
    if plan is not None:
        return response_ok(
            "Incident remediation fetched.",
            {
                "remediation": {
                    "actions": plan.actions,
                    "approval_status": plan.approval_status,
                    "approved_by": plan.approved_by,
                    "executed": plan.executed,
                    "approval_required": plan.approval_status == "PENDING",
                    "status": row.status,
                }
            },
        )
    return response_ok(
        "Incident remediation fetched.",
        {
            "remediation": {
                "action": "Manual review",
                "reason": row.problem,
                "risk": "MEDIUM",
                "approval_required": False,
                "status": row.status,
            }
        },
    )


@router.post("/{incident_id}/remediation/approve", response_model=StandardApiResponse)
def approve_remediation(
    incident_id: str,
    body: RemediationDecisionRequest,
    db: Session = Depends(get_db),
):
    """The single human approval gate: marks the fix plan ready for a future execution
    agent. Nothing is executed by this call or anywhere else in this system today."""
    _require_incident(db, incident_id)
    plan = workflow_repository.set_remediation_decision(
        db, incident_id=incident_id, approved=True, decided_by=body.decided_by
    )
    if plan is None:
        raise HTTPException(status_code=404, detail="No remediation plan found for this incident")
    workflow_repository.record_event(
        db,
        workflow_run_id=plan.workflow_run_id,
        tenant_id=plan.tenant_id,
        agent="Human",
        event_type="APPROVAL_GRANTED",
        payload={"decided_by": body.decided_by, "comment": body.comment},
        incident_id=incident_id,
    )
    return response_ok(
        "Remediation plan approved.",
        {"incident_id": incident_id, "approval_status": plan.approval_status},
    )


@router.post("/{incident_id}/remediation/reject", response_model=StandardApiResponse)
def reject_remediation(
    incident_id: str,
    body: RemediationDecisionRequest,
    db: Session = Depends(get_db),
):
    _require_incident(db, incident_id)
    plan = workflow_repository.set_remediation_decision(
        db, incident_id=incident_id, approved=False, decided_by=body.decided_by
    )
    if plan is None:
        raise HTTPException(status_code=404, detail="No remediation plan found for this incident")
    incident_repository.mark_incident_rejected(db, incident_id)
    workflow_repository.record_event(
        db,
        workflow_run_id=plan.workflow_run_id,
        tenant_id=plan.tenant_id,
        agent="Human",
        event_type="APPROVAL_REJECTED",
        payload={"decided_by": body.decided_by, "comment": body.comment},
        incident_id=incident_id,
    )
    return response_ok(
        "Remediation plan rejected.",
        {"incident_id": incident_id, "approval_status": plan.approval_status},
    )


@router.get("/{incident_id}/execution", response_model=StandardApiResponse)
def get_incident_execution(
    incident_id: str,
    tenant_id: str | None = None,
    platform_id: str | None = None,
    db: Session = Depends(get_db),
):
    row = _require_incident(db, incident_id, tenant_id, platform_id)
    progress = (
        "Succeeded"
        if row.status in {"VALIDATING", "RESOLVED"}
        else "Failed"
        if row.status == "ESCALATED"
        else "Not started"
    )
    return response_ok(
        "Incident execution fetched.",
        {"execution": {"status": row.status, "progress": progress}},
    )


@router.get("/{incident_id}/validation", response_model=StandardApiResponse)
def get_incident_validation(
    incident_id: str,
    tenant_id: str | None = None,
    platform_id: str | None = None,
    db: Session = Depends(get_db),
):
    row = _require_incident(db, incident_id, tenant_id, platform_id)
    recovery_confirmed = row.status == "RESOLVED"
    outcome = row.status if row.status in {"RESOLVED", "ESCALATED"} else ""
    return response_ok(
        "Incident validation fetched.",
        {
            "validation": {
                "status": row.status,
                "outcome": outcome,
                "recovery_confirmed": recovery_confirmed,
            }
        },
    )


@router.post("/{incident_id}/agent/invoke", response_model=StandardApiResponse)
def invoke_langgraph_agent(
    incident_id: str,
    body: AgentInvokeRequest,
    db: Session = Depends(get_db),
):
    row = get_incident_row(db, incident_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Incident not found")

    invocation = langgraph_service.invoke(
        incident_id=incident_id,
        requested_by=body.requested_by,
        action=body.action,
        payload=body.input,
    )
    row = mark_agent_invoked(
        db,
        incident_id=incident_id,
        invocation_payload=invocation,
    )

    notification = notification_service.notify_human(
        incident_id=incident_id,
        title="LangGraph invocation started",
        message="Human-approved remediation was sent to the LangGraph agent.",
        context={
            "status": row.status,
            "requested_by": body.requested_by,
            "action": body.action,
            "invocation_id": invocation["invocation_id"],
        },
    )
    return response_ok(
        "LangGraph invocation accepted.",
        {
            "incident_id": incident_id,
            "status": row.status,
            "invocation": invocation,
            "notification": notification,
        },
    )


@router.post("/{incident_id}/agent/result", response_model=StandardApiResponse)
def record_agent_result(
    incident_id: str,
    body: AgentResultRequest,
    db: Session = Depends(get_db),
):
    try:
        result = process_agent_result(
            db,
            incident_id=incident_id,
            body=body,
        )
    except ValueError as error:
        raise HTTPException(status_code=404, detail=str(error)) from error

    if result["deleted"]:
        title = "Error resolved"
        message = "Failure is corrected, validation passed, incident row was deleted from PostgreSQL, and the error is resolved."
    elif body.outcome == "FAILED":
        title = "Remediation failed"
        message = "LangGraph remediation failed. Incident was updated and remains open."
    else:
        title = "Remediation complete, validation pending"
        message = "Remediation succeeded but resolution validation is still pending."

    notification = notification_service.notify_human(
        incident_id=incident_id,
        title=title,
        message=message,
        context={
            "outcome": body.outcome,
            "validated_resolved": body.validated_resolved,
            "status": result["status"],
            "deleted": result["deleted"],
        },
    )
    return response_ok(
        "Agent result processed.",
        {
            **result,
            "notification": notification,
        },
    )


@router.post("/pipeline-run", response_model=StandardApiResponse, status_code=202)
def ingest_pipeline_run(
    body: PipelineRunIngestRequest,
    db: Session = Depends(get_db),
):
    """Record one completed Run Pipeline click as a history entry."""
    row = incident_repository.upsert_pipeline_run(
        db,
        incident_id=body.incident_id,
        tenant_id=body.tenant_id,
        platform_id=body.platform_id,
        pipeline=body.pipeline,
        outcome=body.outcome,
        details=body.details,
    )
    return response_ok(
        "Pipeline run recorded.",
        {"incident_id": row.incident_id, "status": row.status},
    )


@router.put("/{incident_id}/diagnosis-result", response_model=StandardApiResponse)
def store_diagnosis_result(
    incident_id: str,
    body: DiagnosisResultRequest,
    db: Session = Depends(get_db),
):
    """Store the AI Diagnosis result (logs/remedies/root cause) for later viewing."""
    row = incident_repository.attach_diagnosis_result(
        db,
        incident_id=incident_id,
        diagnosis_payload=body.model_dump(),
    )
    if row is None:
        raise HTTPException(status_code=404, detail="Incident not found")
    return response_ok(
        "Diagnosis result stored.",
        {"incident_id": row.incident_id, "status": row.status},
    )


@router.post("/{incident_id}/resolve", response_model=StandardApiResponse)
def resolve_incident(
    incident_id: str,
    db: Session = Depends(get_db),
):
    """A human marks this incident resolved. Does not execute any remediation."""
    row = incident_repository.mark_incident_resolved(db, incident_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Incident not found")
    return response_ok(
        "Incident marked resolved.",
        {"incident_id": row.incident_id, "status": row.status},
    )
