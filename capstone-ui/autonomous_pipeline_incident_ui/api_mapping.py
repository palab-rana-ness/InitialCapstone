import reflex as rx
import json
import logging
from datetime import datetime, timezone
from typing import Literal
from pydantic import (
    BaseModel,
    AliasChoices,
    Field,
    AwareDatetime,
    ConfigDict,
    field_validator,
)
from autonomous_pipeline_incident_ui.http_client import ServiceError
from autonomous_pipeline_incident_ui.models import (
    IncidentRecord,
    IncidentsResponse,
    DetailResponse,
    DetailSection,
    DetailEntry,
    ScopeOption,
    LogsResponse,
    LogEntry,
    DashboardResponse,
    Metric,
    Pipeline,
    ActionCapability,
    WorkflowResponse,
    ConfigurationResponse,
    ConfigurationSavedResponse,
    PlatformConfiguration,
    PipelineRunStartResponse,
    PipelineRunResultResponse,
    PipelineDiagnosisResponse,
    PipelineHistoryEntry,
    PipelineRemedy,
)


class APIIncident(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    id: str = Field(
        validation_alias=AliasChoices("incident_id", "id"), min_length=1
    )
    tenant_id: str = Field(min_length=1)
    platform_id: str = Field(min_length=1)
    severity: Literal["CRITICAL", "HIGH", "MEDIUM", "LOW"]
    status: Literal[
        "DETECTED",
        "QUEUED",
        "ANALYSIS_QUEUED",
        "SENDING",
        "SENDING_TO_AGENT",
        "ANALYZING",
        "EXECUTING",
        "INVESTIGATING",
        "DIAGNOSED",
        "REMEDIATION_PROPOSED",
        "AWAITING_APPROVAL",
        "REMEDIATING",
        "VALIDATING",
        "RESOLVED",
        "REJECTED",
        "ESCALATED",
    ]
    detected_at: AwareDatetime = Field(
        validation_alias=AliasChoices("detected_at", "created_at")
    )
    pipeline: str = Field(
        default="", validation_alias=AliasChoices("pipeline_name", "pipeline")
    )
    tenant: str = Field(
        default="", validation_alias=AliasChoices("tenant_name", "tenant")
    )
    platform: str = Field(
        default="", validation_alias=AliasChoices("platform_name", "platform")
    )

    @field_validator("id", "tenant_id", "platform_id")
    @classmethod
    def usable(cls, value: str) -> str:
        if (
            not value.strip()
            or value != value.strip()
            or any(ord(c) < 32 for c in value)
        ):
            raise ValueError("Invalid identifier")
        return value

    recovery_confirmed: bool = Field(default=False, strict=True)

    def record(self) -> IncidentRecord:
        data = self.model_dump()
        data["tenant"] = self.tenant or self.tenant_id
        data["platform"] = self.platform or self.platform_id
        return IncidentRecord(**data, detected=self.detected_at.isoformat())


def unwrap(
    data: object,
    keys: tuple[str, ...],
    tenant: str = "",
    platform: str = "",
    identifier: str = "",
) -> object:
    for _ in range(6):
        if not isinstance(data, dict):
            break
        check_scope(data, tenant, platform, identifier)
        key = next((key for key in keys if key in data), "")
        if not key:
            break
        data = data[key]
    return data


def check_scope(data: object, tenant: str, platform: str, identifier: str = ""):
    if not isinstance(data, dict):
        return
    for key, expected in (
        ("tenant_id", tenant),
        ("platform_id", platform),
        ("incident_id", identifier),
    ):
        if expected and key in data and data[key] != expected:
            raise ServiceError("api")


def options(data: object, key: str, identifier: str) -> list[ScopeOption]:
    metadata = data if isinstance(data, dict) else {}
    metadata = metadata.get("metadata", metadata)
    raw = metadata.get(f"authorized_{key}", metadata.get(key, []))
    result = [ScopeOption.model_validate(item) for item in raw]
    if any(not item.id.strip() or not item.label.strip() for item in result):
        raise ServiceError("api")
    if identifier and identifier not in {item.id for item in result}:
        result.append(ScopeOption(id=identifier, label=identifier))
    return result


def normalize_list(
    data: object, tenant: str, platform: str
) -> IncidentsResponse:
    check_scope(data, tenant, platform)
    raw = unwrap(data, ("incidents", "items", "data"), tenant, platform)
    if not isinstance(raw, list):
        raise ServiceError("api")
    rows = [APIIncident.model_validate(item).record() for item in raw]
    meta = data if isinstance(data, dict) else {}
    tenant = (
        tenant
        or meta.get("tenant_id", "")
        or (rows[0].tenant_id if rows else "")
    )
    platform = (
        platform
        or meta.get("platform_id", "")
        or (rows[0].platform_id if rows else "")
    )
    if any(
        (row.tenant_id, row.platform_id) != (tenant, platform) for row in rows
    ):
        raise ServiceError("api")
    if len({row.id for row in rows}) != len(rows):
        raise ServiceError("api")
    aggregates = meta.get("dashboard", meta)
    check_scope(aggregates, tenant, platform)
    metrics = aggregates.get("metrics", [])
    if metrics and len(metrics) != 7:
        raise ServiceError("api")
    return IncidentsResponse(
        tenant_id=tenant,
        platform_id=platform,
        incidents=rows,
        updated_at=str(
            meta.get("updated_at", datetime.now(timezone.utc).isoformat())
        ),
        tenants=options(data, "tenants", tenant),
        platforms=options(data, "platforms", platform),
        metrics=metrics,
        pipelines=aggregates.get("pipelines", []),
        window=aggregates.get("window", ""),
        summary=aggregates.get("summary", ""),
    )


def normalize_detail(
    data: object, tenant: str, platform: str, identifier: str
) -> DetailResponse:
    check_scope(data, tenant, platform, identifier)
    body = unwrap(data, ("data", "detail"), tenant, platform, identifier)
    if not isinstance(body, dict):
        raise ServiceError("api")
    check_scope(body, tenant, platform, identifier)
    raw = unwrap(body, ("incident",))
    record = APIIncident.model_validate(raw).record()
    if (
        record.id != identifier
        or (tenant and record.tenant_id != tenant)
        or (platform and record.platform_id != platform)
    ):
        raise ServiceError("api")
    sections = body.get("sections", {})
    result = DetailResponse(
        tenant_id=record.tenant_id,
        platform_id=record.platform_id,
        incident=record,
        sections={
            name: DetailSection.model_validate(
                sections.get(name, body.get(name, {}))
            )
            for name in (
                "evidence",
                "diagnosis",
                "history",
                "audit",
                "remediation",
                "execution",
                "validation",
            )
        },
        capabilities=capabilities(body),
        approval_required=body.get("approval_required", False),
        execution_policy=body.get("execution_policy", ""),
        revision=body.get("revision", 0),
        tenants=options(data, "tenants", record.tenant_id),
        platforms=options(data, "platforms", record.platform_id),
    )
    validation = result.sections["validation"]
    record.recovery_confirmed = validation.recovery_confirmed
    if validation.outcome == "RESOLVED" and not validation.recovery_confirmed:
        validation.outcome = ""
        validation.progress = "Recovery unconfirmed"
    for name, section in result.sections.items():
        if name != "validation" and section.outcome == "RESOLVED":
            section.outcome = ""
        if section.progress == "RESOLVED" and not validation.recovery_confirmed:
            section.progress = "Recovery unconfirmed"
        if name == "audit":
            section.entries = chronological(section.entries)
    return result


def normalize_timeline(
    data: object, tenant: str, platform: str, identifier: str
) -> DetailSection:
    check_scope(data, tenant, platform, identifier)
    body = unwrap(
        data,
        ("data", "timeline", "items", "events"),
        tenant,
        platform,
        identifier,
    )
    check_scope(body, tenant, platform, identifier)
    if isinstance(body, dict) and "entries" in body:
        body = body["entries"]
    if not isinstance(body, list):
        raise ServiceError("api")
    entries = []
    for item in body:
        if not isinstance(item, dict) or not any(
            item.get(k)
            for k in (
                "label",
                "event_type",
                "action",
                "value",
                "message",
                "description",
            )
        ):
            raise ServiceError("api")
        check_scope(item, tenant, platform, identifier)
        entries.append(
            DetailEntry(
                label=item.get(
                    "label", item.get("event_type", item.get("action", "Event"))
                ),
                value=item.get(
                    "value", item.get("message", item.get("description", ""))
                ),
                timestamp=item.get("timestamp", item.get("created_at", "")),
            )
        )
    return DetailSection(
        entries=chronological(entries),
        progress="Current",
        available=bool(entries),
    )


def normalize_logs(
    data: object, tenant: str, platform: str, identifier: str
) -> LogsResponse:
    check_scope(data, tenant, platform, identifier)
    body = unwrap(
        data, ("data", "logs", "items", "entries"), tenant, platform, identifier
    )
    if not isinstance(body, list):
        raise ServiceError("api")
    entries = []
    for item in body:
        check_scope(item, tenant, platform, identifier)
        if not isinstance(item, dict):
            raise ServiceError("api")
        fields = item.get("fields", item.get("attributes", {}))
        if not isinstance(fields, dict):
            raise ServiceError("api")
        entries.append(
            LogEntry(
                timestamp=item.get("timestamp", item.get("created_at", "")),
                level=item.get("level", "UNKNOWN"),
                source=item.get("source", ""),
                message=item.get("message", ""),
                fields_json=json.dumps(
                    fields, ensure_ascii=False, indent=2, allow_nan=False
                ),
            )
        )
    return LogsResponse(entries=entries)


def chronological(entries: list[DetailEntry]) -> list[DetailEntry]:
    def key(entry: DetailEntry):
        try:
            stamp = datetime.fromisoformat(
                entry.timestamp.replace("Z", "+00:00")
            )
            if stamp.tzinfo is None:
                stamp = stamp.replace(tzinfo=timezone.utc)
            return (0, stamp.timestamp())
        except (ValueError, OverflowError):
            logging.exception("Unexpected error")
            logging.error("Response timestamp invalid: %s", "api")
            return (1, 0.0)

    return sorted(entries, key=key)


def capabilities(body: dict) -> list[ActionCapability]:
    names = {
        "analyze": "Send to Agent",
        "approve": "Approve & Execute",
        "reject": "Reject",
        "execute_retry": "Retry",
    }
    aliases = {"retry": "execute_retry", "start_analysis": "analyze"}
    raw = body.get("capabilities", body.get("allowed_actions", []))
    if isinstance(raw, dict):
        raw = [key for key, enabled in raw.items() if enabled is True]
    if not isinstance(raw, list):
        raise ServiceError("api")
    result = {}
    for item in raw:
        if isinstance(item, str):
            item = {"operation": item}
        if not isinstance(item, dict):
            raise ServiceError("api")
        if item.get("enabled", item.get("allowed", True)) is not True:
            continue
        operation = item.get("operation", item.get("action", ""))
        operation = aliases.get(operation, operation)
        if operation in names:
            result[operation] = ActionCapability(
                operation=operation,
                label=item.get("label", names[operation]),
                confirmation=item.get("confirmation", ""),
            )
    for flag, operation in (
        ("can_analyze", "analyze"),
        ("can_approve", "approve"),
        ("can_reject", "reject"),
        ("can_retry", "execute_retry"),
        ("retry_allowed", "execute_retry"),
    ):
        if flag in body and type(body[flag]) is not bool:
            raise ServiceError("api")
        if body.get(flag) is True:
            result[operation] = ActionCapability(
                operation=operation, label=names[operation]
            )
        elif body.get(flag) is False:
            result.pop(operation, None)
    return list(result.values())


def normalize_workflow(
    data: object, tenant: str, platform: str, identifier: str, name: str
) -> WorkflowResponse:
    body = unwrap(data, ("data", name), tenant, platform, identifier)
    if name == "history" and isinstance(body, list):
        body = {"incidents": body}
    if not isinstance(body, dict) or not body:
        raise ServiceError("api")
    check_scope(body, tenant, platform, identifier)
    recognized = {
        "available",
        "root_cause",
        "summary",
        "entries",
        "incidents",
        "similar_incidents",
        "action",
        "reason",
        "risk",
        "approval_required",
        "capabilities",
        "allowed_actions",
        "can_analyze",
        "can_approve",
        "can_reject",
        "can_retry",
        "retry_allowed",
        "status",
        "progress",
        "outcome",
        "accepted",
        "recovery_confirmed",
        "logs",
        "evidence",
        "confidence",
        "severity",
    }
    if not recognized.intersection(body):
        raise ServiceError("api")
    for key in (
        "available",
        "approval_required",
        "accepted",
        "recovery_confirmed",
    ):
        if key in body and type(body[key]) is not bool:
            raise ServiceError("api")
    status = body.get("status", "")
    if not isinstance(status, str):
        raise ServiceError("api")
    if name in {"analyze", "approve", "reject", "retry"}:
        if body.get("accepted") is False or status.upper() in {
            "ERROR",
            "FAILED",
            "DENIED",
        }:
            raise ServiceError("request_invalid")
        if not status and body.get("accepted") is not True:
            raise ServiceError("api")
    section = DetailSection(
        progress=body.get("progress", status or "Not reported"),
        summary=body.get("root_cause", body.get("summary", "")),
        entries=body.get("entries", []),
        logs=body.get("logs", []),
        confidence=body.get("confidence", 0),
        confidence_supplied="confidence" in body,
        severity=body.get("severity", ""),
        outcome=body.get("outcome", ""),
        recovery_confirmed=body.get("recovery_confirmed", False),
    )
    if name == "diagnosis":
        section.available = body.get("available") is True or bool(
            section.summary or section.entries or body.get("evidence")
        )
        if body.get("available") is False:
            section.available = False
        if not section.available:
            section = DetailSection(summary="Diagnosis is not available yet.")
        elif body.get("evidence"):
            section.entries.append(
                DetailEntry(
                    label="Evidence", value=display_value(body["evidence"])
                )
            )
    elif name == "history":
        rows = body.get("similar_incidents", body.get("incidents", []))
        if not isinstance(rows, list):
            raise ServiceError("api")
        for row in rows:
            if not isinstance(row, dict) or not any(
                row.get(k)
                for k in ("incident_id", "id", "summary", "root_cause")
            ):
                raise ServiceError("api")
            check_scope(row, tenant, platform)
            section.entries.append(
                DetailEntry(
                    label=str(
                        row.get(
                            "incident_id", row.get("id", "Similar incident")
                        )
                    ),
                    value=display_value(row),
                    timestamp=str(
                        row.get("detected_at", row.get("created_at", ""))
                    ),
                )
            )
        section.available = bool(section.entries)
    else:
        for key in (
            "action",
            "reason",
            "risk",
            "approval_required",
            "recovery_confirmed",
        ):
            if key in body:
                section.entries.append(
                    DetailEntry(
                        label=key.replace("_", " ").title(),
                        value=display_value(body[key]),
                    )
                )
        section.available = True
    if section.progress == "RESOLVED" and (
        name != "validation" or not section.recovery_confirmed
    ):
        section.progress = "Recovery unconfirmed"
    if name == "validation":
        section.outcome = body.get("outcome", status)
        if section.outcome == "RESOLVED" and not section.recovery_confirmed:
            section.outcome = ""
            section.summary = "Recovery has not been confirmed."
    elif section.outcome == "RESOLVED":
        section.outcome = ""
    supplied = any(
        k in body
        for k in (
            "capabilities",
            "allowed_actions",
            "can_analyze",
            "can_approve",
            "can_reject",
            "can_retry",
            "retry_allowed",
        )
    )
    return WorkflowResponse(
        section=section,
        status=status,
        accepted=body.get("accepted", False),
        capabilities=capabilities(body),
        capabilities_supplied=supplied,
        approval_required=body.get("approval_required", False),
        approval_supplied="approval_required" in body,
    )


def display_value(value: object) -> str:
    return (
        value
        if isinstance(value, str)
        else json.dumps(value, ensure_ascii=False, allow_nan=False)
    )


def _first_text(*values: object) -> str:
    for value in values:
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _pipeline_outcome(value: object) -> str:
    if not isinstance(value, str):
        return ""
    normalized = value.strip().upper()
    if normalized in {"PASSED", "PASS", "SUCCESS", "SUCCEEDED"}:
        return "PASSED"
    if normalized in {"FAILED", "FAIL", "ERROR"}:
        return "FAILED"
    return ""


def normalize_pipeline_run_start(
    data: object,
    tenant: str,
    platform: str,
    pipeline_type: str,
) -> PipelineRunStartResponse:
    body = unwrap(data, ("data", "run", "result"), tenant, platform)
    if isinstance(body, str):
        body = {"run_id": body}
    if not isinstance(body, dict):
        raise ServiceError("api")
    check_scope(body, tenant, platform)
    meta = data.get("meta", {}) if isinstance(data, dict) else {}
    if not isinstance(meta, dict):
        meta = {}
    run_id = _first_text(
        body.get("run_id"),
        body.get("pipeline_run_id"),
        body.get("execution_id"),
        body.get("request_id"),
        body.get("incident_id"),
        meta.get("request_id"),
    )
    if not run_id:
        raise ServiceError("api")
    accepted = body.get("accepted", True)
    if type(accepted) is not bool:
        raise ServiceError("api")
    status = body.get("status", body.get("state", ""))
    if status and not isinstance(status, str):
        raise ServiceError("api")
    return PipelineRunStartResponse(
        run_id=run_id,
        status=status,
        accepted=accepted,
        tenant_id=_first_text(body.get("tenant_id"), tenant),
        platform_id=_first_text(body.get("platform_id"), platform),
        pipeline_type=_first_text(
            body.get("pipeline_type"),
            body.get("pipeline"),
            body.get("pipeline_name"),
            pipeline_type,
        ),
    )


def normalize_pipeline_run_result(
    data: object,
    tenant: str,
    platform: str,
    run_id: str,
) -> PipelineRunResultResponse:
    body = unwrap(data, ("data", "result", "run"), tenant, platform)
    if isinstance(body, str):
        body = {"outcome": body}
    if not isinstance(body, dict):
        raise ServiceError("api")
    check_scope(body, tenant, platform)
    response_run_id = _first_text(
        body.get("run_id"),
        body.get("pipeline_run_id"),
        body.get("execution_id"),
        run_id,
    )
    if run_id and response_run_id != run_id:
        raise ServiceError("api")
    outcome = _pipeline_outcome(
        body.get(
            "outcome",
            body.get("result", body.get("pipeline_result", body.get("status", ""))),
        )
    )
    if not outcome:
        raise ServiceError("api")
    status = body.get("status", "")
    if status and not isinstance(status, str):
        raise ServiceError("api")
    details = body.get(
        "details",
        body.get("failure_details", body.get("message", body.get("error", ""))),
    )
    return PipelineRunResultResponse(
        run_id=response_run_id,
        outcome=outcome,
        status=status,
        details=display_value(details) if details else "",
        tenant_id=_first_text(body.get("tenant_id"), tenant),
        platform_id=_first_text(body.get("platform_id"), platform),
        pipeline_type=_first_text(
            body.get("pipeline_type"), body.get("pipeline"), body.get("pipeline_name")
        ),
    )


def normalize_pipeline_diagnosis(
    data: object,
    tenant: str,
    platform: str,
    run_id: str,
) -> PipelineDiagnosisResponse:
    """Map the AI Diagnosis app's real `DiagnosisResponse` onto the UI's model.

    The `incident_id` sent to AI Diagnosis is this pipeline run's `run_id`
    (there is no separate incident identifier in this flow), so it is
    checked for a round-trip match the same way other boundaries here check
    `run_id`/`incident_id` echoes.
    """
    if not isinstance(data, dict):
        raise ServiceError("api")
    check_scope(data, tenant, platform, run_id)
    failure_location = _first_text(data.get("failure_location"))
    root_cause = _first_text(data.get("root_cause"))
    if not failure_location or not root_cause:
        raise ServiceError("api")
    message_for_ui = _first_text(data.get("message_for_ui"))
    evidence = data.get("evidence", [])
    remedies = data.get("remedies", [])
    detail_lines = [message_for_ui] if message_for_ui else []
    if isinstance(evidence, list) and evidence:
        detail_lines.append(
            "Evidence: " + "; ".join(display_value(item) for item in evidence)
        )
    if isinstance(remedies, list) and remedies:
        actions = [
            item.get("action", "")
            for item in remedies
            if isinstance(item, dict) and item.get("action")
        ]
        if actions:
            detail_lines.append("Recommended actions: " + ", ".join(actions))
    return PipelineDiagnosisResponse(
        run_id=run_id,
        diagnosis=f"Failed at {failure_location}: {root_cause}",
        details="\n".join(detail_lines),
        tenant_id=_first_text(data.get("tenant_id"), tenant),
        platform_id=platform,
        pipeline_type=_first_text(data.get("pipeline")),
    )


def normalize_pipeline_history(data: object, tenant: str) -> list[PipelineHistoryEntry]:
    body = unwrap(data, ("data", "items"), tenant)
    items = body.get("items") if isinstance(body, dict) else body
    if not isinstance(items, list):
        raise ServiceError("api")
    entries: list[PipelineHistoryEntry] = []
    for row in items:
        if not isinstance(row, dict) or row.get("source") != "PIPELINE_RUN":
            continue
        details = row.get("details") if isinstance(row.get("details"), dict) else {}
        diagnosis = details.get("diagnosis") if isinstance(details.get("diagnosis"), dict) else {}
        remedies_raw = diagnosis.get("remedies", [])
        remedies = [
            PipelineRemedy(
                action=_first_text(item.get("action")),
                explanation=_first_text(item.get("explanation")),
                priority=_first_text(item.get("priority")),
                risk=_first_text(item.get("risk")),
            )
            for item in remedies_raw
            if isinstance(item, dict)
        ]
        entries.append(
            PipelineHistoryEntry(
                incident_id=_first_text(row.get("incident_id")),
                pipeline=_first_text(row.get("pipeline")),
                status=_first_text(row.get("status")),
                outcome=_first_text(details.get("outcome")),
                created_at=_first_text(row.get("created_at")),
                failure_location=_first_text(diagnosis.get("failure_location")),
                root_cause=_first_text(diagnosis.get("root_cause")),
                recent_logs=[str(x) for x in diagnosis.get("recent_logs", [])][:20],
                remedies=remedies,
                message_for_ui=_first_text(diagnosis.get("message_for_ui")),
            )
        )
    return entries


def normalize_backend_pipeline_start(
    data: object,
    tenant: str,
    platform: str,
    pipeline_type: str,
) -> PipelineRunStartResponse:
    """Maps capstone-ui's own POST /pipeline-run/start response (Agent 1)."""
    body = unwrap(data, ("data",), tenant, platform)
    if not isinstance(body, dict):
        raise ServiceError("api")
    workflow_run_id = _first_text(body.get("workflow_run_id"))
    if not workflow_run_id:
        raise ServiceError("api")
    return PipelineRunStartResponse(
        run_id=workflow_run_id,
        status=_first_text(body.get("status")),
        accepted=True,
        tenant_id=tenant,
        platform_id=platform,
        pipeline_type=pipeline_type,
    )


# Workflow states that mean the Pipeline Agent is still working (not a terminal
# PASSED/FAILED outcome) - the caller should keep polling.
_PENDING_WORKFLOW_STATES = {
    "STARTING",
    "RUNNING",
    "FAILED",  # terminal for the pipeline itself, but diagnosis is still in flight
    "DIAGNOSING",
}


def normalize_backend_pipeline_status(
    data: object,
    tenant: str,
    platform: str,
    workflow_run_id: str,
) -> PipelineRunResultResponse:
    """Maps capstone-ui's own GET /pipeline-run/{id}/status response.

    Raises ServiceError("empty") while the workflow is still in flight so the
    existing polling loop in PipelineRunState keeps retrying, exactly like the
    old DataPipeline-direct 404-while-pending behavior.
    """
    body = unwrap(data, ("data",), tenant, platform)
    if not isinstance(body, dict):
        raise ServiceError("api")
    check_scope(body, tenant, platform)
    state = _first_text(body.get("state"))
    # A FAILED pipeline is only "ready" for the UI once Agent 2/3 have attached
    # a diagnosis/remediation plan (state moves past AWAITING_APPROVAL or ERROR).
    if state in _PENDING_WORKFLOW_STATES:
        raise ServiceError("empty")
    outcome = "PASSED" if state == "PASSED" else "FAILED" if state else ""
    if not outcome:
        raise ServiceError("api")
    return PipelineRunResultResponse(
        run_id=workflow_run_id,
        outcome=outcome,
        status=state,
        details=_first_text(body.get("error_detail")),
        tenant_id=tenant,
        platform_id=platform,
        incident_id=_first_text(body.get("incident_id")),
    )


def normalize_backend_diagnosis(
    data: object,
    tenant: str,
    platform: str,
    run_id: str,
) -> PipelineDiagnosisResponse:
    """Maps capstone-ui's own GET /incidents/{id}/diagnosis response - the
    diagnosis Agent 2 already computed and persisted. Never calls AiDiagnosis
    directly from the UI."""
    body = unwrap(data, ("data", "diagnosis"), tenant, platform)
    if not isinstance(body, dict):
        raise ServiceError("api")
    if not body.get("available"):
        raise ServiceError("empty")
    failure_location = _first_text(body.get("failure_location"))
    root_cause = _first_text(body.get("summary"))
    if not root_cause:
        raise ServiceError("empty")
    detail_lines = []
    evidence = body.get("evidence", [])
    if isinstance(evidence, list) and evidence:
        detail_lines.append(
            "Evidence: " + "; ".join(display_value(item) for item in evidence)
        )
    label = (
        f"Failed at {failure_location}: {root_cause}" if failure_location else root_cause
    )
    return PipelineDiagnosisResponse(
        run_id=run_id,
        diagnosis=label,
        details="\n".join(detail_lines),
        tenant_id=tenant,
        platform_id=platform,
        pipeline_type="",
    )


def normalize_retail_tenants(data: object) -> list[ScopeOption]:
    body = unwrap(data, ("data",))
    if not isinstance(body, list):
        raise ServiceError("api")
    result = []
    for item in body:
        if not isinstance(item, dict):
            raise ServiceError("api")
        identifier = item.get("tenant_id", "")
        label = item.get("tenant_name", identifier)
        if (
            not isinstance(identifier, str)
            or not identifier.strip()
            or not isinstance(label, str)
            or not label.strip()
        ):
            raise ServiceError("api")
        result.append(ScopeOption(id=identifier, label=label))
    if len({item.id for item in result}) != len(result):
        raise ServiceError("api")
    return result


def normalize_catalog(data: object, name: str) -> list[ScopeOption]:
    body = unwrap(data, ("data", name, "items", f"authorized_{name}"))
    if not isinstance(body, list):
        raise ServiceError("api")
    singular = "tenant" if name == "tenants" else "platform"
    result = []
    for item in body:
        if not isinstance(item, dict):
            raise ServiceError("api")
        identifier = item.get("id", item.get(f"{singular}_id", ""))
        label = item.get(
            "label", item.get("name", item.get(f"{singular}_name", identifier))
        )
        if (
            not isinstance(identifier, str)
            or not identifier.strip()
            or not isinstance(label, str)
            or not label.strip()
        ):
            raise ServiceError("api")
        result.append(ScopeOption(id=identifier, label=label))
    if len({item.id for item in result}) != len(result):
        raise ServiceError("api")
    return result


def normalize_config(
    data: object, tenant: str, platform: str, saved: bool = False
):
    body = unwrap(data, ("data", "config", "configuration"), tenant, platform)
    check_scope(body, tenant, platform)
    model = ConfigurationSavedResponse if saved else ConfigurationResponse
    result = model.model_validate(body)
    result.demo = False
    return result


def normalize_platform_config(
    data: object, tenant: str, platform: str
) -> PlatformConfiguration:
    body = unwrap(data, ("data", "config", "configuration"), tenant, platform)
    if not isinstance(body, dict) or not any(
        k not in {"tenant_id", "platform_id"} for k in body
    ):
        raise ServiceError("api")
    check_scope(body, tenant, platform)
    public_keys = {
        "platform_name",
        "enabled",
        "monitoring_rules",
        "remediation_policy",
        "allowed_actions",
        "action_options",
        "policies",
        "capabilities",
    }
    return PlatformConfiguration(
        platform_id=platform,
        summary=str(body.get("summary", "")),
        entries=[
            DetailEntry(
                label=k.replace("_", " ").title(), value=display_value(v)
            )
            for k, v in body.items()
            if k in public_keys
        ],
    )


def mapped(mapper, *args):
    kind = "api"
    try:
        return mapper(*args)
    except ServiceError as error:
        logging.exception("Unexpected error")
        kind = error.kind
    except Exception:
        logging.exception("Unexpected error")
        kind = "api"
    logging.error("Response mapping failed")
    raise ServiceError(kind) from None


def dashboard_from_list(result: IncidentsResponse) -> DashboardResponse:
    rows = result.incidents
    statuses = [
        row.status
        if row.status != "RESOLVED" or row.recovery_confirmed
        else "VALIDATING"
        for row in rows
    ]
    labels = [
        "Total incidents",
        "Open incidents",
        "Investigating",
        "Awaiting approval",
        "Remediating",
        "Resolved",
        "Failed/Escalated",
    ]
    counts = [
        len(rows),
        sum(s not in {"RESOLVED", "REJECTED", "ESCALATED"} for s in statuses),
        statuses.count("INVESTIGATING"),
        statuses.count("AWAITING_APPROVAL"),
        statuses.count("REMEDIATING"),
        statuses.count("RESOLVED"),
        statuses.count("ESCALATED"),
    ]
    pipelines = []
    for name in dict.fromkeys(row.pipeline for row in rows if row.pipeline):
        group = [row for row in rows if row.pipeline == name]
        resolved = sum(
            row.status == "RESOLVED" and row.recovery_confirmed for row in group
        )
        pipelines.append(
            Pipeline(
                name=name,
                health=100 * resolved / len(group),
                status="Incident resolution ratio",
                detail=f"{resolved}/{len(group)} returned incidents resolved · not live health",
            )
        )
    return DashboardResponse(
        tenant_id=result.tenant_id,
        platform_id=result.platform_id,
        updated_at=result.updated_at,
        window=result.window or "Returned authorized incidents",
        metrics=result.metrics
        or [
            Metric(
                label=label, value=str(count), note="From returned incidents"
            )
            for label, count in zip(labels, counts)
        ],
        incidents=sorted(rows, key=lambda r: r.detected_at, reverse=True),
        pipelines=result.pipelines or pipelines,
        summary=result.summary
        or f"{len(rows)} authorized incidents returned. Live pipeline health is not reported.",
        tenants=result.tenants,
        platforms=result.platforms,
    )
