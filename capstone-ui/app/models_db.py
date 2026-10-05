from sqlalchemy import JSON, Boolean, Column, DateTime, Float, ForeignKey, Integer, String
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class IncidentDB(Base):
    __tablename__ = "incidents"

    incident_id = Column(String, primary_key=True)
    tenant_id = Column(String, nullable=False, index=True)
    platform_id = Column(String, nullable=False, index=True)
    pipeline = Column(String, nullable=False, default="")
    severity = Column(String, nullable=False)
    status = Column(String, nullable=False)
    problem = Column(String, nullable=False, default="")
    source = Column(String, nullable=False, default="")
    created_at = Column(DateTime(timezone=True), nullable=False)
    updated_at = Column(DateTime(timezone=True), nullable=False)


class IncidentPayloadDB(Base):
    __tablename__ = "incident_payloads"

    incident_id = Column(
        String,
        ForeignKey("incidents.incident_id", ondelete="CASCADE"),
        primary_key=True,
    )
    failure_payload = Column(JSON, nullable=False, default=dict)
    agent_result_payload = Column(JSON, nullable=False, default=dict)
    created_at = Column(DateTime(timezone=True), nullable=False)
    updated_at = Column(DateTime(timezone=True), nullable=False)


class WorkflowRunDB(Base):
    """Persistent state for one 'Start Pipeline' click -> Pipeline Agent lifecycle.

    Survives process restarts: the Pipeline Agent reconciliation hook resumes any
    row whose completed_at is still NULL instead of losing the in-flight workflow.
    """

    __tablename__ = "workflow_runs"

    workflow_run_id = Column(String, primary_key=True)
    tenant_id = Column(String, nullable=False, index=True)
    platform_id = Column(String, nullable=False, index=True)
    pipeline = Column(String, nullable=False, default="")
    incident_id = Column(
        String, ForeignKey("incidents.incident_id", ondelete="SET NULL"), nullable=True
    )
    run_id = Column(String, nullable=True, index=True)
    state = Column(String, nullable=False, default="STARTING")
    current_agent = Column(String, nullable=False, default="PipelineAgent")
    attempt_count = Column(Integer, nullable=False, default=0)
    poll_interval_seconds = Column(Integer, nullable=False, default=15)
    next_poll_at = Column(DateTime(timezone=True), nullable=True)
    deadline_at = Column(DateTime(timezone=True), nullable=True)
    idempotency_key = Column(String, nullable=True, unique=True)
    error_detail = Column(String, nullable=True)
    started_at = Column(DateTime(timezone=True), nullable=False)
    updated_at = Column(DateTime(timezone=True), nullable=False)
    completed_at = Column(DateTime(timezone=True), nullable=True)


class AgentEventDB(Base):
    """Append-only audit trail / event log; also the idempotency gate for handoffs."""

    __tablename__ = "agent_events"

    event_id = Column(String, primary_key=True)
    workflow_run_id = Column(
        String, ForeignKey("workflow_runs.workflow_run_id", ondelete="CASCADE"), nullable=False, index=True
    )
    incident_id = Column(String, nullable=True, index=True)
    tenant_id = Column(String, nullable=False, index=True)
    agent = Column(String, nullable=False)
    event_type = Column(String, nullable=False)
    payload = Column(JSON, nullable=False, default=dict)
    created_at = Column(DateTime(timezone=True), nullable=False)


class DiagnosisDB(Base):
    """Structured Agent-2 output, split out of AiDiagnosis's combined response."""

    __tablename__ = "diagnoses"

    diagnosis_id = Column(String, primary_key=True)
    workflow_run_id = Column(String, ForeignKey("workflow_runs.workflow_run_id", ondelete="CASCADE"), nullable=False)
    incident_id = Column(String, nullable=False, index=True)
    tenant_id = Column(String, nullable=False, index=True)
    failure_location = Column(String, nullable=False, default="")
    root_cause = Column(String, nullable=False, default="")
    confidence = Column(Float, nullable=False, default=0.0)
    evidence = Column(JSON, nullable=False, default=list)
    reasoning_summary = Column(String, nullable=False, default="")
    similar_incidents = Column(JSON, nullable=False, default=list)
    created_at = Column(DateTime(timezone=True), nullable=False)


class RemediationPlanDB(Base):
    """Structured Agent-3 output: a proposed fix plan, never auto-executed."""

    __tablename__ = "remediation_plans"

    plan_id = Column(String, primary_key=True)
    workflow_run_id = Column(String, ForeignKey("workflow_runs.workflow_run_id", ondelete="CASCADE"), nullable=False)
    incident_id = Column(String, nullable=False, index=True)
    tenant_id = Column(String, nullable=False, index=True)
    actions = Column(JSON, nullable=False, default=list)
    validation_result = Column(JSON, nullable=False, default=dict)
    approval_status = Column(String, nullable=False, default="PENDING")
    approved_by = Column(String, nullable=True)
    approved_at = Column(DateTime(timezone=True), nullable=True)
    executed = Column(Boolean, nullable=False, default=False)
    created_at = Column(DateTime(timezone=True), nullable=False)
