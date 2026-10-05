import reflex as rx
import asyncio
import logging
from autonomous_pipeline_incident_ui.service import (
    ServiceError,
    start_pipeline_run,
    get_pipeline_run_result,
    diagnose_pipeline_run,
    polling_settings,
    resolve_pipeline_incident,
    decide_pipeline_remediation,
    get_pipeline_history,
)
from autonomous_pipeline_incident_ui.models import PipelineHistoryEntry
from autonomous_pipeline_incident_ui.states.scope_state import ScopeState


class PipelineRunState(rx.State):
    pipeline_types: list[str] = []
    pipeline_type: str = ""
    run_id: str = ""
    pipeline_outcome: str = ""
    result_details: str = ""
    diagnosis: str = ""
    diagnosis_details: str = ""
    status_text: str = "Select a pipeline type, then run a pipeline."
    fix_note: str = ""
    error_kind: str = ""
    run_loading: bool = False
    result_loading: bool = False
    diagnose_loading: bool = False
    history: list[PipelineHistoryEntry] = []
    history_loading: bool = False
    expanded_history_id: str = ""
    resolve_loading: bool = False
    decision_loading: bool = False
    _generation: int = 0
    _scope: tuple[str, str] = ("", "")

    @rx.var
    def busy(self) -> bool:
        return self.run_loading or self.result_loading or self.diagnose_loading

    @rx.var
    def can_run(self) -> bool:
        return bool(self.pipeline_type) and not self.busy

    @rx.var
    def show_diagnose(self) -> bool:
        return self.pipeline_outcome == "FAILED" and not self.diagnosis

    @rx.var
    def show_fix(self) -> bool:
        return bool(self.diagnosis)

    @rx.var
    def error_message(self) -> str:
        return {
            "request_invalid": "The pipeline request was rejected. Review scope and pipeline type, then retry.",
            "conflict": "Another pipeline operation is in progress. Retry once it completes.",
            "validation": "The service rejected this request payload. Update inputs and retry.",
            "timeout": "The pipeline service timed out. Retry to request a fresh result.",
            "unauthorized": "Access denied for this tenant/platform scope.",
            "unavailable": "Pipeline service is unavailable right now.",
            "empty": "No run result was found for this request.",
            "configuration": "Pipeline integration settings are missing or invalid.",
            "api": "The service returned an invalid or unsuccessful response.",
        }.get(self.error_kind, "")

    def _clear_flow(self):
        self.run_id = ""
        self.pipeline_outcome = ""
        self.result_details = ""
        self.diagnosis = ""
        self.diagnosis_details = ""
        self.fix_note = ""
        self.error_kind = ""
        self.run_loading = False
        self.result_loading = False
        self.diagnose_loading = False
        self.status_text = "Select a pipeline type, then run a pipeline."

    def _invalidate(self):
        self._generation += 1
        self._scope = ("", "")
        self.pipeline_types = []
        self.pipeline_type = ""
        self._clear_flow()

    def sync_scope(
        self,
        tenant: str,
        platform: str,
        pipeline_types: list[str],
    ):
        normalized = [
            value.strip()
            for value in pipeline_types
            if isinstance(value, str) and value.strip()
        ]
        normalized = list(dict.fromkeys(normalized))
        scope = (tenant, platform)
        if scope != self._scope:
            self._generation += 1
            self._scope = scope
            self._clear_flow()
        self.pipeline_types = normalized
        changed_selection = False
        if self.pipeline_type not in set(normalized):
            self.pipeline_type = normalized[0] if normalized else ""
            changed_selection = True
        if changed_selection and self.run_id:
            self._clear_flow()
        if not normalized:
            self.status_text = "No pipeline types are available for this scope."

    async def _matches(self, generation: int, tenant: str, platform: str) -> bool:
        scope = await self.get_state(ScopeState)
        return generation == self._generation and (scope.tenant_id, scope.platform_id) == (
            tenant,
            platform,
        )

    @rx.event
    def set_pipeline_type(self, value: str):
        if not self.busy and value in set(self.pipeline_types):
            self.pipeline_type = value
            self.error_kind = ""
            self.status_text = "Ready to run the selected pipeline."

    @rx.event(background=True)
    async def run_pipeline(self):
        async with self:
            if self.busy or not self.pipeline_type:
                return
            scope = await self.get_state(ScopeState)
            tenant, platform = scope.tenant_id, scope.platform_id
            if not tenant or not platform:
                self.error_kind = "request_invalid"
                self.status_text = "Select tenant and platform before running."
                return
            generation = self._generation
            pipeline_type = self.pipeline_type
            self._clear_flow()
            self.run_loading = True
            self.status_text = "Starting pipeline run…"
        try:
            started = await start_pipeline_run(tenant, platform, pipeline_type)
            async with self:
                if not await self._matches(generation, tenant, platform):
                    return
                self.run_id = started.run_id
                self.run_loading = False
                self.result_loading = True
                self.status_text = (
                    started.status or "Pipeline started. Fetching result…"
                )
            # The real Data Pipeline app only writes a status record once the
            # run finishes, so poll it rather than checking once and failing.
            interval, attempts = polling_settings()
            result = None
            for attempt in range(attempts):
                if attempt:
                    await asyncio.sleep(interval)
                    async with self:
                        if not await self._matches(generation, tenant, platform):
                            return
                        self.status_text = (
                            f"Pipeline is running… (checked {attempt + 1}/{attempts})"
                        )
                try:
                    result = await get_pipeline_run_result(
                        tenant, platform, started.run_id
                    )
                    break
                except ServiceError as pending:
                    if pending.kind != "empty":
                        raise
            if result is None:
                raise ServiceError("timeout")
            async with self:
                if not await self._matches(generation, tenant, platform):
                    return
                self.result_loading = False
                self.pipeline_outcome = result.outcome
                self.result_details = result.details
                self.status_text = (
                    "Pipeline Passed"
                    if result.outcome == "PASSED"
                    else "Pipeline Failed"
                )
            # capstone-ui's backend (Agent 1) already records this incident
            # itself server-side - no client-side write needed anymore.
            history = await self._reload_history(tenant)
            async with self:
                if await self._matches(generation, tenant, platform) and history is not None:
                    self.history = history
        except Exception as error:
            kind = error.kind if isinstance(error, ServiceError) else "api"
            try:
                raise ServiceError(kind) from None
            except ServiceError as wrapped:
                logging.exception(f"Error: {wrapped}")
            async with self:
                if await self._matches(generation, tenant, platform):
                    self.error_kind = kind
                    self.status_text = ""
        finally:
            async with self:
                if await self._matches(generation, tenant, platform):
                    self.run_loading = False
                    self.result_loading = False

    @rx.event(background=True)
    async def diagnose(self):
        async with self:
            if self.busy or self.pipeline_outcome != "FAILED" or not self.run_id:
                return
            scope = await self.get_state(ScopeState)
            tenant, platform = scope.tenant_id, scope.platform_id
            generation = self._generation
            run_id = self.run_id
            pipeline_type = self.pipeline_type
            self.error_kind = ""
            self.fix_note = ""
            self.diagnose_loading = True
            self.status_text = "Requesting diagnosis from the agentic API…"
        try:
            result = await diagnose_pipeline_run(
                tenant,
                platform,
                run_id,
                pipeline_type,
            )
            async with self:
                if not await self._matches(generation, tenant, platform):
                    return
                self.diagnosis = result.diagnosis
                self.diagnosis_details = result.details
                self.status_text = "Diagnosis received."
            history = await self._reload_history(tenant)
            async with self:
                if await self._matches(generation, tenant, platform) and history is not None:
                    self.history = history
        except Exception as error:
            kind = error.kind if isinstance(error, ServiceError) else "api"
            try:
                raise ServiceError(kind) from None
            except ServiceError as wrapped:
                logging.exception(f"Error: {wrapped}")
            async with self:
                if await self._matches(generation, tenant, platform):
                    self.error_kind = kind
                    self.status_text = ""
        finally:
            async with self:
                if await self._matches(generation, tenant, platform):
                    self.diagnose_loading = False

    @rx.event
    def fix_placeholder(self):
        if self.show_fix:
            self.fix_note = "Fix API integration is not implemented yet."

    async def _reload_history(self, tenant: str) -> list[PipelineHistoryEntry] | None:
        try:
            return await get_pipeline_history(tenant)
        except Exception:
            logging.exception("Failed to load pipeline run history")
            return None

    @rx.event(background=True)
    async def load_history(self):
        async with self:
            if self.history_loading:
                return
            scope = await self.get_state(ScopeState)
            tenant = scope.tenant_id
            self.history_loading = True
        try:
            history = await get_pipeline_history(tenant)
            async with self:
                self.history = history
        except Exception:
            logging.exception("Failed to load pipeline run history")
        finally:
            async with self:
                self.history_loading = False

    @rx.event
    def toggle_history_view(self, incident_id: str):
        self.expanded_history_id = (
            "" if self.expanded_history_id == incident_id else incident_id
        )

    @rx.event(background=True)
    async def mark_history_resolved(self, incident_id: str):
        async with self:
            if self.resolve_loading:
                return
            scope = await self.get_state(ScopeState)
            tenant = scope.tenant_id
            self.resolve_loading = True
        try:
            await resolve_pipeline_incident(incident_id)
            history = await self._reload_history(tenant)
            if history is not None:
                async with self:
                    self.history = history
        except ServiceError:
            logging.exception("Failed to mark incident resolved")
        finally:
            async with self:
                self.resolve_loading = False

    @rx.event(background=True)
    async def approve_history_remediation(self, incident_id: str):
        async with self:
            if self.decision_loading:
                return
            scope = await self.get_state(ScopeState)
            tenant = scope.tenant_id
            self.decision_loading = True
        try:
            await decide_pipeline_remediation(incident_id, True)
            history = await self._reload_history(tenant)
            if history is not None:
                async with self:
                    self.history = history
        except ServiceError:
            logging.exception("Failed to approve remediation")
        finally:
            async with self:
                self.decision_loading = False

    @rx.event(background=True)
    async def reject_history_remediation(self, incident_id: str):
        async with self:
            if self.decision_loading:
                return
            scope = await self.get_state(ScopeState)
            tenant = scope.tenant_id
            self.decision_loading = True
        try:
            await decide_pipeline_remediation(incident_id, False)
            history = await self._reload_history(tenant)
            if history is not None:
                async with self:
                    self.history = history
        except ServiceError:
            logging.exception("Failed to reject remediation")
        finally:
            async with self:
                self.decision_loading = False
