import reflex as rx
import asyncio
import json
import logging
import math
import os
import socket
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode, urlsplit
from urllib.request import Request, HTTPRedirectHandler, build_opener


class ServiceError(Exception):
    def __init__(self, kind: str):
        self.kind = kind
        super().__init__(kind)


def _validated_base_url(env_var: str) -> str:
    value = os.getenv(env_var, "").strip().rstrip("/")
    parts = urlsplit(value)
    if (
        parts.scheme not in {"http", "https"}
        or not parts.hostname
        or parts.username
        or parts.password
        or parts.query
        or parts.fragment
    ):
        raise ServiceError("unavailable")
    return value


def base_url() -> str:
    return _validated_base_url("FASTAPI_BASE_URL")


def api_configured() -> bool:
    return bool(os.getenv("FASTAPI_BASE_URL", "").strip())


def data_pipeline_base_url() -> str:
    """Base URL of the separate Data Pipeline application (its own port/process)."""
    return _validated_base_url("DATA_PIPELINE_BASE_URL")


def data_pipeline_configured() -> bool:
    return bool(os.getenv("DATA_PIPELINE_BASE_URL", "").strip())


def ai_diagnosis_base_url() -> str:
    """Base URL of the separate AI Diagnosis application (its own port/process)."""
    return _validated_base_url("AI_DIAGNOSIS_BASE_URL")


def ai_diagnosis_configured() -> bool:
    return bool(os.getenv("AI_DIAGNOSIS_BASE_URL", "").strip())


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def request_bytes(
    path: str,
    params: dict[str, str],
    method: str = "GET",
    payload: bytes = b"",
    idempotency_key: str = "",
    resolve_base=base_url,
    extra_headers: dict[str, str] | None = None,
) -> bytes:
    kind = "api"
    try:
        base = resolve_base()
        if method not in {"GET", "POST", "PUT"}:
            raise ServiceError("request_invalid")
        timeout = float(os.getenv("FASTAPI_TIMEOUT", "12"))
        if not math.isfinite(timeout) or timeout <= 0:
            raise ServiceError("unavailable")
        query = urlencode(
            {key: value for key, value in params.items() if value}
        )
        url = f"{base}{path}"
        if query:
            url = f"{url}?{query}"
        headers = {"Accept": "application/json"}
        token = os.getenv("FASTAPI_BEARER_TOKEN", "")
        if token:
            headers["Authorization"] = f"Bearer {token}"
        if extra_headers:
            headers.update(extra_headers)
        if idempotency_key:
            if any(ord(c) < 32 for c in idempotency_key):
                raise ServiceError("request_invalid")
            headers["Idempotency-Key"] = idempotency_key
        if method in {"POST", "PUT"}:
            headers["Content-Type"] = "application/json"
        request = Request(
            url,
            headers=headers,
            method=method,
            data=(payload or b"{}") if method in {"POST", "PUT"} else None,
        )
        with build_opener(NoRedirect()).open(
            request, timeout=timeout
        ) as response:
            return response.read()
    except HTTPError as error:
        logging.exception("Unexpected error")
        kind = (
            "unauthorized"
            if error.code in {401, 403}
            else "empty"
            if error.code == 404
            else "unavailable"
            if error.code >= 500
            else {
                400: "request_invalid",
                409: "conflict",
                422: "validation",
            }.get(error.code, "api")
        )
    except (TimeoutError, socket.timeout):
        logging.exception("Unexpected error")
        kind = "timeout"
    except URLError as error:
        logging.exception("Unexpected error")
        kind = (
            "timeout"
            if isinstance(error.reason, (TimeoutError, socket.timeout))
            else "unavailable"
        )
    except ServiceError as error:
        logging.exception("Unexpected error")
        kind = error.kind
    except Exception:
        logging.exception("Unexpected error")
        kind = "api"
    logging.error("Request failed")
    raise ServiceError(kind) from None


def segment(value: str) -> str:
    if (
        not value.strip()
        or value in {".", ".."}
        or any(ord(c) < 32 for c in value)
    ):
        raise ServiceError("request_invalid")
    return quote(value, safe="")


def incident_path(identifier: str = "", suffix: str = "") -> str:
    if suffix not in {
        "",
        "/timeline",
        "/logs",
        "/logs/refresh",
        "/analyze",
        "/diagnosis",
        "/history",
        "/remediation",
        "/approve",
        "/reject",
        "/retry",
        "/execution",
        "/validation",
    }:
        raise ServiceError("request_invalid")
    if not identifier:
        if suffix:
            raise ServiceError("request_invalid")
        return "/api/v1/incidents"
    return f"/api/v1/incidents/{segment(identifier)}{suffix}"


def _validate_path_template(value: str) -> str:
    path = value.strip()
    if (
        not path
        or not path.startswith("/")
        or "?" in path
        or "#" in path
        or any(ord(char) < 32 for char in path)
    ):
        raise ServiceError("request_invalid")
    return path.rstrip("/") or "/"


def pipeline_path(operation: str) -> str:
    templates = {
        "start": os.getenv("PIPELINE_RUN_START_PATH", "/api/v1/pipeline/run"),
        "result": os.getenv("PIPELINE_RESULT_PATH", "/api/v1/pipeline/status"),
    }
    if operation not in templates:
        raise ServiceError("request_invalid")
    return _validate_path_template(templates[operation])


def ai_diagnosis_path() -> str:
    return _validate_path_template(
        os.getenv("AI_DIAGNOSIS_PATH", "/api/v1/incidents/ai-diagnosis")
    )


async def json_request(
    path: str,
    params: dict[str, str],
    method: str = "GET",
    body: object = None,
    idempotency_key: str = "",
    resolve_base=base_url,
    extra_headers: dict[str, str] | None = None,
) -> object:
    try:
        payload = json.dumps(
            body if body is not None else {}, allow_nan=False
        ).encode()
    except Exception:
        logging.exception("Unexpected error")
        logging.error("Request encoding failed: %s", "request_invalid")
        payload = b""
    if not payload:
        raise ServiceError("request_invalid")
    if method == "GET" and not idempotency_key:
        raw = await asyncio.to_thread(
            request_bytes,
            path,
            params,
            method,
            resolve_base=resolve_base,
            extra_headers=extra_headers,
        )
    else:
        raw = await asyncio.to_thread(
            request_bytes,
            path,
            params,
            method,
            payload,
            idempotency_key,
            resolve_base=resolve_base,
            extra_headers=extra_headers,
        )
    try:
        return json.loads(raw, parse_constant=lambda _: invalid_json())
    except Exception:
        logging.exception("Unexpected error")
        logging.error("Response decoding failed: %s", "api")
    raise ServiceError("api") from None


def invalid_json():
    raise ServiceError("api")


async def catalog_json(kind: str) -> object:
    if kind not in {"tenants", "platforms"}:
        raise ServiceError("request_invalid")
    return await json_request(f"/api/v1/{kind}", {})


async def retail_tenants_json() -> object:
    """List tenants known to the Data Pipeline's retail data lake (its own app/port).

    This is the canonical tenant catalog -- it is populated independently of
    whether any incident has ever been recorded for a tenant.
    """
    return await json_request(
        "/api/v1/tenants", {}, resolve_base=data_pipeline_base_url
    )


async def config_json(
    tenant: str,
    platform: str,
    method: str = "GET",
    body: object = None,
    idempotency_key: str = "",
    platform_config: bool = False,
) -> object:
    if platform_config:
        if method != "GET":
            raise ServiceError("request_invalid")
        path = f"/api/v1/platforms/{segment(platform)}/config"
    else:
        if method not in {"GET", "PUT"}:
            raise ServiceError("request_invalid")
        path = f"/api/v1/tenants/{segment(tenant)}/config"
    return await json_request(
        path,
        {"tenant_id": tenant, "platform_id": platform},
        method,
        body,
        idempotency_key,
    )


async def incident_json(
    tenant: str,
    platform: str,
    identifier: str = "",
    suffix: str = "",
    method: str = "GET",
    body: object = None,
    idempotency_key: str = "",
) -> object:
    expected = (
        "POST"
        if suffix
        in {"/logs/refresh", "/analyze", "/approve", "/reject", "/retry"}
        else "GET"
    )
    if method != expected:
        raise ServiceError("request_invalid")
    path = incident_path(identifier, suffix)
    if tenant and not identifier:
        path = f"/api/v1/tenants/{segment(tenant)}/incidents"
    return await json_request(
        path,
        {"tenant_id": tenant, "platform_id": platform},
        method,
        body,
        idempotency_key,
    )


async def pipeline_json(
    operation: str,
    tenant: str,
    platform: str,
    run_id: str = "",
    method: str = "POST",
    body: object = None,
    idempotency_key: str = "",
) -> object:
    """Call the separate Data Pipeline application (its own base URL/port).

    Tenant scoping is carried in the X-Tenant-ID header, matching that
    application's `validate_tenant_header` dependency.
    """
    expected = {"start": "POST", "result": "GET"}
    if operation not in expected or method != expected[operation]:
        raise ServiceError("request_invalid")
    if not tenant.strip():
        raise ServiceError("request_invalid")
    params = {"run_id": run_id} if operation == "result" else {}
    return await json_request(
        pipeline_path(operation),
        params,
        method,
        body,
        idempotency_key,
        resolve_base=data_pipeline_base_url,
        extra_headers={"X-Tenant-ID": tenant},
    )


async def ai_diagnosis_json(body: dict) -> object:
    """Call the separate AI Diagnosis application's incident-diagnosis endpoint."""
    return await json_request(
        ai_diagnosis_path(),
        {},
        "POST",
        body,
        resolve_base=ai_diagnosis_base_url,
    )


async def pipeline_run_ingest_json(body: dict) -> object:
    """Record a completed pipeline run as a history entry in capstone-ui's own backend."""
    return await json_request(
        "/api/v1/incidents/pipeline-run",
        {},
        "POST",
        body,
    )


async def diagnosis_result_json(incident_id: str, body: dict) -> object:
    """Store an AI Diagnosis result against a pipeline-run incident."""
    return await json_request(
        f"/api/v1/incidents/{segment(incident_id)}/diagnosis-result",
        {},
        "PUT",
        body,
    )


async def resolve_incident_json(incident_id: str) -> object:
    """Human-only action: mark a pipeline-run incident resolved. Executes nothing."""
    return await json_request(
        f"/api/v1/incidents/{segment(incident_id)}/resolve",
        {},
        "POST",
        {},
    )


async def pipeline_history_json(tenant: str) -> object:
    """List recent pipeline-run incidents (history) for a tenant."""
    return await json_request(
        "/api/v1/incidents",
        {"tenant_id": tenant},
        "GET",
    )
