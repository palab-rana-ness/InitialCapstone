from datetime import datetime, timezone

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from sqlalchemy.orm import Session

from app import db_incidents, store
from app.database.connection import get_db
from app.models_db import IncidentDB
from app.schemas import (
    ConfigUpdateRequest,
    ConfigValues,
    IncidentListResponse,
    Tenant,
    TenantConfig,
    TenantListResponse,
    TenantPlatformConfig,
    TenantPlatformConfigSaved,
)

router = APIRouter(prefix="/api/v1/tenants", tags=["Tenant"])

DEFAULT_PLATFORM_ID = "synapse"


def _tenant_exists(db: Session, tenant_id: str) -> bool:
    return (
        db.query(IncidentDB.tenant_id)
        .filter(IncidentDB.tenant_id == tenant_id)
        .first()
        is not None
    )


def _get_tenant(db: Session, tenant_id: str) -> Tenant:
    if not _tenant_exists(db, tenant_id):
        raise HTTPException(status_code=404, detail="Tenant not found")
    return Tenant(tenant_id=tenant_id, name=db_incidents.humanize(tenant_id))


@router.get("", response_model=TenantListResponse)
def list_tenants(db: Session = Depends(get_db)):
    rows = (
        db.query(IncidentDB.tenant_id)
        .distinct()
        .order_by(IncidentDB.tenant_id.asc())
        .all()
    )
    items = [
        Tenant(
            tenant_id=row.tenant_id,
            name=(
                store.tenants[row.tenant_id].name
                if row.tenant_id in store.tenants
                else db_incidents.humanize(row.tenant_id)
            ),
        )
        for row in rows
    ]
    return TenantListResponse(items=items)


@router.get("/{tenant_id}/incidents", response_model=IncidentListResponse)
def list_tenant_incidents(
    tenant_id: str,
    platform_id: str | None = None,
    db: Session = Depends(get_db),
):
    _get_tenant(db, tenant_id)
    items = db_incidents.list_incidents(
        db, tenant_id=tenant_id, platform_id=platform_id
    )
    return IncidentListResponse(items=items)


def _get_or_create_platform_config(
    tenant_id: str, platform_id: str
) -> TenantPlatformConfig:
    key = (tenant_id, platform_id)
    config = store.tenant_platform_configs.get(key)
    if config is None:
        config = TenantPlatformConfig(
            tenant_id=tenant_id,
            platform_id=platform_id,
            values=ConfigValues(),
            revision=0,
            updated_at=datetime.now(timezone.utc).isoformat(),
            can_edit=True,
            policies=store.REMEDIATION_POLICIES,
            action_options=store.CONFIG_ACTION_OPTIONS,
        )
        store.tenant_platform_configs[key] = config
    return config


@router.get("/{tenant_id}/config", response_model=TenantPlatformConfig)
def get_tenant_config(
    tenant_id: str,
    platform_id: str = Query(DEFAULT_PLATFORM_ID),
    db: Session = Depends(get_db),
):
    _get_tenant(db, tenant_id)
    return _get_or_create_platform_config(tenant_id, platform_id or DEFAULT_PLATFORM_ID)


@router.put("/{tenant_id}/config", response_model=TenantPlatformConfigSaved)
def update_tenant_config(
    tenant_id: str,
    body: ConfigUpdateRequest,
    platform_id: str = Query(DEFAULT_PLATFORM_ID),
    idempotency_key: str = Header(alias="Idempotency-Key"),
    db: Session = Depends(get_db),
):
    _get_tenant(db, tenant_id)
    platform_id = platform_id or DEFAULT_PLATFORM_ID

    if body.values.remediation_policy not in {
        p.id for p in store.REMEDIATION_POLICIES
    }:
        raise HTTPException(status_code=422, detail="Unknown remediation_policy")
    if not set(body.values.allowed_actions).issubset(store.CONFIG_ACTION_OPTIONS):
        raise HTTPException(status_code=422, detail="Unknown allowed_actions")

    cache_key = (tenant_id, platform_id, idempotency_key)
    cached = store.config_save_cache.get(cache_key)
    if cached is not None:
        return cached

    current = _get_or_create_platform_config(tenant_id, platform_id)
    if body.revision != current.revision:
        raise HTTPException(
            status_code=409, detail="Configuration revision is out of date"
        )

    saved = TenantPlatformConfigSaved(
        tenant_id=tenant_id,
        platform_id=platform_id,
        values=body.values,
        revision=current.revision + 1,
        updated_at=datetime.now(timezone.utc).isoformat(),
        can_edit=True,
        policies=store.REMEDIATION_POLICIES,
        action_options=store.CONFIG_ACTION_OPTIONS,
        request_id=idempotency_key,
    )
    store.tenant_platform_configs[(tenant_id, platform_id)] = TenantPlatformConfig(
        **saved.model_dump(exclude={"saved", "request_id"})
    )
    store.config_save_cache[cache_key] = saved
    return saved
