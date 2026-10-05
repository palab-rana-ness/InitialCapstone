-- Schema for the capstone-ui incident-management backend (app/models_db.py).
-- Run this in pgAdmin's Query Tool against the "noticed_incidents" database
-- to create the tables manually. The FastAPI app also creates these
-- automatically on startup (SQLAlchemy Base.metadata.create_all), so running
-- this script is optional but gives you an explicit, inspectable copy.

CREATE TABLE IF NOT EXISTS incidents (
    incident_id VARCHAR PRIMARY KEY,
    tenant_id   VARCHAR NOT NULL,
    platform_id VARCHAR NOT NULL,
    pipeline    VARCHAR NOT NULL DEFAULT '',
    severity    VARCHAR NOT NULL,
    status      VARCHAR NOT NULL,
    problem     VARCHAR NOT NULL DEFAULT '',
    source      VARCHAR NOT NULL DEFAULT '',
    created_at  TIMESTAMPTZ NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_incidents_tenant_id ON incidents (tenant_id);
CREATE INDEX IF NOT EXISTS ix_incidents_platform_id ON incidents (platform_id);

CREATE TABLE IF NOT EXISTS incident_payloads (
    incident_id           VARCHAR PRIMARY KEY REFERENCES incidents(incident_id) ON DELETE CASCADE,
    failure_payload       JSON NOT NULL DEFAULT '{}',
    agent_result_payload  JSON NOT NULL DEFAULT '{}',
    created_at            TIMESTAMPTZ NOT NULL,
    updated_at            TIMESTAMPTZ NOT NULL
);
