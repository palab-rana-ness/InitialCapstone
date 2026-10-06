"""FastAPI application entrypoint."""
from __future__ import annotations

import logging
import os

from fastapi import FastAPI

from app.api.routes import router
from app.config import get_settings

settings = get_settings()
logging.basicConfig(level=settings.log_level)
logger = logging.getLogger(__name__)

if settings.langsmith_tracing and settings.langsmith_api_key:
    # Export explicitly (rather than relying on whichever .env python-dotenv's
    # upward search happens to find first) so tracing is always on when configured.
    os.environ["LANGSMITH_TRACING"] = "true"
    os.environ["LANGSMITH_API_KEY"] = settings.langsmith_api_key
    os.environ["LANGSMITH_PROJECT"] = settings.langsmith_project
    os.environ["LANGSMITH_ENDPOINT"] = settings.langsmith_endpoint
    logger.info("LangSmith tracing enabled for project '%s'", settings.langsmith_project)
else:
    logger.info("LangSmith tracing disabled (no LANGSMITH_TRACING/LANGSMITH_API_KEY configured)")

app = FastAPI(
    title=settings.app_name,
    description="Agentic backend for autonomous data-pipeline incident diagnosis.",
    version="0.1.0",
)

app.include_router(router)


@app.get("/health")
async def health() -> dict[str, str]:
    """Simple liveness check."""
    return {"status": "ok"}
