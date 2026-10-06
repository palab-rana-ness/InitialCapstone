# Capstone1 — Autonomous Pipeline Incident Management

Three cooperating services plus a Reflex UI:

| Service | Folder | Port | Purpose |
|---|---|---|---|
| AiDiagnosis | `AiDiagnosis/` | 8000 | LangGraph-based incident diagnosis (Groq + New Relic + Chroma) |
| DataPipeline | `DataPipeline/` | 8001 | Mock retail data API + pipeline run/status endpoints |
| capstone-ui API | `capstone-ui/` | 8003 | Postgres-backed incident management backend used by the UI |
| capstone-ui frontend | `capstone-ui/` | 3000 (ws backend 8002) | Reflex operator UI |

## Prerequisites

- Python environments already created per service (`AiDiagnosis/.venv`, `DataPipeline/.venv`, `capstone-ui/.venv`).
- PostgreSQL running locally and reachable via `DATABASE_URL` in the repo-root `.env`.
- `JAVA_HOME` and `HADOOP_HOME` set at the Windows user level (required by DataPipeline's Spark subprocess).
- Repo-root `.env` populated (Groq, New Relic, AWS/S3, `FASTAPI_BASE_URL`, `DATA_PIPELINE_BASE_URL`, `AI_DIAGNOSIS_BASE_URL`, `DATABASE_URL`, etc.).

## Run everything

From the repo root, in PowerShell:

```powershell
./start-all.ps1
```

This opens one PowerShell window per service (AiDiagnosis, DataPipeline, capstone-ui API, capstone-ui Reflex UI), each activating its own virtual environment. Close a window (or `Ctrl+C` inside it) to stop that service.

Once all four windows show "Application startup complete" / "App Running", open the UI at [http://localhost:3000](http://localhost:3000).

## Running services individually

```powershell
# AiDiagnosis
cd AiDiagnosis; .\.venv\Scripts\Activate.ps1; uvicorn app.main:app --port 8000

# DataPipeline (needs JAVA_HOME/HADOOP_HOME on PATH)
cd DataPipeline; .\.venv\Scripts\Activate.ps1
$env:JAVA_HOME = [System.Environment]::GetEnvironmentVariable('JAVA_HOME','User')
$env:HADOOP_HOME = [System.Environment]::GetEnvironmentVariable('HADOOP_HOME','User')
$env:PATH = "$env:JAVA_HOME\bin;$env:HADOOP_HOME\bin;$env:PATH"
uvicorn app.main:app --port 8001

# capstone-ui API
cd capstone-ui; .\.venv\Scripts\Activate.ps1; uvicorn app.main:app --port 8003

# capstone-ui Reflex UI
cd capstone-ui; .\.venv\Scripts\Activate.ps1; reflex run --backend-port 8002 --frontend-port 3000
```

## More docs

- [capstone-ui/TEAMMATE_ONBOARDING.md](capstone-ui/TEAMMATE_ONBOARDING.md)
- [capstone-ui/ARCHITECTURE_FLOW_DIAGRAM.md](capstone-ui/ARCHITECTURE_FLOW_DIAGRAM.md)
- [AiDiagnosis/README.md](AiDiagnosis/README.md)
- [DataPipeline/README.md](DataPipeline/README.md)
