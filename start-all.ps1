<#
.SYNOPSIS
  Starts all four Capstone1 services, each in its own PowerShell window.

.DESCRIPTION
  - AiDiagnosis FastAPI backend       -> http://127.0.0.1:8000
  - DataPipeline FastAPI backend      -> http://127.0.0.1:8001
  - capstone-ui FastAPI backend       -> http://127.0.0.1:8003
  - capstone-ui Reflex app (UI)       -> http://127.0.0.1:3000 (ws backend on 8002)

  Each service keeps its own .venv, so they are started independently with
  their own Activate.ps1. DataPipeline additionally needs JAVA_HOME/HADOOP_HOME
  on PATH for the Spark subprocess it launches.

.NOTES
  Requires: PostgreSQL already running and reachable via DATABASE_URL in
  the repo-root .env (used by the capstone-ui FastAPI backend).
#>

$root = $PSScriptRoot

function Start-Service {
    param(
        [string]$Title,
        [string]$WorkDir,
        [string]$Command
    )
    Start-Process powershell.exe -ArgumentList @(
        "-NoExit",
        "-Command",
        "`$host.ui.RawUI.WindowTitle = '$Title'; Set-Location '$WorkDir'; $Command"
    )
}

Start-Service -Title "AiDiagnosis (8000)" -WorkDir "$root\AiDiagnosis" `
    -Command "& .\.venv\Scripts\Activate.ps1; uvicorn app.main:app --port 8000"

Start-Service -Title "DataPipeline (8001)" -WorkDir "$root\DataPipeline" `
    -Command "& .\.venv\Scripts\Activate.ps1; `$env:JAVA_HOME = [System.Environment]::GetEnvironmentVariable('JAVA_HOME','User'); `$env:HADOOP_HOME = [System.Environment]::GetEnvironmentVariable('HADOOP_HOME','User'); `$env:PATH = `$env:JAVA_HOME + '\bin;' + `$env:HADOOP_HOME + '\bin;' + `$env:PATH; uvicorn app.main:app --port 8001"

Start-Service -Title "capstone-ui API (8003)" -WorkDir "$root\capstone-ui" `
    -Command "& .\.venv\Scripts\Activate.ps1; uvicorn app.main:app --port 8003"

Start-Service -Title "capstone-ui Reflex (3000)" -WorkDir "$root\capstone-ui" `
    -Command "& .\.venv\Scripts\Activate.ps1; reflex run --backend-port 8002 --frontend-port 3000"

Write-Host ""
Write-Host "Started 4 services, each in its own window:" -ForegroundColor Green
Write-Host "  AiDiagnosis backend   : http://127.0.0.1:8000"
Write-Host "  DataPipeline backend  : http://127.0.0.1:8001"
Write-Host "  capstone-ui API       : http://127.0.0.1:8003"
Write-Host "  capstone-ui frontend  : http://127.0.0.1:3000"
Write-Host ""
Write-Host "Close each window (or Ctrl+C inside it) to stop that service." -ForegroundColor Yellow
