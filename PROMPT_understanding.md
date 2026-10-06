You are working on an EXISTING repository containing exactly THREE SEPARATE APPLICATIONS:

1. UI application
2. Data Pipeline application
3. AI Diagnosis application

They are separate applications with separate ports and must remain separate.

IMPORTANT:
This is an EXISTING project.

Do NOT generate a new architecture.
Do NOT create a new schema based on assumptions.
Do NOT use any schema from previous conversations as the source of truth.
Do NOT modify files in this step.
Do NOT refactor.
Do NOT merge the applications.

Your first job is ONLY to inspect the repository and understand what already exists.

==================================================
PART 1 — IDENTIFY THE THREE APPLICATIONS
========================================

Find the exact folders that belong to:

* UI
* Data Pipeline
* AI Diagnosis

For each application determine:

* framework
* language
* entry point
* startup command
* port
* environment/configuration files
* dependencies
* exposed APIs
* APIs it calls

Do not guess.

For every conclusion, give the exact file path and relevant function/class.

==================================================
PART 2 — TRACE THE ACTUAL END-TO-END FLOW
=========================================

Trace the real code, not the intended architecture.

Determine how this currently works:

USER
|
| clicks Run Pipeline
v
UI
|
v
Data Pipeline
|
v
UI

Then determine:

What request does UI actually send to Data Pipeline?

What response does Data Pipeline actually return?

How does UI determine:

RUNNING
SUCCESS
FAILED

How is pipeline execution identified?

Find whether the project uses:

* run_id
* execution_id
* pipeline_id
* incident_id
* another identifier

Use the identifier already present in the code.

==================================================
PART 3 — TRACE AI DIAGNOSIS FLOW
================================

Determine exactly what happens when:

Pipeline status = FAILED

and the user clicks:

AI Diagnosis

Trace:

UI
->
AI Diagnosis API
->
LangGraph
->
Chroma
->
New Relic if implemented
->
LLM
->
AI Diagnosis response
->
UI

Identify every actual request and response.

==================================================
PART 4 — DETERMINE THE REAL SCHEMA
==================================

THIS IS CRITICAL.

Do NOT invent a schema.

Search the complete repository for:

* Pydantic models
* dataclasses
* TypeScript interfaces/types
* request models
* response models
* JSON examples
* API request bodies
* API response bodies
* frontend fetch/axios/request code
* FastAPI endpoint definitions
* LangGraph input state
* Chroma metadata
* New Relic request/response structures

Create a data-flow table:

FIELD
SOURCE
CONSUMER
PURPOSE
REQUIRED/OPTIONAL
ACTUAL TYPE

For example:

tenant_id
UI
AI Diagnosis
tenant identification
required
string

Only include fields that actually exist or are required by the existing implementation.

==================================================
PART 5 — REMOVE UNNECESSARY SCHEMA FIELDS
=========================================

Identify fields that:

* are defined but never used
* are sent but never consumed
* are duplicated
* are stored but never needed
* exist only because of an old implementation
* conflict with another service
* are unnecessary for the AI diagnosis flow

Do NOT delete anything yet.

Instead report:

FIELD
CURRENT LOCATION
USED BY
SAFE TO REMOVE?
REASON

==================================================
PART 6 — DETERMINE DATA OWNERSHIP
=================================

Determine from the actual code which information comes from:

A. UI / Data Pipeline

and which information should come from:

B. New Relic

Do not assume.

Specifically identify:

* identifiers sent by UI
* pipeline information sent by UI/Data Pipeline
* failure information already available
* logs
* stack traces
* telemetry
* metrics
* timestamps
* infrastructure evidence

Determine whether the AI Diagnosis service currently receives logs directly or is expected to obtain them from New Relic.

If both happen, identify duplication.

==================================================
PART 7 — ADAPTER
================

Inspect how `adapter` currently works.

Determine:

* where it is defined
* possible values
* how Spark is handled
* how Synapse is handled
* whether adapter affects the LangGraph
* whether adapter-specific code exists

Spark is the main prototype technology for our current implementation.

Synapse is only a prototype.

Do NOT add another adapter.

==================================================
PART 8 — MULTI-TENANCY
======================

Trace tenant_id through:

UI
->
Data Pipeline
->
AI Diagnosis
->
Chroma

Determine exactly how tenant isolation currently works.

Check whether Chroma searches can accidentally return another tenant's incidents.

Do not modify anything yet.

==================================================
PART 9 — PORTS AND COMMUNICATION
================================

Find the actual ports and URLs.

Determine:

UI port:
Data Pipeline port:
AI Diagnosis port:

Determine actual service-to-service URLs.

Check for:

* hard-coded localhost URLs
* wrong ports
* wrong HTTP methods
* wrong endpoint paths
* CORS problems
* environment variable mismatches

==================================================
PART 10 — OUTPUT
================

DO NOT MODIFY CODE.

Return ONLY an audit report with:

1. Three application folders
2. Framework/language for each
3. Entry point for each
4. Port for each
5. Startup command for each
6. All service endpoints
7. UI → Data Pipeline flow
8. Data Pipeline → UI flow
9. UI → AI Diagnosis flow
10. AI Diagnosis → Chroma flow
11. AI Diagnosis → New Relic flow
12. LangGraph flow
13. Actual schema currently used
14. Schema conflicts found
15. Unused/unnecessary fields
16. Data ownership: UI vs Data Pipeline vs New Relic
17. Multi-tenant flow
18. Problems preventing end-to-end execution
19. Exact files that need changes

For every finding, cite the exact file path and function/class.

Again:

NO CODE CHANGES IN THIS STEP.
NO NEW SCHEMA.
NO ASSUMPTIONS.
NO REFACTORING.
