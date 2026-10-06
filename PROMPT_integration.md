Use the audit from the previous step as the ONLY basis for modifications.

Now you may modify the existing code.

IMPORTANT:
Keep the THREE applications separate.

Do NOT merge:

UI
Data Pipeline
AI Diagnosis

They must continue running as three independent applications with their own ports.

Do NOT rewrite working business logic.

Do NOT create a new architecture.

Do NOT invent fields.

Do NOT use a schema from an earlier conversation.

Use the schema discovered from the actual repository.

==================================================
GOAL
====

Make the existing three applications communicate correctly end-to-end.

The desired flow is:

==================================================
FLOW A — RUN PIPELINE
=====================

UI
|
| user clicks "Run Pipeline"
v
Data Pipeline
|
| starts pipeline
v
UI shows RUNNING

The UI must not fake the RUNNING state.

Use the actual Data Pipeline response/status mechanism.

==================================================
FLOW B — PIPELINE FAILURE
=========================

If the pipeline fails:

Data Pipeline
|
| actual failure status
v
UI

UI must show:

FAILED

Only when the current pipeline run is FAILED:

show:
AI Diagnosis button

When status is:

RUNNING
SUCCESS

AI Diagnosis must be hidden.

==================================================
FLOW C — AI DIAGNOSIS
=====================

When the user clicks:

AI Diagnosis

UI sends the ACTUAL validated incident payload required by the AI Diagnosis application.

Do not send unnecessary fields.

Do not remove fields that are actually required.

Use exactly one canonical contract between UI and AI Diagnosis.

If the current applications use different field names for the same concept, fix the boundary mismatch with the smallest possible transformation.

Example:

snake_case vs camelCase

Only normalize where necessary.

Do not duplicate models unnecessarily.

==================================================
SCHEMA RULE
===========

The actual repository is the source of truth.

For every field crossing a service boundary, verify:

* exact field name
* type
* required/optional
* nullability
* producer
* consumer

Remove fields ONLY when the audit proved that they are not used.

If a field is used by another application, do not remove it.

If two fields represent the same thing:

determine which existing field is actually used,
then remove unnecessary duplication only when safe.

==================================================
NEW RELIC
=========

Use the audit to determine what the AI Diagnosis service receives from the UI and what it obtains from New Relic.

Do not fabricate New Relic data.

Do not make the UI send logs if the AI Diagnosis service is supposed to retrieve them from New Relic.

Do not make AI Diagnosis retrieve logs from New Relic if the existing architecture already sends the required logs and there is no reason to duplicate the request.

Fix only what is necessary according to the actual repository.

==================================================
LANGGRAPH
=========

When AI Diagnosis receives the request, the LangGraph must actually execute.

Ensure the graph can:

1. validate incident
2. resolve adapter
3. obtain required evidence
4. retrieve similar historical incidents
5. diagnose failure
6. generate remedies
7. validate remedies
8. return structured result

Do not execute remediation.

==================================================
CHROMA
======

Chroma is AI incident memory.

Preserve Chroma.

Historical incident retrieval MUST respect tenant isolation.

Current request:

tenant_id = X

must not retrieve historical incidents belonging to:

tenant_id = Y

Use the existing Chroma implementation where possible.

Fix tenant filtering if it is incorrect.

==================================================
SPARK
=====

Spark is the main adapter for this project.

Keep adapter-based design.

Do not create real Spark cluster execution/integration unless it already exists and is required by the current application.

The incident information is already available to the AI Diagnosis service.

Do not invent Spark APIs.

Synapse remains only a prototype adapter.

==================================================
FLOW D — AI RESPONSE
====================

AI Diagnosis should return only the fields actually required by the UI.

At minimum the UI should be able to display, using the existing project models:

* where the pipeline failed
* root cause
* supporting evidence where implemented
* remedies
* recommended action

Do not add random fields just because they seem useful.

Use the existing UI code to determine what it actually needs.

==================================================
FLOW E — SUCCESS
================

When pipeline succeeds:

Data Pipeline
|
| success
v
UI shows SUCCESS

UI must NOT display AI Diagnosis.

When a new pipeline starts:

clear stale diagnosis data from a previous run.

==================================================
FIX IT BUTTON
=============

The UI may show:

Fix It

after diagnosis.

IMPORTANT:

This button must NOT execute anything.

Do NOT connect it to a remediation endpoint.

Do NOT restart the pipeline.

Do NOT modify Spark.

Do NOT execute shell commands.

It is only a future placeholder.

==================================================
PORTS / URLS
============

Preserve the existing configured ports.

Fix:

* wrong service URLs
* wrong ports
* wrong paths
* wrong HTTP methods
* CORS
* environment variable mismatches

Do not hard-code URLs if the existing architecture supports configuration.

==================================================
ERROR HANDLING
==============

Fix integration failures involving:

* connection refused
* timeout
* malformed JSON
* HTTP 4xx
* HTTP 5xx
* missing fields
* invalid adapter
* unavailable AI Diagnosis service
* unavailable Chroma
* unavailable New Relic

Do not solve failures by returning fake SUCCESS.

==================================================
CHANGE CONTROL
==============

Before each modification, verify that the problem is actually present in the code.

Make the smallest change necessary.

After modifications, check all three applications again for:

* broken imports
* stale schema references
* endpoint mismatches
* incorrect environment variable names
* circular dependencies
* serialization errors

At the end report:

1. Files changed
2. Why each file changed
3. Schema changes
4. Fields removed
5. Fields retained
6. API changes
7. Port/URL changes
8. Any unresolved issue

Do NOT perform full end-to-end runtime debugging yet.
