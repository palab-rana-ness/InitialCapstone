Now test the EXISTING THREE APPLICATIONS end-to-end.

The applications MUST remain separate:

1. UI
2. Data Pipeline
3. AI Diagnosis

Do not merge them.

Do not redesign them.

Your task is to make the actual integrated system RUNNING.

==================================================
STEP 1 — START ALL THREE
========================

Start each application independently using its actual configured startup command.

Verify:

UI:

* starts
* correct port
* loads correctly

Data Pipeline:

* starts
* correct port
* API responds

AI Diagnosis:

* starts
* correct port
* API responds

Verify the service URLs used by other applications actually point to these ports.

==================================================
STEP 2 — TEST RUN PIPELINE
==========================

From the actual UI:

click:

Run Pipeline

Verify the real network request.

Trace:

UI
->
Data Pipeline

Capture/inspect the actual request and response.

Then verify:

UI = RUNNING

Do not fake this state.

==================================================
STEP 3 — TEST SUCCESS
=====================

Run a successful pipeline scenario.

Verify:

Data Pipeline
->
actual success status
->
UI

UI must show:

SUCCESS

Verify:

AI Diagnosis button = hidden

Verify previous diagnosis data is cleared.

If this fails:

find the actual request/response mismatch.

Fix it.

Retest.

==================================================
STEP 4 — TEST FAILURE
=====================

Run a failing pipeline scenario.

Verify:

Data Pipeline
->
actual FAILED status
->
UI

UI must show:

FAILED

Verify:

AI Diagnosis button = visible

The button must not appear for SUCCESS or RUNNING.

==================================================
STEP 5 — TEST AI DIAGNOSIS
==========================

Click:

AI Diagnosis

Inspect the actual JSON request sent from UI.

Compare it against the actual AI Diagnosis request model.

Do NOT assume the schema.

Check:

* field names
* field types
* required fields
* optional fields
* tenant_id
* incident/run identifier
* adapter
* pipeline
* failure information
* logs/details
* metadata

Verify that only the appropriate information is sent by UI.

Verify New Relic data is obtained from New Relic only when the current architecture requires it.

Do not fabricate data to make diagnosis succeed.

==================================================
STEP 6 — TRACE LANGGRAPH
========================

Verify that the AI Diagnosis API actually invokes the LangGraph.

Trace:

API
->
graph
->
adapter
->
evidence
->
Chroma
->
diagnosis
->
remediation
->
response

Verify the graph actually runs.

Do not replace the graph with a hard-coded response.

==================================================
STEP 7 — TEST CHROMA
====================

Verify the current incident can be stored/retrieved through Chroma.

Verify similar incident retrieval.

Verify tenant isolation.

Test:

tenant_A current incident
->
only tenant_A historical incidents

tenant_B current incident
->
only tenant_B historical incidents

If a cross-tenant result is possible, fix it.

==================================================
STEP 8 — TEST SPARK
===================

Use:

adapter = spark

Test with realistic JSON already supported by the repository.

Verify that Spark-specific diagnosis/remediation logic is used.

Do not make external Spark cluster calls unless the existing application explicitly requires them.

==================================================
STEP 9 — VERIFY AI RESPONSE
===========================

Verify the response reaches the UI.

The UI should be able to display:

* pipeline failure location
* root cause
* remedies
* recommended action
* evidence if supported by the existing schema

Do not add unnecessary response properties.

Use the schema already established in the repository.

==================================================
STEP 10 — FIX IT BUTTON
=======================

Verify that:

Fix It

does NOT execute remediation.

It must remain a UI placeholder.

==================================================
STEP 11 — TEST FAILURE CONDITIONS
=================================

Test:

1. Data Pipeline unavailable
2. AI Diagnosis unavailable
3. malformed JSON
4. missing required field
5. invalid adapter
6. Chroma unavailable
7. New Relic unavailable when required
8. repeated AI Diagnosis clicks
9. repeated Run Pipeline clicks
10. slow pipeline
11. slow AI response

The application should fail gracefully.

Do not hide failures with fake responses.

==================================================
STEP 12 — TEST OLD RESPONSE / NEW RUN
=====================================

Start pipeline run A.

Then start pipeline run B.

Verify that a delayed response from run A cannot overwrite the state/result of run B.

Use the existing:

run_id
execution_id
incident_id
or equivalent identifier

Do not invent a second identifier if the project already has a suitable one.

==================================================
STEP 13 — SCHEMA CONSISTENCY CHECK
==================================

Search the entire repository again after fixes.

Find all definitions/usages of:

* request models
* response models
* incident fields
* status values
* adapter values
* endpoint URLs
* ports
* environment variables

Verify no stale schema remains.

Specifically search for:

* fields removed in Prompt 2
* old endpoint paths
* old port numbers
* old field names
* duplicate request models
* conflicting status strings

There should be no unexplained conflict.

==================================================
STEP 14 — FINAL RESULT
======================

Do NOT claim the application works unless you actually tested it.

Return:

THREE APPLICATIONS

UI:
status:
port:
startup command:

Data Pipeline:
status:
port:
startup command:

AI Diagnosis:
status:
port:
startup command:

END-TO-END TESTS:

Run Pipeline:
PASS/FAIL

RUNNING status:
PASS/FAIL

SUCCESS status:
PASS/FAIL

FAILED status:
PASS/FAIL

AI Diagnosis visibility:
PASS/FAIL

AI Diagnosis request:
PASS/FAIL

LangGraph:
PASS/FAIL

Chroma:
PASS/FAIL

Tenant isolation:
PASS/FAIL

Spark diagnosis:
PASS/FAIL

Root cause displayed:
PASS/FAIL

Remedies displayed:
PASS/FAIL

Fix It remains non-functional:
PASS/FAIL

Error handling:
PASS/FAIL

Race-condition handling:
PASS/FAIL

List every file changed during debugging.

List every remaining problem.

For every remaining problem, give the exact application, file, endpoint/function, and reason.

IMPORTANT:

Do not merge applications.
Do not fake API responses.
Do not hard-code success.
Do not invent schemas.
Do not invent New Relic data.
Do not bypass LangGraph.
Do not remove Chroma.
Do not execute remediation.
