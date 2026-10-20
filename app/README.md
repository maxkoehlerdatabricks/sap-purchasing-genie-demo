# SAP Purchasing Demo — Databricks App

FastAPI backend + a static frontend. Three tabs:

1. **Visualizations** — KPI cards + charts sourced from the UC **metric views** and
   **trusted SQL functions** via the serverless SQL warehouse.
2. **Chat (Agent)** — natural-language chat proxied to the **Agent Bricks Multi-Agent
   Supervisor** serving endpoint (Responses API), which routes to the two Genie spaces.
   Where the workspace cannot create a Supervisor Agent, `06a` records
   `mode = genie_router` and the chat uses `backend/genie_router.py` instead: the routing
   LLM picks the agent, the Genie Conversation API answers.
3. **Mitigation cases (Lakebase)** — the risk worklist (synced from the Delta table
   `RISK_WORKLIST` into Lakebase) and the mitigation cases buyers open, update and close.
   Reads and writes go to Lakebase Postgres; every write is one transaction with its
   audit row in `ops.case_event`.

## Layout
```
app/
  app.yaml            # Databricks App manifest (command + env defaults)
  requirements.txt    # backend Python deps (never pins pyspark)
  deploy_app.py       # notebook: create/recreate app, resources, grants, deploy with env vars
  backend/main.py     # FastAPI: /api/charts/*, /api/chat, serves frontend/dist
  backend/cases.py    # FastAPI router /api/cases/* on Lakebase (psycopg, OAuth token per connection)
  frontend/dist/      # SHIPPED frontend: no-build index.html (Chart.js from CDN), all 3 tabs
  frontend/src/       # React + Vite source of the first two tabs (reference only)
```

> **Do not run `npm run build` before deploying** unless you have ported the
> Mitigation cases tab to `frontend/src`. The build overwrites `frontend/dist/index.html`,
> which is the only frontend that contains the Lakebase tab.

## Deploy (recommended: the notebook)
`00_run_all` runs `app/deploy_app`, which:
1. creates the app (or recreates it if its service principal was deleted),
2. wires the resources: `sql-warehouse` (CAN_USE), `mas-endpoint` (CAN_QUERY) and
   `lakebase` (Postgres database, CAN_CONNECT_AND_CREATE),
3. grants the app SP read/write on `ops.*` and read on the synced worklist in Postgres,
   UC `SELECT` on the Lakebase catalog and `CAN_RUN` on both Genie spaces,
4. deploys from this folder with **env vars from `00_config`** (catalog, schema, Lakebase
   host/endpoint/database/schemas). They override the defaults in `app.yaml`.

## Local devloop
```bash
cd app
DATABRICKS_CONFIG_PROFILE=<profile> \
  DATABRICKS_WAREHOUSE_ID=<warehouse id> DEMO_CATALOG=<catalog> DEMO_SCHEMA=purchasing_demo \
  MAS_ENDPOINT_NAME=<mas endpoint> \
  LAKEBASE_HOST=<endpoint host> \
  LAKEBASE_ENDPOINT=projects/sap-purchasing-ops/branches/production/endpoints/primary \
  uvicorn backend.main:app --reload --port 8000     # http://localhost:8000
```
The Lakebase coordinates are in the `_LAKEBASE` table written by `10a_create_lakebase`.
Locally the backend connects to Postgres as your own user.

## API (cases)
| Method | Path | Purpose |
|--------|------|---------|
| GET | `/api/cases/meta` | buyers + action catalog |
| GET | `/api/cases/worklist` | synced worklist with each row's active case |
| GET | `/api/cases/summary` | worklist size, risks without an active case, cases by status |
| GET | `/api/cases?buyer_id=&status=` | case list |
| POST | `/api/cases` | open a case (404 if not on the worklist, 409 if an active case exists) |
| PATCH | `/api/cases/{id}` | change status / buyer / note (audited) |
| GET | `/api/cases/{id}/events` | audit trail |
