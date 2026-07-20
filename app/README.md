# SAP Purchasing Demo — Databricks App

FastAPI backend + React (Vite) frontend. Two tabs:

1. **Visualizations** — KPI cards + charts sourced from the UC **metric views** and
   **trusted SQL functions** via the serverless SQL warehouse.
2. **Chat (MAS)** — natural-language chat proxied to the **Agent Bricks Multi-Agent
   Supervisor** serving endpoint (Responses API), which routes to the two Genie spaces.

## Layout
```
app/
  app.yaml            # Databricks App manifest (command + env + resources)
  requirements.txt    # backend Python deps (never pins pyspark)
  backend/main.py     # FastAPI: /api/charts/*, /api/chat, serves frontend/dist
  frontend/           # Vite + React + recharts
```

## Local devloop
```bash
# 1) Backend (uses your Databricks CLI profile for auth)
cd app
DATABRICKS_CONFIG_PROFILE=fe-vm-fevm-serverless-stable-7qzrfp \
  DATABRICKS_WAREHOUSE_ID=117d425266741646 \
  MAS_ENDPOINT_NAME=mas-655dcd32-endpoint \
  uvicorn backend.main:app --reload --port 8000

# 2) Frontend (proxies /api to :8000)
cd app/frontend && npm install && npm run dev   # http://localhost:5173
```

## Build + deploy to Databricks Apps
```bash
# Build the static frontend (backend serves app/frontend/dist)
cd app/frontend && npm run build

# Create the app (once) and deploy the source
databricks apps create sap-purchasing-app -p <profile>
databricks sync app /Workspace/Users/<you>/sap-purchasing-app -p <profile>   # or workspace import-dir
databricks apps deploy sap-purchasing-app \
  --source-code-path /Workspace/Users/<you>/sap-purchasing-demo/app -p <profile>
```

## App resources to wire in the UI (or app.yaml)
- **SQL warehouse** (`sql-warehouse`) → the serverless Starter Warehouse. Grant the app
  service principal `CAN USE` on the warehouse and `SELECT`/`EXECUTE` on the demo schema.
- **Serving endpoint** → the MAS endpoint (`mas-655dcd32-endpoint`). Grant the app SP
  `CAN QUERY`.
