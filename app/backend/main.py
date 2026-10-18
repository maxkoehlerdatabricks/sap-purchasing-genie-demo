"""SAP Purchasing demo — Databricks App backend (FastAPI).

Three responsibilities:
  1) /api/charts/*  — run governed queries (metric views + trusted functions) against
     the SQL warehouse and return JSON for the Visualizations tab.
  2) /api/chat      — proxy a user message to the MAS serving endpoint (Responses API)
     and return the assistant's answer for the Chat tab.
  3) /api/cases/*   — read the synced risk worklist and read/write mitigation cases in
     Lakebase (Postgres) for the Mitigation cases tab. See backend/cases.py.

Auth model: when running as a Databricks App, credentials come from the app's service
principal via the environment (the SDK + SQL connector pick these up automatically).
Locally, we fall back to the configured CLI profile.
"""

import os
import logging
from functools import lru_cache

import requests
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("sap-app")

# All values are injected at deploy time by app/deploy_app (from 00_config).
CATALOG = os.environ.get("DEMO_CATALOG", "max_kohler_demo_catalog")
SCHEMA = os.environ.get("DEMO_SCHEMA", "purchasing_demo")
NS = f"{CATALOG}.{SCHEMA}"
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
MAS_ENDPOINT = os.environ.get("MAS_ENDPOINT_NAME", "")

app = FastAPI(title="SAP Purchasing Demo")


# ---------------------------------------------------------------------------
# Databricks clients (lazy, cached)
# ---------------------------------------------------------------------------
@lru_cache(maxsize=1)
def _ws():
    from databricks.sdk import WorkspaceClient
    # In the App runtime, no args needed. Locally, honor DATABRICKS_CONFIG_PROFILE.
    return WorkspaceClient()


@lru_cache(maxsize=1)
def _sql_conn_params():
    """Resolve host + warehouse http_path + auth token for the SQL connector."""
    w = _ws()
    host = w.config.host.replace("https://", "").rstrip("/")
    http_path = f"/sql/1.0/warehouses/{WAREHOUSE_ID}"
    return host, http_path


def run_sql(query: str):
    """Execute a read-only query on the warehouse; return (columns, rows)."""
    from databricks import sql as dbsql
    w = _ws()
    host, http_path = _sql_conn_params()
    # Obtain a bearer token from the SDK's auth (works for PAT, OAuth, or SP).
    token = None
    try:
        headers = w.config.authenticate()
        auth = headers.get("Authorization", "")
        if auth.startswith("Bearer "):
            token = auth[len("Bearer "):]
    except Exception:
        pass
    conn = dbsql.connect(server_hostname=host, http_path=http_path, access_token=token)
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            cols = [c[0] for c in cur.description]
            rows = [list(r) for r in cur.fetchall()]
        return cols, rows
    finally:
        conn.close()


def _json_safe(v):
    """Coerce warehouse types that FastAPI's default JSON encoder can't handle.

    The SQL connector returns DECIMAL columns as ``decimal.Decimal`` and date/time
    columns as ``datetime`` objects — neither is JSON-serializable by default, which
    would surface as a 500. Convert them to plain floats / ISO strings.
    """
    import datetime
    from decimal import Decimal
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (datetime.date, datetime.datetime)):
        return v.isoformat()
    return v


def rows_to_records(cols, rows):
    return [{c: _json_safe(v) for c, v in zip(cols, r)} for r in rows]


# ---------------------------------------------------------------------------
# Chart endpoints — each backed by a governed metric view or trusted function
# ---------------------------------------------------------------------------
@app.get("/api/health")
def health():
    from backend.cases import LAKEBASE_INFO
    return {"status": "ok", "namespace": NS, "mas_endpoint": MAS_ENDPOINT, "lakebase": LAKEBASE_INFO}


@app.get("/api/charts/sourcing_coverage")
def sourcing_coverage():
    """Vendor coverage per material group (distinct vs valid vendors)."""
    cols, rows = run_sql(f"""
        SELECT `Material Group` AS material_group,
               MEASURE(`Distinct Vendors`) AS distinct_vendors,
               MEASURE(`Valid Vendors`)    AS valid_vendors,
               MEASURE(`Distinct Materials`) AS materials
        FROM {NS}.mv_sourcing_coverage
        WHERE `Material Group` IS NOT NULL
        GROUP BY ALL ORDER BY material_group
    """)
    return JSONResponse(rows_to_records(cols, rows))


@app.get("/api/charts/risk_exposure")
def risk_exposure():
    """Delivery-risk vendors by risk category."""
    cols, rows = run_sql(f"""
        SELECT `Risk Category` AS risk_category,
               MEASURE(`Distinct Vendors`) AS vendors,
               MEASURE(`Delivery Risk Vendors`) AS delivery_risk_vendors,
               ROUND(MEASURE(`Avg Risk Score`),1) AS avg_score
        FROM {NS}.mv_supplier_risk
        GROUP BY ALL ORDER BY delivery_risk_vendors DESC
    """)
    return JSONResponse(rows_to_records(cols, rows))


@app.get("/api/charts/otd")
def otd():
    """On-time delivery rate per supplier (worst 12)."""
    cols, rows = run_sql(f"""
        SELECT Supplier AS supplier,
               CAST(ROUND(MEASURE(`OTD Rate`),3) AS DOUBLE) AS otd_rate,
               MEASURE(Deliveries) AS deliveries
        FROM {NS}.mv_delivery_performance
        GROUP BY ALL HAVING MEASURE(Deliveries) > 0
        ORDER BY otd_rate ASC LIMIT 12
    """)
    return JSONResponse(rows_to_records(cols, rows))


@app.get("/api/charts/price_variance")
def price_variance():
    """Top-15 materials by net-price spread across vendors."""
    cols, rows = run_sql(f"""
        SELECT MATNR AS material, price_spread, vendor_count
        FROM {NS}.net_price_variance_per_material()
        ORDER BY price_spread DESC LIMIT 15
    """)
    return JSONResponse(rows_to_records(cols, rows))


@app.get("/api/charts/kpis")
def kpis():
    """Headline KPI cards."""
    cols, rows = run_sql(f"""
        SELECT
          (SELECT COUNT(*) FROM {NS}.materials_without_multisourcing(2)) AS no_multisource,
          (SELECT COUNT(*) FROM {NS}.single_sourced_risky(current_date()))  AS single_sourced_risky,
          (SELECT COUNT(DISTINCT LIFNR) FROM {NS}.suppliers_with_delivery_risk(current_date())) AS risky_suppliers,
          (SELECT COUNT(*) FROM {NS}.materials_without_valid_sourcing()) AS no_sourcing,
          (SELECT CAST(ROUND(AVG(CASE WHEN ON_TIME_FLAG THEN 1.0 ELSE 0.0 END),3) AS DOUBLE)
             FROM {NS}.PURCHASE_ORDER WHERE GR_DATE IS NOT NULL) AS overall_otd
    """)
    return JSONResponse(rows_to_records(cols, rows)[0] if rows else {})


# ---------------------------------------------------------------------------
# Chat endpoint — proxy to the MAS serving endpoint (Responses API)
# ---------------------------------------------------------------------------
class ChatRequest(BaseModel):
    message: str


@app.post("/api/chat")
def chat(req: ChatRequest):
    """Send the user's message to the MAS endpoint and return the answer text + SQL."""
    w = _ws()
    try:
        headers = w.config.authenticate()
        url = f"{w.config.host}/serving-endpoints/{MAS_ENDPOINT}/invocations"
        r = requests.post(url, headers=headers,
                          json={"input": [{"role": "user", "content": req.message}]},
                          timeout=180)
        r.raise_for_status()
        output = r.json().get("output", [])
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"MAS query failed: {e}")

    # Extract the routed agent name, any SQL, and the final answer text.
    # The MAS emits a function_call whose `name` is the sub-agent it routed to
    # (e.g. "Sourcing_Supplier_Risk"); prefer that over the supervisor's own name.
    answer, sql, agent = "", "", ""
    for item in output:
        if item.get("type") == "function_call":
            nm = item.get("name", "")
            if nm and nm != MAS_ENDPOINT:
                agent = nm
        if item.get("type") == "message":
            for c in item.get("content", []):
                if c.get("type") == "output_text":
                    txt = c["text"]
                    if txt.startswith("<name>"):
                        agent = txt.replace("<name>", "").replace("</name>", "").strip()
                    else:
                        answer = txt  # keep the last non-routing message as the answer
    return {"answer": answer, "agent": agent, "sql": sql}


# ---------------------------------------------------------------------------
# Mitigation cases (Lakebase) — routes live in backend/cases.py
# ---------------------------------------------------------------------------
from backend.cases import router as cases_router  # noqa: E402

app.include_router(cases_router)


# ---------------------------------------------------------------------------
# Serve the built React frontend (static files) — mounted LAST so /api/* wins.
# ---------------------------------------------------------------------------
_static_dir = os.path.join(os.path.dirname(__file__), "..", "frontend", "dist")
if os.path.isdir(_static_dir):
    app.mount("/", StaticFiles(directory=_static_dir, html=True), name="static")
