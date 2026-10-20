# Databricks notebook source
# MAGIC %md
# MAGIC # 06a — Agent Bricks Multi-Agent Supervisor (MAS) over the 2 Genie spaces
# MAGIC
# MAGIC Creates an **Agent Bricks Multi-Agent Supervisor** that orchestrates the two
# MAGIC domain Genie spaces and exposes them behind a **single serving endpoint**. The
# MAGIC supervisor routes each question to the right space (or both for cross-domain
# MAGIC questions) — this is what the App's chat tab talks to.
# MAGIC
# MAGIC * **Agent 1 — Sourcing_Supplier_Risk** → Genie space 1.
# MAGIC * **Agent 2 — Purchase_Order_Analytics** → Genie space 2.
# MAGIC
# MAGIC Uses the `/api/2.0/multi-agent-supervisors` API directly (self-contained, no
# MAGIC external package). Idempotent by name.
# MAGIC
# MAGIC **Fallback: Genie router.** Some workspaces cannot create Supervisor Agents (for
# MAGIC example when serverless compute access control is enabled, the API answers "Supervisor
# MAGIC Agent is not supported"). Then this notebook records `mode = genie_router`: the App
# MAGIC chat and the benchmark use `app/backend/genie_router.py` instead, a small supervisor
# MAGIC that routes with the same agent descriptions and instructions (Foundation Model API)
# MAGIC and asks the Genie spaces through the Genie Conversation API. Everything downstream
# MAGIC reads the mode from `_MAS_ENDPOINT`.

# COMMAND ----------

# MAGIC %run ../00_config

# COMMAND ----------

import time
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()


def _do(method, path, body=None, params=None):
    return w.api_client.do(method, path, body=body, query=params)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Look up the Genie space IDs created in 05a

# COMMAND ----------

spaces = {r["space_key"]: r["space_id"] for r in spark.table(CFG.fq("_GENIE_SPACES")).collect()}
S1 = spaces["sourcing_supplier_risk"]
S2 = spaces["purchase_order_analytics"]
print(f"Space 1 (sourcing/risk): {S1}")
print(f"Space 2 (PO analytics):  {S2}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define the two agents (each backed by a Genie space)

# COMMAND ----------

agents = [
    {
        "name": "Sourcing_Supplier_Risk",
        "description": ("Answers sourcing-risk and supply-continuity questions: "
                        "multi-sourcing gaps, single-sourced materials, supplier "
                        "delivery risk, purchasing blocks, quota concentration, and the "
                        "live mitigation cases buyers opened on those risks (open cases, "
                        "risks nobody is working on yet)."),
        "agent_type": "genie",
        "genie_space": {"id": S1},
    },
    {
        "name": "Purchase_Order_Analytics",
        "description": ("Answers purchase-order questions: on-time delivery rates, "
                        "net-price variance across vendors, total spend, and orders "
                        "placed under active supplier delivery risk."),
        "agent_type": "genie",
        "genie_space": {"id": S2},
    },
]

INSTRUCTIONS = (
    "Route sourcing, multi-sourcing, single-sourcing, supplier-risk, purchasing-block "
    "and quota questions to Sourcing_Supplier_Risk. Route on-time delivery, net price, "
    "price variance, spend and orders-under-risk questions to Purchase_Order_Analytics. "
    "For cross-domain questions (e.g. 'did we place POs with single-sourced risky "
    "suppliers?'), consult both agents and combine the answers."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create or update the MAS (idempotent by name)

# COMMAND ----------


def mas_find_by_name(name):
    # MAS tiles are discovered via the /api/2.0/tiles listing endpoint (filtered to
    # MAS tiles by name), NOT the multi-agent-supervisors endpoint.
    page_token = None
    try:
        while True:
            params = {"filter": f"name_contains={name}&&tile_type=MAS"}
            if page_token:
                params["page_token"] = page_token
            resp = _do("GET", "/api/2.0/tiles", params=params)
            for t in (resp.get("tiles") or []):
                if t.get("name") == name:
                    return t.get("tile_id")
            page_token = resp.get("next_page_token")
            if not page_token:
                break
    except Exception:
        return None
    return None


MODE = "mas"
existing = mas_find_by_name(CFG.mas_name)
if not existing:
    try:
        res = _do("POST", "/api/2.0/multi-agent-supervisors",
                  body={"name": CFG.mas_name, "agents": agents,
                        "description": "Supervisor over the 2 SAP purchasing Genie spaces "
                                       "(Sourcing & Supplier Risk; Purchase Order Analytics).",
                        "instructions": INSTRUCTIONS})
    except Exception as e:
        if "not supported" not in str(e).lower():
            raise
        MODE = "genie_router"
        print(f"⚠ Supervisor Agent cannot be created in this workspace: {e}")
        print("→ Falling back to the Genie router (app/backend/genie_router.py).")

if MODE == "genie_router":
    tile_id, endpoint_name = None, None
elif existing:
    print(f"Updating existing MAS '{CFG.mas_name}' ({existing})")
    res = _do("PATCH", f"/api/2.0/multi-agent-supervisors/{existing}",
              body={"tile_id": existing, "name": CFG.mas_name, "agents": agents,
                    "description": "Supervisor over the 2 SAP purchasing Genie spaces.",
                    "instructions": INSTRUCTIONS})
    mas = res.get("multi_agent_supervisor", res)
    tile = mas.get("tile", mas)
    tile_id = tile.get("tile_id", existing)
else:
    mas = res["multi_agent_supervisor"]
    tile = mas["tile"]
    tile_id = tile["tile_id"]

if MODE == "mas":
    endpoint_name = tile["serving_endpoint_name"]
    print(f"✓ MAS tile_id={tile_id}")
    print(f"✓ Serving endpoint: {endpoint_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wait for the serving endpoint to come ONLINE
# MAGIC The MAS provisions its serving endpoint asynchronously (can take several minutes).

# COMMAND ----------


# The authoritative readiness signal is the SERVING ENDPOINT's own state (READY),
# not the MAS tile status field (which reports NOT_APPLICABLE for MAS tiles).
def endpoint_ready():
    try:
        e = w.serving_endpoints.get(endpoint_name)
        return (e.state and str(e.state.ready) == "EndpointStateReady.READY"), str(e.state)
    except Exception as ex:
        return False, f"(not yet created: {ex})"


deadline = time.time() + (1800 if MODE == "mas" else 0)  # up to 30 min
status = "genie_router (no serving endpoint)" if MODE == "genie_router" else None
while time.time() < deadline:
    ready, status = endpoint_ready()
    print(f"  endpoint state: {status}")
    if ready:
        break
    time.sleep(30)

print(f"Final endpoint state: {status}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Persist the MAS endpoint name for the App & benchmark

# COMMAND ----------

from pyspark.sql import Row

spark.createDataFrame([Row(
    mas_name=CFG.mas_name, tile_id=tile_id, endpoint_name=endpoint_name, mode=MODE,
)], "mas_name STRING, tile_id STRING, endpoint_name STRING, mode STRING").write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(CFG.fq("_MAS_ENDPOINT"))

print("Saved to _MAS_ENDPOINT:")
display(spark.table(CFG.fq("_MAS_ENDPOINT")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## (Optional) Smoke-test the endpoint with a cross-domain question

# COMMAND ----------

# The MAS endpoint uses the OpenAI-style **Responses API**: send an `input` list of
# messages, receive an `output` list of items (function_call + assistant messages).
import requests

SMOKE_Q = "Which materials have no multi-sourcing strategy?"
try:
    if MODE == "mas":
        headers = w.config.authenticate()
        url = f"{w.config.host}/serving-endpoints/{endpoint_name}/invocations"
        r = requests.post(url, headers=headers,
                          json={"input": [{"role": "user", "content": SMOKE_Q}]}, timeout=160)
        print("HTTP", r.status_code)
        out = r.json().get("output", [])
    else:
        import sys
        sys.path.insert(0, f"{demo_root()}/app")
        from backend.genie_router import ask
        out, _sql = ask(w, CFG.model, {"Sourcing_Supplier_Risk": S1, "Purchase_Order_Analytics": S2}, SMOKE_Q)
        print("Genie router answered via:", [i["content"][0]["text"] for i in out if i["content"][0]["text"].startswith("<name>")])
    # The final assistant message holds the answer text.
    for item in out:
        if item.get("type") == "message":
            for c in item.get("content", []):
                if c.get("type") == "output_text":
                    print("Answer:", c["text"][:500])
except Exception as e:
    print(f"(endpoint query skipped/failed — it may still be warming up: {e})")

# COMMAND ----------

dbutils.notebook.exit(f"06a_create_mas: SUCCESS — mode={MODE}, endpoint={endpoint_name}, status={status}")
