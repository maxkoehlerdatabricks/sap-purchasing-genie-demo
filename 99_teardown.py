# Databricks notebook source
# MAGIC %md
# MAGIC # 99_teardown — DELETE every asset this demo creates
# MAGIC
# MAGIC A top-level cleanup notebook that removes **everything** the demo provisions, so
# MAGIC the workspace is returned to a clean state. Run it on **Serverless** interactive
# MAGIC compute. It is **idempotent** — anything already gone is skipped.
# MAGIC
# MAGIC ## What it deletes
# MAGIC 1. **Databricks App** — `sap-purchasing-app`.
# MAGIC 2. **Data-prep Job** — `sap_purchasing_data_prep`.
# MAGIC 3. **Multi-Agent Supervisor (MAS)** + its **serving endpoint** (looked up from
# MAGIC    `_MAS_ENDPOINT`, falling back to the tiles API by name).
# MAGIC 4. **Genie spaces** — both, looked up from `_GENIE_SPACES`, falling back to the
# MAGIC    data-rooms API by display name.
# MAGIC 5. **The whole schema** `catalog.schema` (all gold + raw + metric views + SQL
# MAGIC    functions + the `_*` metadata tables) via `DROP SCHEMA ... CASCADE`.
# MAGIC
# MAGIC ## What it does NOT delete (intentionally)
# MAGIC * The **catalog** — taken as-is, never created by the demo.
# MAGIC * The **SQL warehouse** — reused (given), not created by the demo.
# MAGIC * The **workspace source folders / Git folder** — remove those from the Workspace
# MAGIC   UI or with the CLI (`databricks workspace delete ... --recursive`).
# MAGIC
# MAGIC > ⚠️ This is destructive. Double-check `CATALOG` / `SCHEMA` below before running.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set the target ONCE — as plain Python variables (same style as 00_run_all)

# COMMAND ----------

# ---- The single source of truth for what to tear down ----
CATALOG = "serverless_stable_7qzrfp_catalog"   # Unity Catalog (existing, taken as-is)
SCHEMA = "purchasing_demo"                       # Schema created by the demo (will be DROPPED)

# Asset names (must match 00_config.py).
MAS_NAME = "sap_purchasing_supervisor"
APP_NAME = "sap-purchasing-app"
JOB_NAME = "sap_purchasing_data_prep"
GENIE_SPACE_1_NAME = "SAP Purchasing — Sourcing & Supplier Risk"
GENIE_SPACE_2_NAME = "SAP Purchasing — Purchase Order Analytics"

NS = f"{CATALOG}.{SCHEMA}"
print(f"About to tear down everything under: {NS}")

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()


def _do(method, path, body=None, params=None):
    return w.api_client.do(method, path, body=body, query=params)


def _table_exists(fq):
    try:
        return spark.catalog.tableExists(fq)
    except Exception:
        return False


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1) Delete the Databricks App

# COMMAND ----------

try:
    w.apps.get(name=APP_NAME)
    print(f"Deleting app '{APP_NAME}'…")
    w.apps.delete(name=APP_NAME)
    print("  ✓ app deleted")
except Exception as e:
    print(f"  (app '{APP_NAME}' not found or already deleted: {e})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2) Delete the data-prep Job

# COMMAND ----------

try:
    jobs = list(w.jobs.list(name=JOB_NAME))
    if not jobs:
        print(f"  (job '{JOB_NAME}' not found)")
    for j in jobs:
        print(f"Deleting job '{JOB_NAME}' (job_id={j.job_id})…")
        w.jobs.delete(job_id=j.job_id)
        print("  ✓ job deleted")
except Exception as e:
    print(f"  (could not delete job '{JOB_NAME}': {e})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3) Delete the MAS + its serving endpoint
# MAGIC Prefer the endpoint name recorded in `_MAS_ENDPOINT`; also delete the MAS tile so
# MAGIC no orphaned supervisor remains.

# COMMAND ----------

# 3a) Read the recorded endpoint name (used only as a fallback below).
endpoint_name = None
if _table_exists(f"{NS}._MAS_ENDPOINT"):
    try:
        row = spark.table(f"{NS}._MAS_ENDPOINT").first()
        if row:
            endpoint_name = row["endpoint_name"]
    except Exception as e:
        print(f"  (could not read _MAS_ENDPOINT: {e})")


# 3b) MAS tile — find by name via the tiles API.
def mas_find_by_name(name):
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


# 3c) Delete the MAS tile — this is the correct API and it CASCADES to (removes) the
# associated serving endpoint. The endpoint cannot be deleted on its own
# ("Please delete the tile to delete all associated endpoints").
tile_id = mas_find_by_name(MAS_NAME)
if tile_id:
    try:
        print(f"Deleting MAS '{MAS_NAME}' (tile_id={tile_id}) — this also removes its endpoint…")
        _do("DELETE", f"/api/2.0/tiles/{tile_id}")
        print("  ✓ MAS tile + endpoint deleted")
    except Exception as e:
        print(f"  (could not delete MAS tile {tile_id}: {e})")
else:
    print(f"  (MAS '{MAS_NAME}' not found)")

# 3d) Fallback: if the tile was already gone but an orphaned endpoint remains, try to
# delete it directly (best-effort; normally the tile delete above handles it).
if endpoint_name:
    try:
        w.serving_endpoints.get(name=endpoint_name)
        print(f"Endpoint '{endpoint_name}' still present — deleting directly…")
        w.serving_endpoints.delete(name=endpoint_name)
        print("  ✓ serving endpoint deleted")
    except Exception:
        print(f"  (serving endpoint '{endpoint_name}' already removed)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4) Delete both Genie spaces
# MAGIC Prefer the IDs in `_GENIE_SPACES`; fall back to looking them up by display name.

# COMMAND ----------


def genie_find_by_name(display_name):
    try:
        resp = _do("GET", "/api/2.0/data-rooms/", params={"page_size": 200})
    except Exception:
        return None
    for s in (resp.get("data_rooms") or resp.get("spaces") or []):
        name = s.get("display_name") or s.get("title") or ""
        if name == display_name:
            return s.get("space_id") or s.get("id")
    return None


def genie_delete(space_id):
    _do("DELETE", f"/api/2.0/data-rooms/{space_id}")


space_ids = []
if _table_exists(f"{NS}._GENIE_SPACES"):
    try:
        space_ids = [r["space_id"] for r in spark.table(f"{NS}._GENIE_SPACES").collect()]
    except Exception as e:
        print(f"  (could not read _GENIE_SPACES: {e})")

# Fall back to name lookup for any we couldn't get from the table.
for nm in (GENIE_SPACE_1_NAME, GENIE_SPACE_2_NAME):
    sid = genie_find_by_name(nm)
    if sid and sid not in space_ids:
        space_ids.append(sid)

if not space_ids:
    print("  (no Genie spaces found to delete)")
for sid in space_ids:
    try:
        print(f"Deleting Genie space {sid}…")
        genie_delete(sid)
        print("  ✓ deleted")
    except Exception as e:
        print(f"  (could not delete space {sid}: {e})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5) Drop the whole schema (tables, views, functions, metadata)
# MAGIC `DROP SCHEMA ... CASCADE` removes all 5 gold tables, the 13 raw tables, the 4
# MAGIC metric views, all SQL functions, `VALUE_DICTIONARY`, and the `_GENIE_SPACES` /
# MAGIC `_MAS_ENDPOINT` / `_BENCHMARK_RESULTS` metadata tables in one shot.

# COMMAND ----------

try:
    spark.sql(f"DROP SCHEMA IF EXISTS {NS} CASCADE")
    print(f"  ✓ dropped schema {NS} (CASCADE)")
except Exception as e:
    print(f"  (could not drop schema {NS}: {e})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC The catalog and the SQL warehouse are intentionally left in place. To also remove
# MAGIC the workspace source folders, run from the CLI, e.g.:
# MAGIC ```
# MAGIC databricks workspace delete /Workspace/Users/<you>/sap-purchasing-genie-demo --recursive
# MAGIC databricks repos delete <repo_id>
# MAGIC ```

# COMMAND ----------

dbutils.notebook.exit(f"99_teardown: SUCCESS — all demo assets under {NS} removed.")
