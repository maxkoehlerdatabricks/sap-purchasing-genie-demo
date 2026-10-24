# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # 99_teardown — DELETE every asset this demo creates
# MAGIC
# MAGIC A top-level cleanup notebook that removes **everything** the demo provisions, so
# MAGIC the workspace is returned to a clean state. Run it on **Serverless** interactive
# MAGIC compute. It is **idempotent** — anything already gone is skipped.
# MAGIC
# MAGIC ## What it deletes
# MAGIC 1. **Databricks App** — `sap-purchasing-app`.
# MAGIC 2. **Lakeflow Job** — `sap_purchasing_lakeflow_pipeline` (and the earlier
# MAGIC    single-task job `sap_purchasing_data_prep`, if still present).
# MAGIC 3. **Supervisor agent** `agent-bricks-sap-supervisor` + its **Runtime Store**
# MAGIC    (via `agentbricks deployments delete`) and its MLflow tracing experiment. Also
# MAGIC    removes a legacy Supervisor Agent (MAS) tile + endpoint left by older versions.
# MAGIC 4. **Genie spaces** — both, looked up from `_GENIE_SPACES`, falling back to the
# MAGIC    data-rooms API by display name.
# MAGIC 5. **Discover pages, domains & governed tags** *(optional, controlled by
# MAGIC    `DELETE_DISCOVER`)* — deletes all 5 governed pages, the 3 domains/subdomains,
# MAGIC    and the 3 governed tag policies.
# MAGIC 6. **Lakebase** — the synced table, the UC catalog `sap_purchasing_ops` over the
# MAGIC    database, and the Lakebase project `sap-purchasing-ops` (purged, so the name can
# MAGIC    be reused at once). This deletes all mitigation cases.
# MAGIC 7. **The whole schema** `catalog.schema` (all gold + raw + metric views + SQL
# MAGIC    functions + the `_*` metadata tables) and the sync schema `<schema>_sync`
# MAGIC    via `DROP SCHEMA ... CASCADE`.
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

# DBTITLE 1,Config
# ---- The single source of truth for what to tear down ----
CATALOG = "max_kohler_demo_catalog"              # Unity Catalog (existing, taken as-is)
SCHEMA = "purchasing_demo"                       # Schema created by the demo (will be DROPPED)

# Asset names (must match 00_config.py).
SUPERVISOR_APP_NAME = "agent-bricks-sap-supervisor"
SUPERVISOR_EXPERIMENT = "/Shared/agentbricks_traces/sap-purchasing-supervisor"
LEGACY_MAS_NAME = "sap_purchasing_supervisor"   # legacy Supervisor Agent tile, if still present
APP_NAME = "sap-purchasing-app"
JOB_NAMES = ["sap_purchasing_lakeflow_pipeline", "sap_purchasing_data_prep"]  # current + legacy
GENIE_SPACE_1_NAME = "SAP Purchasing — Sourcing & Supplier Risk"
GENIE_SPACE_2_NAME = "SAP Purchasing — Purchase Order Analytics"

# ---- Discover assets (governed tags, domains, pages) ----
# Set to False to keep tags, domains and pages when tearing down. Can also be overridden
# with the widget/job parameter `delete_discover` ("true"/"false"), e.g. to keep Pages
# that were created by hand.
DELETE_DISCOVER = True
try:
    dbutils.widgets.text("delete_discover", str(DELETE_DISCOVER).lower(), "Delete Discover tags/domains/pages")
    DELETE_DISCOVER = dbutils.widgets.get("delete_discover").strip().lower() == "true"
except Exception:
    pass

# Governed tag keys (must match 09a_create_domains).
PARENT_TAG = "SAP_Purchasing"
SUB_TAG_1 = f"{PARENT_TAG}/Sourcing_Supplier_Risk"
SUB_TAG_2 = f"{PARENT_TAG}/Purchase_Order_Analytics"
DISCOVER_TAG_KEYS = [SUB_TAG_1, SUB_TAG_2, PARENT_TAG]  # delete children first

# Lakebase (must match 00_config.py).
LAKEBASE_PROJECT = "sap-purchasing-ops"
LAKEBASE_CATALOG = "sap_purchasing_ops"
SYNC_SCHEMA = f"{SCHEMA}_sync"

# Page names (must match 09b_create_pages).
PAGE_NAMES = [
    "Multi-Sourcing Strategy",
    "Delivery Risk",
    "On-Time Delivery (OTD)",
    "Quota Arrangement",
    "SAP Code Glossary",
]

NS = f"{CATALOG}.{SCHEMA}"
print(f"About to tear down everything under: {NS}")
print(f"DELETE_DISCOVER = {DELETE_DISCOVER}")

# COMMAND ----------

# DBTITLE 1,SDK + helpers
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()


def _do(method, path, body=None, params=None):
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    return w.api_client.do(method, path, body=body, query=params, headers=headers)


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
# MAGIC ## 2) Delete the Lakeflow Job (and the legacy data-prep job)

# COMMAND ----------

for job_name in JOB_NAMES:
    try:
        jobs = list(w.jobs.list(name=job_name))
        if not jobs:
            print(f"  (job '{job_name}' not found)")
        for j in jobs:
            print(f"Deleting job '{job_name}' (job_id={j.job_id})…")
            w.jobs.delete(job_id=j.job_id)
            print("  ✓ job deleted")
    except Exception as e:
        print(f"  (could not delete job '{job_name}': {e})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3) Delete the supervisor agent (Agent Bricks CLI) + its Runtime Store
# MAGIC `agentbricks deployments delete` drops the Runtime Store first (it needs the app's
# MAGIC service principal), then the app. The tracing experiment is deleted separately.
# MAGIC A legacy Supervisor Agent (MAS) tile is removed too; deleting the tile also removes
# MAGIC its serving endpoint.

# COMMAND ----------

# MAGIC %run ./06_agent/_agentbricks_helpers

# COMMAND ----------

# 3a) Supervisor agent app + Runtime Store, then the source folder `agentbricks deploy` synced.
try:
    source_path = w.api_client.do("GET", f"/api/2.0/apps/{SUPERVISOR_APP_NAME}").get("default_source_code_path")
    print(f"Deleting supervisor '{SUPERVISOR_APP_NAME}' and its Runtime Store…")
    agentbricks("deployments", "delete", SUPERVISOR_APP_NAME, "--yes")
    print("  ✓ supervisor deleted")
    if source_path:
        w.workspace.delete(source_path, recursive=True)
        print(f"  ✓ deleted deployment source {source_path}")
except Exception as e:
    if "does not exist" in str(e).lower() or "not found" in str(e).lower():
        print(f"  (supervisor '{SUPERVISOR_APP_NAME}' not found)")
    else:
        print(f"  ⚠ could not delete supervisor '{SUPERVISOR_APP_NAME}': {e}")

# 3b) MLflow tracing experiment.
try:
    exp = w.experiments.get_by_name(SUPERVISOR_EXPERIMENT)
    w.experiments.delete_experiment(exp.experiment.experiment_id)
    print(f"  ✓ deleted experiment {SUPERVISOR_EXPERIMENT}")
except Exception as e:
    print(f"  (experiment {SUPERVISOR_EXPERIMENT} not found: {str(e)[:80]})")

# 3c) Legacy Supervisor Agent (MAS) tile, found by name via the tiles API.
try:
    resp = _do("GET", "/api/2.0/tiles", params={"filter": f"name_contains={LEGACY_MAS_NAME}&&tile_type=MAS"})
    tiles = [t["tile_id"] for t in (resp.get("tiles") or []) if t.get("name") == LEGACY_MAS_NAME]
except Exception:
    tiles = []
for tile_id in tiles:
    try:
        _do("DELETE", f"/api/2.0/tiles/{tile_id}")
        print(f"  ✓ deleted legacy MAS tile {tile_id} (and its serving endpoint)")
    except Exception as e:
        print(f"  (could not delete legacy MAS tile {tile_id}: {e})")
if not tiles:
    print(f"  (no legacy MAS '{LEGACY_MAS_NAME}' found)")

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

# DBTITLE 1,Step 5 heading
# MAGIC %md
# MAGIC ## 5) Delete Discover pages, domains & governed tags (optional)
# MAGIC Controlled by `DELETE_DISCOVER`. Deletes in reverse dependency order:
# MAGIC pages → domains (children first) → governed tag policies.

# COMMAND ----------

# DBTITLE 1,Delete Discover assets
if not DELETE_DISCOVER:
    print("  (DELETE_DISCOVER=False — skipping pages, domains & governed tags)")
else:
    # ---- 5a) Discover the APIs ----
    DOMAINS_API = None
    for candidate in ["/api/2.0/domains", "/api/2.1/domains"]:
        try:
            _do("GET", candidate)
            DOMAINS_API = candidate
            break
        except Exception:
            pass

    PAGES_API = None
    for candidate in ["/api/2.0/pages", "/api/2.1/pages",
                      "/api/2.0/discover/pages", "/api/2.1/discover/pages"]:
        try:
            _do("GET", candidate)
            PAGES_API = candidate
            break
        except Exception as e:
            if "Not Found" not in str(e) and "No API found" not in str(e) and "404" not in str(e):
                PAGES_API = candidate
                break

    # ---- 5b) Delete governed Pages ----
    print("\n--- Deleting governed Pages ---")
    if PAGES_API:
        try:
            resp = _do("GET", PAGES_API)
            all_pages = resp.get("pages", [])
            for pname in PAGE_NAMES:
                found = [p for p in all_pages if (p.get("name") or "") == pname]
                for p in found:
                    pid = p.get("page_id") or p.get("id")
                    try:
                        _do("DELETE", f"{PAGES_API}/{pid}")
                        print(f"  \u2713 Deleted page: {pname} (id={pid})")
                    except Exception as e:
                        print(f"  \u2022 Could not delete page {pname}: {e}")
                if not found:
                    print(f"  \u2022 Page not found: {pname}")
        except Exception as e:
            print(f"  \u2022 Could not list pages: {e}")
    else:
        print("  (Pages API not available — skipping)")

    # ---- 5c) Delete domains (children first, then parent) ----
    print("\n--- Deleting Discover domains ---")
    if DOMAINS_API:
        # Read domain IDs from the _DOMAINS table if available
        domain_ids = {}
        if _table_exists(f"{NS}._DOMAINS"):
            for r in spark.table(f"{NS}._DOMAINS").collect():
                domain_ids[r["domain_key"]] = r["domain_id"]
        # Fall back to listing from the API
        if not domain_ids:
            try:
                resp = _do("GET", DOMAINS_API)
                for d in resp.get("domains", []):
                    tk = d.get("tag_key", "")
                    did = d.get("domain_id")
                    if tk in DISCOVER_TAG_KEYS:
                        domain_ids[tk] = did
            except Exception:
                pass
        # Delete in order: subdomains first, parent last
        for tag_key in DISCOVER_TAG_KEYS:
            did = domain_ids.get(tag_key)
            if not did:
                print(f"  \u2022 Domain not found: {tag_key}")
                continue
            try:
                _do("DELETE", f"{DOMAINS_API}/{did}", params={"force": "true"})
                print(f"  \u2713 Deleted domain: {tag_key} (id={did})")
            except Exception as e:
                if "not found" in str(e).lower():
                    print(f"  \u2022 Already deleted: {tag_key}")
                else:
                    print(f"  \u2022 Could not delete domain {tag_key}: {e}")
    else:
        print("  (Domains API not available — skipping)")

    # ---- 5d) Delete governed tag policies ----
    print("\n--- Deleting governed tag policies ---")
    try:
        all_policies = list(w.tag_policies.list_tag_policies())
        for tag_key in DISCOVER_TAG_KEYS:
            found = [p for p in all_policies if p.tag_key == tag_key]
            for p in found:
                try:
                    _do("DELETE", f"/api/2.1/unity-catalog/tag-policies/{p.id}")
                    print(f"  \u2713 Deleted tag policy: {tag_key} (id={p.id})")
                except Exception as e:
                    if "not found" in str(e).lower():
                        print(f"  \u2022 Already deleted: {tag_key}")
                    else:
                        print(f"  \u2022 Could not delete tag policy {tag_key}: {e}")
            if not found:
                print(f"  \u2022 Tag policy not found: {tag_key}")
    except Exception as e:
        print(f"  \u2022 Could not list tag policies: {e}")

    print("\n\u2713 Discover teardown complete.")

# COMMAND ----------

# DBTITLE 1,Step 6 heading
# MAGIC %md
# MAGIC ## 6) Delete Lakebase: synced table → UC catalog → project (purged)
# MAGIC In dependency order. The synced table goes first (its pipeline writes into the
# MAGIC project), then the catalog registration, then the project with `purge=true`. A plain
# MAGIC delete only soft-deletes and blocks re-creating the same project name.

# COMMAND ----------

# DBTITLE 1,Delete Lakebase assets
import time

synced_id = f"{CATALOG}.{SYNC_SCHEMA}.risk_worklist"
try:
    _do("DELETE", f"/api/2.0/postgres/synced_tables/{synced_id}")
    print(f"  ✓ deleted synced table {synced_id}")
    time.sleep(10)
except Exception as e:
    print(f"  (synced table {synced_id} not found: {str(e)[:80]})")

try:
    _do("DELETE", f"/api/2.0/postgres/catalogs/{LAKEBASE_CATALOG}")
    print(f"  ✓ deleted UC catalog {LAKEBASE_CATALOG}")
except Exception as e:
    print(f"  (UC catalog {LAKEBASE_CATALOG} not found: {str(e)[:80]})")

try:
    _do("DELETE", f"/api/2.0/postgres/projects/{LAKEBASE_PROJECT}", params={"purge": "true"})
    print(f"  ✓ deleted and purged Lakebase project {LAKEBASE_PROJECT}")
except Exception as e:
    print(f"  (Lakebase project {LAKEBASE_PROJECT} not found: {str(e)[:80]})")

# COMMAND ----------

# DBTITLE 1,Step 7 heading
# MAGIC %md
# MAGIC ## 7) Drop the whole schema (tables, views, functions, metadata)
# MAGIC `DROP SCHEMA ... CASCADE` removes all 5 gold tables, the 13 raw tables, the 4
# MAGIC metric views, all SQL functions, `VALUE_DICTIONARY`, and the `_GENIE_SPACES` /
# MAGIC `_SUPERVISOR` / `_BENCHMARK_RESULTS` metadata tables in one shot.

# COMMAND ----------

for ns in (f"{CATALOG}.{SYNC_SCHEMA}", NS):
    try:
        spark.sql(f"DROP SCHEMA IF EXISTS {ns} CASCADE")
        print(f"  ✓ dropped schema {ns} (CASCADE)")
    except Exception as e:
        print(f"  (could not drop schema {ns}: {e})")

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

# DBTITLE 1,Exit
discover_msg = " (incl. Discover tags/domains/pages)" if DELETE_DISCOVER else " (Discover tags/domains/pages kept)"
dbutils.notebook.exit(f"99_teardown: SUCCESS — all demo assets under {NS} removed{discover_msg}.")