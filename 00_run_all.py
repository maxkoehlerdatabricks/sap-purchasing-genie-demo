# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Cell 1
# MAGIC %md
# MAGIC # 00_run_all — TOP-LEVEL Orchestrator (parameterize ONCE here)
# MAGIC
# MAGIC This is the **only** notebook where you set `catalog` and `schema`. It assigns
# MAGIC those values to plain **Python variables** (`CATALOG = ...`, `SCHEMA = ...`,
# MAGIC `WAREHOUSE_ID = ...`) and then runs every child notebook in dependency order,
# MAGIC forwarding those Python variables as the child's parameters. Child notebooks read
# MAGIC the same names, so the parameters flow through the whole pipeline from a single
# MAGIC point of control — driven by Python assignments, not inline widget-dict literals.
# MAGIC
# MAGIC ## What it runs (in order)
# MAGIC 0. `_helper/00_preflight_checks`        — validate catalog perms, warehouse, governed tags.
# MAGIC 1. `_helper/01_simulate_raw_tables`   — generate the 13 RAW_* tables.
# MAGIC 2. `02_build_gold/02a..02e`           — Workflows A–E → 5 gold tables.
# MAGIC 3. `02_build_gold/02f_constraints_comments` — PK/FK + column/table comments.
# MAGIC 4. `03_semantics/03a..03c`            — SQL functions, metric views, value dicts.
# MAGIC 5. `04_resources/04a_resources`       — ensure warehouse + grant UC permissions.
# MAGIC 6. `05_genie/05a_create_genie_spaces` — create the 2 Genie spaces + instructions.
# MAGIC 7. `06_agent/06a_create_mas`          — create the Agent Bricks MAS + endpoint.
# MAGIC 8. `09_domains/09a_create_domains`    — create Discover domains, subdomains & Pages.
# MAGIC 9. `07_benchmark/07a_benchmark`       — run the accuracy benchmark.
# MAGIC 10. `08_jobs/08a_create_jobs`          — register the serverless data-prep Job.
# MAGIC 11. `app/deploy_app`                   — create & deploy the Databricks App.
# MAGIC
# MAGIC ## What it does NOT run
# MAGIC * `99_teardown` — destructive cleanup (deletes everything). Run manually when needed.
# MAGIC
# MAGIC > **Compute:** run this on **Serverless** interactive compute.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set the parameters ONCE — as plain Python variables
# MAGIC Change these three assignments to point the whole demo at a different
# MAGIC catalog / schema / warehouse. No widget dictionaries are used anywhere below —
# MAGIC everything downstream is driven from these Python variables.

# COMMAND ----------

# ---- The single source of truth for the whole pipeline (Python "X = ..." style) ----
CATALOG = "classic_stable_3n1juw_catalog"   # Unity Catalog (existing, taken as-is)
SCHEMA = "purchasing_demo"                       # Schema (created by the demo)
WAREHOUSE_ID = "db9813035505e7b0"               # Serverless SQL Warehouse ID

print(f"Orchestrating with: catalog={CATALOG}  schema={SCHEMA}  warehouse_id={WAREHOUSE_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper to run a child notebook with shared params + timing

# COMMAND ----------

import time

# Default per-notebook timeout (seconds). Simulation + gold builds are the heaviest.
DEFAULT_TIMEOUT = 3600


def run_child(path: str, catalog: str = CATALOG, schema: str = SCHEMA,
              warehouse_id: str = WAREHOUSE_ID, timeout: int = DEFAULT_TIMEOUT):
    """Run a child notebook (relative path), forwarding the shared Python variables.

    The parameters are taken from the ``CATALOG`` / ``SCHEMA`` / ``WAREHOUSE_ID``
    Python variables set once above (passed here as default argument values), then
    assembled into the child's arguments at call time. This keeps the parameterization
    driven by Python ``X = ...`` assignments rather than an inline widget dictionary.
    """
    print(f"\n{'=' * 70}\n▶ Running: {path}\n{'=' * 70}")
    args = {"catalog": catalog, "schema": schema, "warehouse_id": warehouse_id}
    t0 = time.time()
    result = dbutils.notebook.run(path, timeout, arguments=args)
    dt = time.time() - t0
    print(f"✓ Finished: {path}  ({dt:,.1f}s)  → {result}")
    return result


# COMMAND ----------

# DBTITLE 1,Step 0 — Preflight checks
# MAGIC %md
# MAGIC ## 0) Preflight checks
# MAGIC Validate catalog permissions, SQL warehouse usability, and governed-tag support
# MAGIC before running the pipeline. Fails fast with a clear message if anything is wrong.

# COMMAND ----------

# DBTITLE 1,Run preflight checks
result = run_child("./_helper/00_preflight_checks")

print(f"\n{'=' * 70}")
print("  PREFLIGHT REPORT")
print(f"{'=' * 70}")
print(result)
print(f"{'=' * 70}")

if "FAILED" in result:
    print("\n❌ Pipeline halted. Fix the issues above, then re-run this cell.")
    print("   Open _helper/00_preflight_checks for the full diagnostic output.")
    raise RuntimeError(result.splitlines()[0])
else:
    print("\n✅ All clear — proceeding with pipeline.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1) Simulate the 13 raw tables

# COMMAND ----------

run_child("./_helper/01_simulate_raw_tables")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2) Build the 5 gold tables (Workflows A–E, in dependency order)
# MAGIC Order matters: C depends on B's LIFNR set; E depends on A/B keys for validation.

# COMMAND ----------

run_child("./02_build_gold/02a_material")     # Workflow A → MATERIAL
run_child("./02_build_gold/02b_supplier")     # Workflow B → SUPPLIER
run_child("./02_build_gold/02c_sourcing")     # Workflow C → SOURCING
run_child("./02_build_gold/02d_supplier_risk")# Workflow D → SUPPLIER_RISK
run_child("./02_build_gold/02e_purchase_order")# Workflow E → PURCHASE_ORDER

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3) Add PK/FK constraints + column & table comments to all gold tables

# COMMAND ----------

run_child("./02_build_gold/02f_constraints_comments")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4) Build the semantic layer (SQL functions, metric views, value dictionaries)

# COMMAND ----------

run_child("./03_semantics/03a_sql_functions")
run_child("./03_semantics/03b_metric_views")
run_child("./03_semantics/03c_value_dictionaries")

# COMMAND ----------

# DBTITLE 1,Step 5 — Warehouse & permissions
# MAGIC %md
# MAGIC ## 5) Ensure warehouse & grant UC permissions

# COMMAND ----------

# DBTITLE 1,Run 04a_resources
run_child("./04_resources/04a_resources")

# COMMAND ----------

# DBTITLE 1,Step 6 — Genie spaces & MAS
# MAGIC %md
# MAGIC ## 6) Create the 2 Genie spaces + Agent Bricks MAS
# MAGIC Genie spaces must exist before the MAS, and the MAS waits for its serving
# MAGIC endpoint to reach READY (may take a few minutes).

# COMMAND ----------

# DBTITLE 1,Run 05a + 06a
run_child("./05_genie/05a_create_genie_spaces")  # 2 domain-split Genie spaces
run_child("./06_agent/06a_create_mas")            # Agent Bricks MAS + serving endpoint

# COMMAND ----------

# DBTITLE 1,Step 7 — Discover domains & Pages
# MAGIC %md
# MAGIC ## 7) Create Discover domains, subdomains & governed Pages
# MAGIC Organises gold tables, metric views, functions, and Genie spaces into a
# MAGIC business-aligned hierarchy on the Discover page. Creates 5 governed Pages
# MAGIC (business glossary) that Genie One reads as ontology.

# COMMAND ----------

# DBTITLE 1,Run 09a_create_domains
run_child("./09_domains/09a_create_domains")

# COMMAND ----------

# DBTITLE 1,Step 7 — Benchmark, Job & App
# MAGIC %md
# MAGIC ## 8) Benchmark, serverless Job & App deployment
# MAGIC Benchmark validates accuracy against known-good SQL (and optionally queries the
# MAGIC MAS). The Job wraps `00_run_all` for scheduled re-runs. The App is deployed last.

# COMMAND ----------

# DBTITLE 1,Run 07a + 08a + deploy_app
run_child("./07_benchmark/07a_benchmark")    # accuracy benchmark
run_child("./08_jobs/08a_create_jobs")       # serverless data-prep Job
run_child("./app/deploy_app")               # Databricks App (React + FastAPI)

# COMMAND ----------

# DBTITLE 1,Cell 14
# MAGIC %md
# MAGIC ## Done — full deployment complete
# MAGIC All data, semantics, Genie spaces, MAS endpoint, Discover domains & Pages,
# MAGIC benchmark, Job, and App have been deployed. Run `99_teardown` to clean up
# MAGIC when you're done.

# COMMAND ----------

# DBTITLE 1,Cell 15
dbutils.notebook.exit("00_run_all: SUCCESS — full deployment (data + semantics + Genie + MAS + domains + benchmark + Job + App).")