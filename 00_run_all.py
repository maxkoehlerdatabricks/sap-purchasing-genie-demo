# Databricks notebook source
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
# MAGIC 1. `_helper/01_simulate_raw_tables`   — generate the 13 RAW_* tables.
# MAGIC 2. `02_build_gold/02a..02e`           — Workflows A–E → 5 gold tables.
# MAGIC 3. `02_build_gold/02f_constraints_comments` — PK/FK + column/table comments.
# MAGIC 4. `03_semantics/03a..03c`            — SQL functions, metric views, value dicts.
# MAGIC
# MAGIC ## What it does NOT run (run these manually / separately)
# MAGIC * `04_resources/04a_resources`  — creates infra (warehouse ensure, permissions).
# MAGIC * `05_genie/05a_create_genie_spaces` — creates the 2 Genie spaces.
# MAGIC * `06_agent/06a_create_mas`      — creates the Agent Bricks MAS + endpoint.
# MAGIC * `07_benchmark/07a_benchmark`   — runs the accuracy benchmark.
# MAGIC * `08_jobs/08a_create_jobs`      — registers the serverless data-prep Job.
# MAGIC
# MAGIC These are deliberately separate because they create *account/workspace-level*
# MAGIC resources (Genie, endpoints, jobs) rather than just data, and you typically run
# MAGIC them once, review them in the UI, and re-run selectively.
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
CATALOG = "serverless_stable_7qzrfp_catalog"   # Unity Catalog (existing, taken as-is)
SCHEMA = "purchasing_demo"                       # Schema (created by the demo)
WAREHOUSE_ID = "117d425266741646"               # Serverless SQL Warehouse ID

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

# MAGIC %md
# MAGIC ## Done — data + semantics are ready
# MAGIC Next, run the separate notebooks (resources, Genie, MAS, benchmark, jobs) and
# MAGIC deploy the App. See `README.md` for the full sequence.

# COMMAND ----------

dbutils.notebook.exit("00_run_all: SUCCESS — 5 gold tables + semantics built.")
