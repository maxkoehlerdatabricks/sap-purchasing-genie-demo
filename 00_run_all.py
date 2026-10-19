# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Title
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
# MAGIC 1. `_helper/01_simulate_raw_tables`   — generate 12 RAW_* tables + the SAP Ariba supplier-risk export files.
# MAGIC 2. `02_build_gold/02a..02e`           — Workflows A–E → 5 gold tables. Workflow D (`02d`) runs a
# MAGIC    **Lakeflow Declarative Pipeline**: Auto Loader → `RAW_SUPPLIER_RISK` → gold `SUPPLIER_RISK`.
# MAGIC 3. `02_build_gold/02f_constraints_comments` — PK/FK + column/table comments.
# MAGIC 4. `03_semantics/03a..03c`            — SQL functions, metric views, value dicts.
# MAGIC 5. `04_resources/04a_resources`       — ensure warehouse + grant UC permissions.
# MAGIC 6. `10_lakebase/10a_create_lakebase`  — Lakebase project, case tables, seed data, UC catalog, case functions.
# MAGIC    `10_lakebase/10b_sync_worklist`    — RISK_WORKLIST → Lakebase synced table; case snapshot → Delta.
# MAGIC 7. `05_genie/05a_create_genie_spaces` — create the 2 Genie spaces + instructions (incl. case functions).
# MAGIC 8. `06_agent/06a_create_mas`          — create the Agent Bricks MAS + endpoint.
# MAGIC 9. `07_benchmark/07a_benchmark`       — run the accuracy benchmark.
# MAGIC 10. `app/deploy_app`                  — create & deploy the Databricks App (charts, chat, cases).
# MAGIC 11. `08_jobs/08a_create_jobs`         — register the multi-task Lakeflow Job and run it once.
# MAGIC 12. `09_domains/09a_create_domains`   — create governed tags, Discover domains & subdomains.
# MAGIC     `09_domains/09b_create_pages`     — create governed Pages (**commented out** — Pages API not yet available).
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
CATALOG = "max_kohler_demo_catalog"   # Unity Catalog (existing, taken as-is)
SCHEMA = "purchasing_demo"                       # Schema (created by the demo)
WAREHOUSE_ID = "d7a3d1cfbba73c0a"               # Serverless SQL Warehouse ID

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
# MAGIC ## 1) Simulate the raw SAP tables (12 Delta tables + Ariba export files in a volume)

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
run_child("./02_build_gold/02d_supplier_risk")# Workflow D → Lakeflow pipeline → SUPPLIER_RISK
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

# DBTITLE 1,Step 6 — Lakebase
# MAGIC %md
# MAGIC ## 6) Lakebase — the operational layer for mitigation cases
# MAGIC `10a` creates the Lakebase project, the case tables with seed data and the UC catalog
# MAGIC over Lakebase (and resets the cases to the demo seed state). `10b` builds the
# MAGIC `RISK_WORKLIST` and syncs it into Lakebase for the App. Runs before Genie, so space 1
# MAGIC can register the live case functions.

# COMMAND ----------

# DBTITLE 1,Run 10a + 10b
run_child("./10_lakebase/10a_create_lakebase")   # project, schema, seed data, UC catalog, functions
run_child("./10_lakebase/10b_sync_worklist")     # Delta worklist -> Lakebase synced table

# COMMAND ----------

# DBTITLE 1,Step 7 — Genie spaces & MAS
# MAGIC %md
# MAGIC ## 7) Create the 2 Genie spaces + Agent Bricks MAS
# MAGIC Genie spaces must exist before the MAS, and the MAS waits for its serving
# MAGIC endpoint to reach READY (may take a few minutes).

# COMMAND ----------

# DBTITLE 1,Run 05a + 06a
run_child("./05_genie/05a_create_genie_spaces")  # 2 domain-split Genie spaces
run_child("./06_agent/06a_create_mas")            # Agent Bricks MAS + serving endpoint

# COMMAND ----------

# DBTITLE 1,Step 8 — Benchmark & App
# MAGIC %md
# MAGIC ## 8) Benchmark & App deployment
# MAGIC Benchmark validates accuracy against known-good SQL (and optionally queries the
# MAGIC MAS). The App gets the charts, the MAS chat and the Lakebase cases tab.

# COMMAND ----------

# DBTITLE 1,Run 07a + deploy_app
run_child("./07_benchmark/07a_benchmark")    # accuracy benchmark
run_child("./app/deploy_app")               # Databricks App (React + FastAPI + Lakebase)

# COMMAND ----------

# DBTITLE 1,Step 9 — Lakeflow Job
# MAGIC %md
# MAGIC ## 9) Lakeflow Job — the same pipeline as an orchestrated task graph
# MAGIC Registers the multi-task job (raw → gold → DQ gate → semantics → Lakebase) and
# MAGIC runs it once, so the demo has a completed run to show.

# COMMAND ----------

# DBTITLE 1,Run 08a
run_child("./08_jobs/08a_create_jobs", timeout=7200)  # create + run the Lakeflow job

# COMMAND ----------

# DBTITLE 1,Step 10 — Discover domains
# MAGIC %md
# MAGIC ## 10) Create Discover domains & subdomains (tags + asset tagging + publish)
# MAGIC Organises gold tables, metric views, functions, and Genie spaces into a
# MAGIC business-aligned hierarchy on the Discover page.
# MAGIC
# MAGIC > **Pages** (business glossary) are in a separate notebook `09b_create_pages`.
# MAGIC > The Pages API is not yet available on this workspace, so the call is
# MAGIC > commented out below. See `09b_create_pages` for manual creation instructions
# MAGIC > (UI or Genie Code prompt) and how to publish pages from draft to production.

# COMMAND ----------

# DBTITLE 1,Run 09a (+ 09b commented out)
run_child("./09_domains/09a_create_domains")  # governed tags + domains + asset tagging + publish

# ---- Pages (commented out — Pages API not yet available on this workspace) ----
# Uncomment the line below when the /api/2.0/pages endpoint is enabled.
# Until then, create pages manually — see 09b_create_pages for instructions.
# run_child("./09_domains/09b_create_pages")

displayHTML("""
<div style="border:3px solid #D32F2F; border-radius:10px; padding:18px 22px; margin:16px 0;
            background:#FFEBEE; color:#B71C1C; font-size:15px; line-height:1.6;">
  <strong style="font-size:17px;">⚠️ MANUAL STEP REQUIRED — Governed Pages</strong><br/>
  The 5 governed <b>Pages</b> (business glossary) must be created <b>manually</b>
  because the Pages REST API is not yet available on this workspace.<br/><br/>
  👉 Scroll down to <b>"Manual Page Creation"</b> (the cells below this one)
  for step-by-step instructions (Discover UI or Genie Code prompt) and the
  full content for each page.<br/><br/>
  After creating them, <b>publish</b> each page (draft → production) so Genie One
  can read them as ontology.
</div>
""")

# COMMAND ----------

# DBTITLE 1,Manual Page creation instructions
# MAGIC %md
# MAGIC ## Manual Page Creation (Pages API not yet available)
# MAGIC
# MAGIC The 5 governed **Pages** below are business-term definitions that Genie One reads
# MAGIC as ontology. Because the Pages REST API (`/api/2.0/pages`) is not yet exposed on
# MAGIC this workspace, create them manually using one of the two methods below.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Method A — Create via the Discover UI
# MAGIC
# MAGIC 1. Open **Discover** (left sidebar → Discover).
# MAGIC 2. Click the target **domain** or **subdomain** (noted in each page below).
# MAGIC 3. Click **"+ New page"** (top-right within the domain).
# MAGIC 4. Fill in the **Name**, **Description**, **Synonyms**, and **Body** exactly as shown.
# MAGIC 5. Optionally attach **Related assets** (tables, metric views, functions).
# MAGIC 6. Click **Save** — this creates the page as a **draft**.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Method B — Create via Genie Code prompt
# MAGIC
# MAGIC 1. Open the **Discover** page in your workspace.
# MAGIC 2. Open **Genie Code** (the assistant sidebar).
# MAGIC 3. Copy the prompt from each page cell below and paste it into Genie Code.
# MAGIC 4. Genie Code will create the page for you (as a **draft**).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Publishing a Page (draft → production)
# MAGIC
# MAGIC 1. Open **Discover** → navigate to the domain/subdomain containing the page.
# MAGIC 2. Click the **page name** to open it.
# MAGIC 3. Click the **"Publish"** button (top-right).
# MAGIC 4. Confirm the publish action.
# MAGIC 5. The page is now **live** and visible to all workspace users. Genie One will
# MAGIC    read it as part of its ontology.
# MAGIC
# MAGIC > **Tip:** You can edit a published page at any time. Edits create a new draft
# MAGIC > that must be re-published to take effect.

# COMMAND ----------

# DBTITLE 1,Page 1 — Multi-Sourcing Strategy
# MAGIC %md
# MAGIC ### Page 1 — Multi-Sourcing Strategy
# MAGIC
# MAGIC | Field | Value |
# MAGIC | --- | --- |
# MAGIC | **Attach to** | Sourcing & Supplier Risk (subdomain) |
# MAGIC | **Description** | What multi-sourcing means and why it matters for supply continuity. |
# MAGIC | **Synonyms** | multi-sourcing, single-sourced, dual sourcing, second source |
# MAGIC | **Related assets** | `mv_sourcing_coverage`, `SOURCING` |
# MAGIC
# MAGIC **Body:**
# MAGIC
# MAGIC > **Definition** — A material is **multi-sourced** when it has ≥2 valid,
# MAGIC > non-blocked vendors (IS_VALID_SOURCE = true) in its source list at the relevant
# MAGIC > plant. Materials below this threshold are flagged as **single-source risks**.
# MAGIC >
# MAGIC > **Why it matters** — Single-sourced materials create supply-chain concentration
# MAGIC > risk: if the sole vendor experiences disruption, the production line has no
# MAGIC > fallback. Multi-sourcing is the primary mitigation.
# MAGIC >
# MAGIC > **Threshold** — The standard threshold is **2**. The certified function
# MAGIC > `materials_without_multisourcing(min_vendors)` is parameterized so the
# MAGIC > threshold can be adjusted.
# MAGIC >
# MAGIC > **Related assets:**
# MAGIC > * **Metric view:** `mv_sourcing_coverage` — vendor count by material, plant, group
# MAGIC > * **Function:** `materials_without_multisourcing(min_vendors)` — Q1
# MAGIC > * **Table:** `SOURCING` (IS_VALID_SOURCE column)
# MAGIC
# MAGIC **Genie Code prompt:**
# MAGIC
# MAGIC ````text
# MAGIC Create a governed Page called "Multi-Sourcing Strategy" in the "Sourcing & Supplier Risk" subdomain.
# MAGIC Description: What multi-sourcing means and why it matters for supply continuity.
# MAGIC Synonyms: multi-sourcing, single-sourced, dual sourcing, second source
# MAGIC Body:
# MAGIC ## Definition
# MAGIC A material is multi-sourced when it has ≥2 valid, non-blocked vendors (IS_VALID_SOURCE = true) in its source list at the relevant plant. Materials below this threshold are flagged as single-source risks.
# MAGIC ## Why it matters
# MAGIC Single-sourced materials create supply-chain concentration risk: if the sole vendor experiences disruption, the production line has no fallback. Multi-sourcing is the primary mitigation.
# MAGIC ## Threshold
# MAGIC The standard threshold is 2. The certified function materials_without_multisourcing(min_vendors) is parameterized so the threshold can be adjusted.
# MAGIC ## Related assets
# MAGIC * Metric view: mv_sourcing_coverage — vendor count by material, plant, group
# MAGIC * Function: materials_without_multisourcing(min_vendors) — Q1
# MAGIC * Table: SOURCING (IS_VALID_SOURCE column)
# MAGIC ````

# COMMAND ----------

# DBTITLE 1,Page 2 — Delivery Risk
# MAGIC %md
# MAGIC ### Page 2 — Delivery Risk
# MAGIC
# MAGIC | Field | Value |
# MAGIC | --- | --- |
# MAGIC | **Attach to** | Sourcing & Supplier Risk (subdomain) |
# MAGIC | **Description** | How delivery risk is defined, assessed, and why it is time-sliced. |
# MAGIC | **Synonyms** | delivery risk, supplier risk, risk flag, risk score, risk window |
# MAGIC | **Related assets** | `mv_supplier_risk`, `SUPPLIER_RISK` |
# MAGIC
# MAGIC **Body:**
# MAGIC
# MAGIC > **Definition** — Delivery risk is a **time-sliced** assessment. A supplier
# MAGIC > carries **active** delivery risk when:
# MAGIC > 1. `DELIVERY_RISK_FLAG = 'X'`, **and**
# MAGIC > 2. The reference date falls **between VALID_FROM and VALID_TO**.
# MAGIC >
# MAGIC > Risk outside this window is historical, not current.
# MAGIC >
# MAGIC > **Risk dimensions** — Each risk record carries:
# MAGIC > * **RISK_CATEGORY** — financial, quality, geopolitical, or logistics
# MAGIC > * **RISK_SCORE** — numeric 0–100 (higher = more severe)
# MAGIC > * **DELIVERY_RISK_FLAG** — 'X' if the supplier is flagged
# MAGIC >
# MAGIC > **Why it matters** — A material that is **single-sourced AND its sole supplier
# MAGIC > has active delivery risk** is the highest-priority supply-continuity exposure.
# MAGIC >
# MAGIC > **Related assets:**
# MAGIC > * **Metric view:** `mv_supplier_risk` — risk exposure by category/flag/country
# MAGIC > * **Function:** `single_sourced_risky(as_of_date)` — Q3
# MAGIC > * **Table:** `SUPPLIER_RISK` (VALID_FROM, VALID_TO columns)
# MAGIC
# MAGIC **Genie Code prompt:**
# MAGIC
# MAGIC ````text
# MAGIC Create a governed Page called "Delivery Risk" in the "Sourcing & Supplier Risk" subdomain.
# MAGIC Description: How delivery risk is defined, assessed, and why it is time-sliced.
# MAGIC Synonyms: delivery risk, supplier risk, risk flag, risk score, risk window
# MAGIC Body:
# MAGIC ## Definition
# MAGIC Delivery risk is a time-sliced assessment. A supplier carries active delivery risk when:
# MAGIC 1. DELIVERY_RISK_FLAG = 'X', and
# MAGIC 2. The reference date falls between VALID_FROM and VALID_TO.
# MAGIC Risk outside this window is historical, not current.
# MAGIC ## Risk dimensions
# MAGIC Each risk record carries:
# MAGIC * RISK_CATEGORY — financial, quality, geopolitical, or logistics
# MAGIC * RISK_SCORE — numeric 0–100 (higher = more severe)
# MAGIC * DELIVERY_RISK_FLAG — 'X' if the supplier is flagged
# MAGIC ## Why it matters
# MAGIC A material that is single-sourced AND its sole supplier has active delivery risk is the highest-priority supply-continuity exposure.
# MAGIC ## Related assets
# MAGIC * Metric view: mv_supplier_risk — risk exposure by category/flag/country
# MAGIC * Function: single_sourced_risky(as_of_date) — Q3
# MAGIC * Table: SUPPLIER_RISK (VALID_FROM, VALID_TO columns)
# MAGIC ````

# COMMAND ----------

# DBTITLE 1,Page 3 — On-Time Delivery (OTD)
# MAGIC %md
# MAGIC ### Page 3 — On-Time Delivery (OTD)
# MAGIC
# MAGIC | Field | Value |
# MAGIC | --- | --- |
# MAGIC | **Attach to** | Purchase Order Analytics (subdomain) |
# MAGIC | **Description** | How OTD rate is calculated and what counts as on-time. |
# MAGIC | **Synonyms** | OTD, on-time delivery, on-time rate, delivery performance |
# MAGIC | **Related assets** | `mv_delivery_performance`, `PURCHASE_ORDER` |
# MAGIC
# MAGIC **Body:**
# MAGIC
# MAGIC > **Definition** — **OTD rate** = on-time lines ÷ received lines. A PO line is
# MAGIC > **on-time** when `GR_DATE ≤ REQUESTED_DATE` (goods received by the requested
# MAGIC > date). Lines where `GR_DATE IS NULL` (not yet received) are **excluded** from
# MAGIC > both numerator and denominator.
# MAGIC >
# MAGIC > **Standard window** — The default analysis window is the **last 12 months** of
# MAGIC > goods-receipt dates.
# MAGIC >
# MAGIC > **Why it matters** — OTD is the primary measure of supplier delivery
# MAGIC > performance. Suppliers with persistently low OTD rates are candidates for
# MAGIC > escalation, re-sourcing, or additional risk mitigation.
# MAGIC >
# MAGIC > **Related assets:**
# MAGIC > * **Metric view:** `mv_delivery_performance` — OTD % by supplier/plant/month
# MAGIC > * **Table:** `PURCHASE_ORDER` (ON_TIME_FLAG, GR_DATE columns)
# MAGIC
# MAGIC **Genie Code prompt:**
# MAGIC
# MAGIC ````text
# MAGIC Create a governed Page called "On-Time Delivery (OTD)" in the "Purchase Order Analytics" subdomain.
# MAGIC Description: How OTD rate is calculated and what counts as on-time.
# MAGIC Synonyms: OTD, on-time delivery, on-time rate, delivery performance
# MAGIC Body:
# MAGIC ## Definition
# MAGIC OTD rate = on-time lines ÷ received lines. A PO line is on-time when GR_DATE ≤ REQUESTED_DATE (goods received by the requested date). Lines where GR_DATE IS NULL (not yet received) are excluded from both numerator and denominator.
# MAGIC ## Standard window
# MAGIC The default analysis window is the last 12 months of goods-receipt dates.
# MAGIC ## Why it matters
# MAGIC OTD is the primary measure of supplier delivery performance. Suppliers with persistently low OTD rates are candidates for escalation, re-sourcing, or additional risk mitigation.
# MAGIC ## Related assets
# MAGIC * Metric view: mv_delivery_performance — OTD % by supplier/plant/month
# MAGIC * Table: PURCHASE_ORDER (ON_TIME_FLAG, GR_DATE columns)
# MAGIC ````

# COMMAND ----------

# DBTITLE 1,Page 4 — Quota Arrangement
# MAGIC %md
# MAGIC ### Page 4 — Quota Arrangement
# MAGIC
# MAGIC | Field | Value |
# MAGIC | --- | --- |
# MAGIC | **Attach to** | Sourcing & Supplier Risk (subdomain) |
# MAGIC | **Description** | How procurement volume is split across approved vendors. |
# MAGIC | **Synonyms** | quota, quota split, quota concentration, vendor share |
# MAGIC | **Related assets** | `SOURCING` |
# MAGIC
# MAGIC **Body:**
# MAGIC
# MAGIC > **Definition** — A **quota arrangement** splits procurement of a material across
# MAGIC > approved vendors by percentage. The `QUOTA` column in the SOURCING table
# MAGIC > represents the vendor's allocated share.
# MAGIC >
# MAGIC > **NULL quota** — A `NULL` quota means the vendor is **listed as a valid source**
# MAGIC > but has **no formal quota split** assigned. This is a meaningful gap, not a data
# MAGIC > error — it indicates the source list is approved but procurement volume
# MAGIC > allocation is not governed.
# MAGIC >
# MAGIC > **Concentration risk** — When a single vendor holds **>80%** of the quota for a
# MAGIC > material, it represents a concentration risk even if the material is technically
# MAGIC > multi-sourced. The function `quota_concentration(threshold_pct)` flags these
# MAGIC > cases.
# MAGIC >
# MAGIC > **Related assets:**
# MAGIC > * **Function:** `quota_concentration(threshold_pct)` — Q6
# MAGIC > * **Table:** `SOURCING` (QUOTA column)
# MAGIC
# MAGIC **Genie Code prompt:**
# MAGIC
# MAGIC ````text
# MAGIC Create a governed Page called "Quota Arrangement" in the "Sourcing & Supplier Risk" subdomain.
# MAGIC Description: How procurement volume is split across approved vendors.
# MAGIC Synonyms: quota, quota split, quota concentration, vendor share
# MAGIC Body:
# MAGIC ## Definition
# MAGIC A quota arrangement splits procurement of a material across approved vendors by percentage. The QUOTA column in the SOURCING table represents the vendor's allocated share.
# MAGIC ## NULL quota
# MAGIC A NULL quota means the vendor is listed as a valid source but has no formal quota split assigned. This is a meaningful gap, not a data error — it indicates the source list is approved but procurement volume allocation is not governed.
# MAGIC ## Concentration risk
# MAGIC When a single vendor holds >80% of the quota for a material, it represents a concentration risk even if the material is technically multi-sourced. The function quota_concentration(threshold_pct) flags these cases.
# MAGIC ## Related assets
# MAGIC * Function: quota_concentration(threshold_pct) — Q6
# MAGIC * Table: SOURCING (QUOTA column)
# MAGIC ````

# COMMAND ----------

# DBTITLE 1,Page 5 — SAP Code Glossary
# MAGIC %md
# MAGIC ### Page 5 — SAP Code Glossary
# MAGIC
# MAGIC | Field | Value |
# MAGIC | --- | --- |
# MAGIC | **Attach to** | SAP Purchasing **parent domain** (not a subdomain) |
# MAGIC | **Description** | How cryptic SAP codes are translated to human-readable labels. |
# MAGIC | **Synonyms** | SAP codes, value dictionary, code mapping, MTART, SPERM, NOTKZ |
# MAGIC | **Related assets** | `VALUE_DICTIONARY` |
# MAGIC
# MAGIC **Body:**
# MAGIC
# MAGIC > **Definition** — SAP uses **short alphanumeric codes** throughout its modules.
# MAGIC > The `VALUE_DICTIONARY` table maps each code to a human-readable label so that
# MAGIC > Genie (and humans) can translate between plain English and SAP terminology.
# MAGIC >
# MAGIC > **Key domains:**
# MAGIC >
# MAGIC > | Code domain | Example code | Meaning |
# MAGIC > | --- | --- | --- |
# MAGIC > | MTART (material type) | ROH | Raw material |
# MAGIC > | MATKL (material group) | 001 | Mechanical components |
# MAGIC > | SPERM (purchasing block) | X | Supplier is blocked for purchasing |
# MAGIC > | NOTKZ (source-list block) | X | Source record is blocked |
# MAGIC > | DELIVERY_RISK_FLAG | X | Active delivery risk |
# MAGIC > | LAND1 (country) | DE | Germany |
# MAGIC >
# MAGIC > **How Genie uses it** — When a user asks about 'blocked suppliers', Genie
# MAGIC > translates this to `SPERM = 'X'` using the value dictionary. When a user asks
# MAGIC > about 'raw materials', Genie translates to `MTART = 'ROH'`.
# MAGIC >
# MAGIC > **Related assets:**
# MAGIC > * **Table:** `VALUE_DICTIONARY` (DOMAIN, CODE, LABEL, DESCRIPTION columns)
# MAGIC
# MAGIC **Genie Code prompt:**
# MAGIC
# MAGIC ````text
# MAGIC Create a governed Page called "SAP Code Glossary" in the "SAP Purchasing" PARENT domain (not a subdomain).
# MAGIC Description: How cryptic SAP codes are translated to human-readable labels.
# MAGIC Synonyms: SAP codes, value dictionary, code mapping, MTART, SPERM, NOTKZ
# MAGIC Body:
# MAGIC ## Definition
# MAGIC SAP uses short alphanumeric codes throughout its modules. The VALUE_DICTIONARY table maps each code to a human-readable label so that Genie (and humans) can translate between plain English and SAP terminology.
# MAGIC ## Key domains
# MAGIC | Code domain | Example code | Meaning |
# MAGIC | --- | --- | --- |
# MAGIC | MTART (material type) | ROH | Raw material |
# MAGIC | MATKL (material group) | 001 | Mechanical components |
# MAGIC | SPERM (purchasing block) | X | Supplier is blocked for purchasing |
# MAGIC | NOTKZ (source-list block) | X | Source record is blocked |
# MAGIC | DELIVERY_RISK_FLAG | X | Active delivery risk |
# MAGIC | LAND1 (country) | DE | Germany |
# MAGIC ## How Genie uses it
# MAGIC When a user asks about 'blocked suppliers', Genie translates this to SPERM = 'X' using the value dictionary. When a user asks about 'raw materials', Genie translates to MTART = 'ROH'.
# MAGIC ## Related assets
# MAGIC * Table: VALUE_DICTIONARY (DOMAIN, CODE, LABEL, DESCRIPTION columns)
# MAGIC ````

# COMMAND ----------

# DBTITLE 1,Cell 14
# MAGIC %md
# MAGIC ## Done — full deployment complete
# MAGIC All data, semantics, Lakebase (cases + synced worklist + UC catalog), Genie
# MAGIC spaces, MAS endpoint, benchmark, App, the Lakeflow Job (with one completed run)
# MAGIC and Discover domains have been deployed. Run `99_teardown` to clean up
# MAGIC when you're done.

# COMMAND ----------

# DBTITLE 1,Cell 15
dbutils.notebook.exit("00_run_all: SUCCESS — full deployment (data + semantics + Lakebase + Genie + MAS + benchmark + App + Lakeflow Job + domains).")