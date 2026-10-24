# SAP Purchasing — Sourcing Risk & PO Analytics (Databricks Demo)

A **demo-ready**, reproducible Databricks demo that simulates an automotive OEM's SAP
purchasing landscape so a purchasing business analyst can answer **sourcing-risk** and
**supply-continuity** questions *without SAP access*.

It builds, end-to-end and on a blank workspace:

- **13 simulated raw SAP tables** → **5 governed gold tables** (with PK/FK constraints,
  table & column comments).
- A **semantic layer**: parameterized **SQL functions** (trusted assets), **UC metric
  views**, and a **value dictionary**.
- **2 Genie Agents** (formerly Genie spaces; domain-split) with **all instruction types**
  (ontology snippets) + certified answers.
- A **supervisor agent** over the two Genie Agents, built with the **Agent Bricks CLI**
  (LangGraph, `DurableAgentServer`) and deployed to **Agent Runtime** as the app
  `agent-bricks-sap-supervisor`, with MLflow tracing. It replaces the legacy Supervisor
  Agent (MAS): Supervisor Agent is a legacy offering and the Supervisor API reached end of
  life on Sep 30, 2026.
- **Governed tags**, **Discover domains/subdomains**, and **5 governed Pages** (business
  glossary) — the ontology layer that Genie One reads.
- A **benchmark** of representative questions with known-good SQL and an **accuracy
  score**.
- A **Databricks App** (React + FastAPI) with visualizations, a chat interface over
  the supervisor agent, and a **Mitigation cases** tab where buyers act on the risks.
- A **Lakebase** (managed Postgres) operational layer: the risk worklist is synced from
  Delta into Lakebase, the App writes mitigation cases there, and a **Unity Catalog
  catalog over Lakebase** lets Genie read the cases live (closed loop).
- A multi-task **Lakeflow Job** that refreshes it all: raw → gold → data-quality gate →
  semantics → Lakebase.

> No Asset Bundles — every notebook runs **manually**. Parameterize **once** in the
> top-level orchestrator.

---

## The business questions this demo answers

1. Which materials have no multi-sourcing strategy (< 2 valid, non-blocked vendors)?
2. Which materials are linked to a supplier flagged with delivery risk?
3. Which materials are single-sourced AND their sole supplier has a delivery-risk flag?
4. What is the vendor count per material at each plant?
5. Which suppliers hold a purchasing block but still have active sourcing records?
6. Which materials have quota arrangements where one vendor holds over 80% of quota?
7. What is the on-time delivery rate per supplier over the last 12 months?
8. Which materials had POs placed with a supplier despite an active delivery-risk flag at order date?
9. Which suppliers provide materials across more than one material group?
10. Which materials have no valid sourcing record at all?
11. What is the average net-price variance across vendors for the same material?

All 11 are answerable from the **5 gold tables** alone (never the raw tables).

With the Lakebase module, one more question closes the loop between insight and action:

12. Which single-sourced risky materials have **no open mitigation case**? (gold layer +
    live Lakebase cases, via `single_sourced_risky_without_open_case()`)

---

## Architecture & data model

```
13 RAW_* (simulated SAP tables)                5 GOLD tables            Semantics
──────────────────────────────────            ───────────────          ─────────────────────
MARA, MAKT, MARC        ─ Workflow A ─▶  MATERIAL (MATNR+WERKS)    ┐
LFA1, LFB1, LFM1        ─ Workflow B ─▶  SUPPLIER (LIFNR+EKORG)    │   SQL functions (trusted)
EORD, EQUK, EQUP        ─ Workflow C ─▶  SOURCING (MATNR+WERKS+LIFNR)│  UC metric views
SUPPLIER_RISK           ─ Workflow D ─▶  SUPPLIER_RISK (LIFNR, time)│   Value dictionary
EKKO, EKPO, EKBE        ─ Workflow E ─▶  PURCHASE_ORDER (EBELN+EBELP)┘
                                              │
                          ┌───────────────────┴────────────────────┐
                    Genie Agent 1                              Genie Agent 2
              Sourcing & Supplier Risk                    Purchase Order Analytics
                          └───────────────────┬────────────────────┘
                     Supervisor agent (Agent Bricks CLI, on Agent Runtime)
                     tools: sourcing_supplier_risk / purchase_order_analytics
                                               │
                                       Databricks App
                         (Visualizations + Chat + Mitigation cases)
```

### Lakebase loop: from insight to action

```
 Delta RISK_WORKLIST ──(synced table, TRIGGERED)──▶ Lakebase purchasing_ops
 (single-sourced + risky today)                       purchasing_demo_sync.risk_worklist
                                                      ops.mitigation_case / case_event   ◀── App writes cases (ms)
                                                      ops.buyer / ops.action_type        (seed CSVs)
                                                                │
          UC catalog sap_purchasing_ops (registered over the Lakebase database, read live)
                                                                │
   single_sourced_risky_without_open_case() / open_mitigation_cases()  ──▶ Genie Agent 1 + supervisor
   Lakeflow Job snapshot ──▶ Delta MITIGATION_CASE_HISTORY
```

* The worklist is computed in the Lakehouse and pushed to Postgres, so the App reads and
  writes in milliseconds (foreign keys, CHECK constraints, a partial unique index of one
  active case per material+plant, and an audit trail).
* The cases come back to the governed layer **without a copy**: the UC catalog over
  Lakebase is queried live by the warehouse, so Genie sees a new case immediately.
* Lakebase change data feed into Delta needs a catalog with external storage. On a
  Default Storage catalog the Lakeflow Job writes snapshots instead.

### Lakeflow Job: `sap_purchasing_lakeflow_pipeline`

```
simulate_raw_sap ─┬─ gold_material ─┬─ gold_sourcing ───────┐
                  └─ gold_supplier ─┼─ gold_supplier_risk ──┼─ constraints_comments ─ data_quality_gate ─ dq_passed? ─(true)─┐
                                    └─ gold_purchase_order ─┘                                                                │
            ┌───────────────────────────────┬────────────────────────────────────────────────────────────────────────────────┤
     sql_functions                    metric_views                                                                   value_dictionaries
            └───────────────────────────────┴────────────────── sync_lakebase_worklist ───────────────────────────────────────┘
```

Serverless, parameterized (`catalog`, `schema`, `warehouse_id`), with a data-quality gate
(`08b`, results in `_DQ_RESULTS`) and a condition task that skips the refresh when a check
fails. A daily 06:00 schedule is created **paused**.

### Gold tables (grains)
| Table | Grain | Built by |
|-------|-------|----------|
| `MATERIAL` | MATNR + WERKS | Workflow A (MARC ⨝ MARA ⨝ MAKT[EN]) |
| `SUPPLIER` | LIFNR + EKORG | Workflow B (LFM1 ⨝ LFA1; LFB1 dropped) |
| `SOURCING` | MATNR + WERKS + LIFNR | Workflow C (EORD ⨝ flattened EQUK/EQUP) |
| `SUPPLIER_RISK` | LIFNR, time-sliced | Workflow D (RAW_SUPPLIER_RISK, RI-validated) |
| `PURCHASE_ORDER` | EBELN + EBELP | Workflow E (EKPO ⨝ EKKO ⨝ earliest GR from EKBE) |

Constraints: PK on each gold table; FK `SOURCING → MATERIAL`. Every table & column has a
`COMMENT`.

---

## Repository layout

```
sap-purchasing-genie-demo/
  00_config.py                      # single source of truth (catalog/schema/warehouse/model/names)
  00_run_all.py                     # TOP-LEVEL orchestrator — parameterize ONCE here
  _helper/
    01_simulate_raw_tables.py       # generate all 13 RAW_* tables (deterministic seed)
  02_build_gold/
    02a_material.py … 02e_purchase_order.py   # Workflows A–E
    02f_constraints_comments.py     # PK/FK + column/table comments
  03_semantics/
    03a_sql_functions.py            # parameterized trusted SQL functions
    03b_metric_views.py             # UC metric views
    03c_value_dictionaries.py       # value dictionary
  04_resources/
    04a_resources.py                # warehouse + permissions (Python SDK)
  05_genie/
    05a_create_genie_spaces.py      # create 2 Genie Agents + all instruction types
    genie_instructions_space1.md    # instructions / ontology snippets (with "use first" recommendations)
    genie_instructions_space2.md
  06_agent/
    06a_deploy_supervisor.py        # bind both Genie Agents as tools + `agentbricks deploy`
    _agentbricks_helpers.py         # installs the databricks CLI + Agent Bricks CLI on serverless
    supervisor/                     # the Agent Bricks CLI project (LangGraph agent, agent.toml, app.yaml)
      agent/agent.py                # routing system prompt + graph
      eval/ask.py                   # in-process batch runner used by 07a
  07_benchmark/
    07a_benchmark.py                # 17 questions vs known-good SQL → accuracy + routing score
    benchmark_questions.yaml
  08_jobs/
    08a_create_jobs.py              # multi-task Lakeflow Job (raw → gold → DQ gate → semantics → Lakebase)
    08b_data_quality_gate.py        # DQ checks on the gold layer; task value for the condition task
  09_domains/
    09a_create_domains.ipynb        # governed tags + Discover domains/subdomains + asset tagging
    09b_create_pages.ipynb          # governed Pages (business glossary) — API or manual
  10_lakebase/
    10a_create_lakebase.py          # Lakebase project, case tables, seed data, UC catalog, case functions
    10b_sync_worklist.py            # RISK_WORKLIST → Lakebase synced table; cases → MITIGATION_CASE_HISTORY
    data/buyers.csv                 # seed dataset: 6 buyers (one per material group + lead buyer)
    data/action_types.csv           # seed dataset: 6 mitigation actions with typical lead times
  app/                              # Databricks App (FastAPI + static frontend; backend/cases.py = Lakebase)
  99_teardown.py                    # DELETE every asset (apps, jobs, supervisor, Genie, Lakebase, domains, tags, schemas)
  demo_script.html                  # comprehensive demo walkthrough
```

---

## How to run (blank workspace)

**Prereqs:** a Unity Catalog you can write to, a serverless SQL warehouse, permission to
create Genie Agents + Databricks Apps (the supervisor runs as an app), and serverless
compute with internet access (06a installs `databricks-agentbricks` from PyPI and the
`databricks` CLI from its GitHub release).

1. **Import** this folder into your workspace (Repos/Git folder, or
   `databricks workspace import-dir . /Workspace/Users/<you>/sap-purchasing-genie-demo`).
2. **Run `00_run_all`** on **serverless** compute — set the `CATALOG` / `SCHEMA` /
   `WAREHOUSE_ID` Python variables **once** at the top, then Run All. It orchestrates
   everything in order: raw simulation → gold Workflows A–E → constraints/comments →
   SQL functions → metric views → value dictionary → warehouse & permissions →
   Lakebase (10a + 10b) → 2 Genie Agents → supervisor agent (Agent Bricks CLI) → benchmark →
   Databricks App → Lakeflow Job (created and run once) → governed tags + Discover domains.

Each child notebook can still be run individually (it reads the same parameters), but
`00_run_all` is the single-command path for a full deployment.

**Governed Pages** (business glossary) are created by `09b_create_pages` — the Pages REST
API may not be available on all workspaces. If the API is not exposed, `09b` provides
manual creation instructions (Discover UI + Genie Code prompts). The page prompts are also
inlined in `00_run_all` for easy reference.

**Demo reset (Lakebase):** re-running `10_lakebase/10a_create_lakebase` alone resets the
mitigation cases to the 3 seed cases (10 risks without a case).

### Teardown
Run `99_teardown.py` (top level, serverless) to delete **everything** the demo creates:
the App, the Lakeflow Job (and the legacy data-prep job), the supervisor agent with its
Runtime Store and tracing experiment (plus a legacy MAS tile, if one is left), both
Genie Agents, the Lakebase synced table, the UC catalog over Lakebase and the Lakebase
project (purged), and the schemas `<schema>` and `<schema>_sync` (`DROP SCHEMA … CASCADE`,
which removes all gold/raw tables, metric views, SQL functions and the `_*` metadata tables). Set `DELETE_DISCOVER = True` (the default) to also
delete governed Pages, Discover domains, and governed tag policies; set it to `False` to keep
them. The **catalog** and the **SQL warehouse** are left in place (neither is created by the
demo). Remove the workspace source folders separately via the Workspace UI or
`databricks workspace delete … --recursive`.

### Parameterization
`00_config.py` defines defaults and reads widgets. `00_run_all.py` sets `catalog` / `schema` /
`warehouse_id` as plain **Python variables once** (`CATALOG = …`, `SCHEMA = …`) and forwards
those variables to every child notebook when it runs them. The **catalog is taken as-is**;
the **schema is created**.

---

## Default configuration (this workspace)

| Setting | Value |
|---------|-------|
| Catalog | `max_kohler_demo_catalog` (existing) |
| Schema | `purchasing_demo` (created) |
| Warehouse | Serverless SQL Warehouse (`d7a3d1cfbba73c0a`) |
| LLM (supervisor) | `system.ai.claude-sonnet-4-5` (AI Gateway) |
| Genie Agent 1 | SAP Purchasing — Sourcing & Supplier Risk |
| Genie Agent 2 | SAP Purchasing — Purchase Order Analytics |
| Supervisor agent | `agent-bricks-sap-supervisor` (Databricks App on Agent Runtime) |
| Lakebase project / database | `sap-purchasing-ops` / `purchasing_ops` (schema `ops`) |
| UC catalog over Lakebase | `sap_purchasing_ops` |
| Sync schema (Delta → Lakebase) | `purchasing_demo_sync` |
| Lakeflow Job | `sap_purchasing_lakeflow_pipeline` |

---

## Verification checklist

- `00_run_all` completes → full deployment (data, semantics, Genie, supervisor, benchmark, Job, App).
- All 11 example questions return correct rows via the trusted SQL functions.
- Both Genie Agents answer sample questions (and call the certified functions).
- The supervisor routes each question to the correct Genie Agent (`routing` in 07a).
- The benchmark prints a high accuracy score (target ≥ 90%), with exact and lenient
  passes reported separately.
- The App loads: charts render; chat returns supervisor answers with the Genie Agent used
  and the SQL Genie ran.
- The **Mitigation cases** tab lists the 13-row worklist; creating a case drops "risks without
  an active case" from 10 to 9, and Genie Agent 1 answers the closed-loop question with 9 rows.
- The Lakeflow Job run is green, including `data_quality_gate` and `sync_lakebase_worklist`.
