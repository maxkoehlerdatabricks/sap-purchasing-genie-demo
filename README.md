# SAP Purchasing — Sourcing Risk & PO Analytics (Databricks Demo)

A **demo-ready**, reproducible Databricks demo that simulates an automotive OEM's SAP
purchasing landscape so a purchasing business analyst can answer **sourcing-risk** and
**supply-continuity** questions *without SAP access*.

It builds, end-to-end and on a blank workspace:

- **13 simulated raw SAP tables** → **5 governed gold tables** (with PK/FK constraints,
  table & column comments).
- A **semantic layer**: parameterized **SQL functions** (trusted assets), **UC metric
  views**, and a **value dictionary**.
- **2 Genie spaces** (domain-split) with **all instruction types** (ontology snippets)
  + certified answers.
- An **Agent Bricks Multi-Agent Supervisor (MAS)** over the two spaces, deployed as a
  serving endpoint.
- **Governed tags**, **Discover domains/subdomains**, and **5 governed Pages** (business
  glossary) — the ontology layer that Genie One reads.
- A **benchmark** of representative questions with known-good SQL and an **accuracy
  score**.
- A **Databricks App** (React + FastAPI) with visualizations + a chat interface over
  the MAS.

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
                    Genie Space 1                              Genie Space 2
              Sourcing & Supplier Risk                    Purchase Order Analytics
                          └───────────────────┬────────────────────┘
                                    Agent Bricks MAS (serving endpoint)
                                               │
                                       Databricks App
                                 (Visualizations + Chat)
```

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
    05a_create_genie_spaces.py      # create 2 Genie spaces + all instruction types
    genie_instructions_space1.md    # instructions / ontology snippets (with "use first" recommendations)
    genie_instructions_space2.md
  06_agent/
    06a_create_mas.py               # Agent Bricks MAS + serving endpoint
  07_benchmark/
    07a_benchmark.py                # runs 16 questions vs known-good SQL → accuracy score
    benchmark_questions.yaml
  08_jobs/
    08a_create_jobs.py              # serverless data-prep Job (Python SDK)
  09_domains/
    09a_create_domains.py           # governed tags + Discover domains/subdomains + asset tagging
    09b_create_pages.py             # governed Pages (business glossary) — API or manual
  app/                              # Databricks App (React + FastAPI)
  99_teardown.py                    # DELETE every asset (app, job, MAS, Genie, domains, tags, schema)
  demo_script.html                  # comprehensive demo walkthrough
```

---

## How to run (blank workspace)

**Prereqs:** a Unity Catalog you can write to, a serverless SQL warehouse, permission to
create Genie spaces + Agent Bricks + serving endpoints + Apps.

1. **Import** this folder into your workspace (Repos/Git folder, or
   `databricks workspace import-dir . /Workspace/Users/<you>/sap-purchasing-genie-demo`).
2. **Run `00_run_all`** on **serverless** compute — set the `CATALOG` / `SCHEMA` /
   `WAREHOUSE_ID` Python variables **once** at the top, then Run All. It orchestrates
   everything in order: raw simulation → gold Workflows A–E → constraints/comments →
   SQL functions → metric views → value dictionary → warehouse & permissions → 2 Genie
   spaces → MAS + serving endpoint → governed tags + Discover domains →
    benchmark → serverless Job → Databricks App.

Each child notebook can still be run individually (it reads the same parameters), but
`00_run_all` is the single-command path for a full deployment.

**Governed Pages** (business glossary) are created by `09b_create_pages` — the Pages REST
API may not be available on all workspaces. If the API is not exposed, `09b` provides
manual creation instructions (Discover UI + Genie Code prompts). The page prompts are also
inlined in `00_run_all` for easy reference.

### Teardown
Run `99_teardown.py` (top level, serverless) to delete **everything** the demo creates —
the App, the data-prep Job, the MAS + serving endpoint, both Genie spaces, and the entire
schema (`DROP SCHEMA … CASCADE`, which removes all gold/raw tables, metric views, SQL
functions and the `_*` metadata tables). Set `DELETE_DISCOVER = True` (the default) to also
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
| LLM | `databricks-claude-sonnet-4-5` |
| Genie Space 1 | SAP Purchasing — Sourcing & Supplier Risk |
| Genie Space 2 | SAP Purchasing — Purchase Order Analytics |
| MAS endpoint | `sap-purchasing-mas` |

---

## Verification checklist

- `00_run_all` completes → full deployment (data, semantics, Genie, MAS, benchmark, Job, App).
- All 11 example questions return correct rows via the trusted SQL functions.
- Both Genie spaces answer sample questions (and call the certified functions).
- The MAS endpoint routes a question to the correct Genie space.
- The benchmark prints a high accuracy score (target ≥ 90%).
- The App loads: charts render; chat returns MAS answers.
