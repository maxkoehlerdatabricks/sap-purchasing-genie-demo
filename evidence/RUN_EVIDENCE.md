# Run evidence: end-to-end run on 19 October 2026

Every number below comes from the executed notebooks in [`notebooks/`](notebooks/). They
were exported with their outputs from the Databricks job runs using
`_helper/export_run_evidence.py`.

| | |
|---|---|
| Workspace | `fe-sandbox-serverless-sandbox-gs5ou3` (AWS), serverless compute |
| Code | branch `febar` of this repo, run from the workspace Git folder |
| End-to-end run | job run `119236254659749`: **20 of 20 tasks SUCCESS** (same order as `00_run_all`) |
| Lakeflow Job run | job `sap_purchasing_lakeflow_pipeline`, run `837085352084892`: **13 of 13 tasks SUCCESS**, including the pipeline task |

## 1. Raw → gold, including the Lakeflow Declarative Pipeline

Raw layer (`notebooks/02_01_simulate_raw_tables.ipynb`):

| Raw table | Rows | How it is created |
|---|---:|---|
| RAW_MARA / RAW_MAKT / RAW_MARC | 60 / 90 / 117 | simulated Delta table |
| RAW_LFA1 / RAW_LFB1 / RAW_LFM1 | 25 / 33 / 42 | simulated Delta table |
| RAW_EORD / RAW_EQUK / RAW_EQUP | 220 / 39 / 107 | simulated Delta table |
| RAW_EKKO / RAW_EKPO / RAW_EKBE | 400 / 711 / 1,422 | simulated Delta table |
| **RAW_SUPPLIER_RISK** | **34** | **SAP Ariba JSON export in a UC volume → Auto Loader streaming table** |

Lakeflow Declarative Pipeline `sap_purchasing_ariba_supplier_risk_purchasing_demo`
(`notebooks/06_02d_supplier_risk.ipynb`):

* Update `59ae4344-…` **COMPLETED**. It also ran as the `gold_supplier_risk` pipeline task
  in the Lakeflow Job (`notebooks/lakeflow_job/`).
* Landing file `supplier_risk_export_20261019.json`: 34 records, 32 valid and 2
  deliberately bad.
* `RAW_SUPPLIER_RISK` (STREAMING_TABLE): 34 rows. `SUPPLIER_RISK` (MATERIALIZED_VIEW):
  **32 rows**, so 2 records were kept out of gold.
* Expectations from the event log:

  | Dataset | Expectation | Passed | Failed |
  |---|---|---:|---:|
  | supplier_risk | valid_score | 32 | 1 |
  | supplier_risk | valid_window | 33 | 0 |
  | raw_supplier_risk | has_vendor_id | 34 | 0 (first update; later updates find no new file, so they ingest 0 rows) |

  The unknown vendor `V999999` is removed by the referential-integrity join to `SUPPLIER`.
* Primary key on the materialized view: `pk_risk PRIMARY KEY (LIFNR, VALID_FROM)`.

Gold layer:

| Gold table | Rows | Primary key | Built by |
|---|---:|---|---|
| MATERIAL | 117 | pk_material (MATNR, WERKS) | notebook 02a |
| SUPPLIER | 42 | pk_supplier (LIFNR, EKORG) | notebook 02b |
| SOURCING | 220 | pk_sourcing (MATNR, WERKS, LIFNR) + FK fk_sourcing_material → MATERIAL | notebook 02c |
| SUPPLIER_RISK | 32 | pk_risk (LIFNR, VALID_FROM) | **Lakeflow pipeline** |
| PURCHASE_ORDER | 711 | pk_po (EBELN, EBELP) | notebook 02e |

The data-quality gate passed all 15 checks (`notebooks/lakeflow_job/08_data_quality_gate.ipynb`).

## 2. Genie spaces and supervisor agent coming online

* Genie space 1 "SAP Purchasing — Sourcing & Supplier Risk": `01f1cbe0a7e616b185a50f44a22099be`
* Genie space 2 "SAP Purchasing — Purchase Order Analytics": `01f1cbe0aa081ca0b7f81ab0a72fb650`
  (`notebooks/15_05a_create_genie_spaces.ipynb`)
* Agent Bricks supervisor `sap_purchasing_supervisor`, serving endpoint
  `mas-5c65408d-endpoint`: state **READY** (`notebooks/16_06a_create_mas.ipynb`). The smoke
  test routed a question to `Sourcing_Supplier_Risk` and answered "45 material-plant
  combinations lack a multi-sourcing strategy". That matches the known-good SQL (Q1 = 45).

## 3. Benchmark and a real answer

`notebooks/17_07a_benchmark.ipynb`: 17 questions, each compared with known-good SQL.

* **17 of 17 accepted.** 13 match exactly. 4 (Q2, Q4, Q5, Q8) are accepted by the lenient
  rule "both return rows" but return a different row count. For example, Q5 expects 4
  rows and the agent returned 64. Those 4 still need work.
* Full answer from the supervisor endpoint for Q3, *"Which materials are single-sourced
  AND their sole supplier has a delivery-risk flag?"*: **13 rows, the same as the
  known-good SQL.**

  | MATNR | WERKS | LIFNR | RISK_SCORE |
  |---|---|---|---:|
  | MAT100047 | 1030 | V200013 | 94 |
  | MAT100049 | 1030 | V200013 | 94 |
  | MAT100051 | 1010 | V200013 | 94 |
  | MAT100027 | 1010 | V200020 | 86 |
  | MAT100028 | 1030 | V200013 | 94 |
  | MAT100021 | 1030 | V200022 | 76 |
  | MAT100031 | 1010 | V200020 | 86 |
  | MAT100049 | 1020 | V200017 | 86 |
  | MAT100014 | 1030 | V200020 | 86 |
  | MAT100040 | 1010 | V200006 | 78 |
  | MAT100032 | 1030 | V200017 | 86 |
  | MAT100007 | 1010 | V200020 | 86 |
  | MAT100058 | 1020 | V200020 | 86 |

## 4. Action layer and app

* Lakebase: project `sap-purchasing-ops`, 3 seed cases, 10 unmitigated risks. The
  13-row risk worklist synced (`SYNCED_TABLE_ONLINE_NO_PENDING_UPDATE`).
* Databricks App `sap-purchasing-app` deployed with Lakebase and Genie access
  (`notebooks/18_deploy_app.ipynb`).
* Governed tags and Discover domain created. `SUPPLIER_RISK` was tagged as a materialized
  view (`notebooks/20_09a_create_domains.ipynb`).

## Reproduce

```bash
# run 00_run_all (or the same notebooks as job tasks), then:
python3 _helper/export_run_evidence.py --profile <profile> --run-id <job run id> --out evidence/notebooks
```
