# Databricks notebook source
# MAGIC %md
# MAGIC # 02d — Workflow D → SUPPLIER_RISK with a Lakeflow Declarative Pipeline
# MAGIC
# MAGIC Enrichment layer joined at query time to flag delivery risk. Time-sliced so a PO
# MAGIC placed on a given date can be evaluated against the risk window that was active
# MAGIC *at that date* (drives Q8).
# MAGIC
# MAGIC Unlike the other gold tables, this slice is **ingested from files**: SAP Ariba
# MAGIC Supplier Risk exports land as JSON in the UC volume `ariba_landing` (written by
# MAGIC `_helper/01_simulate_raw_tables`). The pipeline `02d_supplier_risk_pipeline.sql`:
# MAGIC
# MAGIC ```
# MAGIC /Volumes/<catalog>/<schema>/ariba_landing/supplier_risk/*.json
# MAGIC     │  Auto Loader (read_files, incremental)
# MAGIC     ▼
# MAGIC RAW_SUPPLIER_RISK   streaming table · Ariba fields → SAP-style columns · lineage columns
# MAGIC     │  latest snapshot · RI join to SUPPLIER · expectations · PK (LIFNR, VALID_FROM)
# MAGIC     ▼
# MAGIC SUPPLIER_RISK       materialized view (gold)
# MAGIC ```
# MAGIC
# MAGIC **Steps (spec §4, Workflow D):**
# MAGIC 1. Take the raw Ariba risk records (Auto Loader → `RAW_SUPPLIER_RISK`).
# MAGIC 2. Validate LIFNR values exist in Workflow B's SUPPLIER output (referential
# MAGIC    integrity) — we keep only vendors that exist in SUPPLIER.
# MAGIC 3. No merging — this table stands alone as an enrichment layer.
# MAGIC
# MAGIC This notebook creates (or updates) the pipeline, runs one update and prints the
# MAGIC evidence: update state, row counts, expectation results and the primary key. In the
# MAGIC Lakeflow Job the same pipeline runs as the `gold_supplier_risk` pipeline task.

# COMMAND ----------

# MAGIC %run ../00_config

# COMMAND ----------

import time
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
pipeline_id = ensure_supplier_risk_pipeline()
print(f"Pipeline UI: {w.config.host}/pipelines/{pipeline_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run one pipeline update and wait

# COMMAND ----------

update_id = w.pipelines.start_update(pipeline_id=pipeline_id).update_id
print(f"Started update {update_id}")
while True:
    state = w.pipelines.get_update(pipeline_id=pipeline_id, update_id=update_id).update.state.value
    if state in ("COMPLETED", "FAILED", "CANCELED"):
        break
    time.sleep(15)
print(f"Pipeline update {update_id}: {state}")
if state != "COMPLETED":
    errors = [e.message for e in w.pipelines.list_pipeline_events(pipeline_id=pipeline_id, max_results=50)
              if e.level and e.level.value == "ERROR"]
    raise RuntimeError(f"Pipeline update {state}: {errors[:3]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evidence: what the pipeline produced

# COMMAND ----------

NS = CFG.namespace
files = dbutils.fs.ls(CFG.ariba_landing_path)
raw = spark.table(CFG.fq("RAW_SUPPLIER_RISK"))
latest = raw.agg({"EXPORT_DATE": "max"}).first()[0]
raw_latest = raw.filter(f"EXPORT_DATE = '{latest}'").count()
cnt = spark.table(CFG.fq("SUPPLIER_RISK")).count()
active = (spark.table(CFG.fq("SUPPLIER_RISK"))
          .filter("DELIVERY_RISK_FLAG = 'X' AND VALID_FROM <= current_date() "
                  "AND VALID_TO >= current_date()")
          .select("LIFNR").distinct().count())

print(f"Ariba export files in volume:   {len(files)}  {[f.name for f in files]}")
print(f"RAW_SUPPLIER_RISK rows:         {raw.count():,}  (latest export {latest}: {raw_latest})")
print(f"SUPPLIER_RISK rows (gold):      {cnt:,}  | not in gold: {raw_latest - cnt}")
print(f"Vendors with ACTIVE delivery-risk flag today: {active}")

display(spark.sql(f"""SELECT table_name, table_type, comment
                      FROM {CFG.catalog}.information_schema.tables
                      WHERE table_schema = '{CFG.schema}'
                        AND lower(table_name) IN ('raw_supplier_risk', 'supplier_risk')"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Expectations (data quality) from the pipeline event log

# COMMAND ----------

display(spark.sql(f"""
  SELECT e.dataset, e.name AS expectation, e.passed_records, e.failed_records
  FROM (
    SELECT explode(from_json(details:flow_progress.data_quality.expectations,
                   'array<struct<name:string,dataset:string,passed_records:bigint,failed_records:bigint>>')) AS e
    FROM {CFG.fq(CFG.ariba_event_log)}
    WHERE event_type = 'flow_progress' AND origin.update_id = '{update_id}'
      AND details:flow_progress.data_quality IS NOT NULL)
  ORDER BY e.dataset, e.name"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Primary key declared on the materialized view

# COMMAND ----------

display(spark.sql(f"""
  SELECT tc.table_name, tc.constraint_name, tc.constraint_type,
         concat_ws(', ', collect_list(kcu.column_name)) AS columns
  FROM {CFG.catalog}.information_schema.table_constraints tc
  JOIN {CFG.catalog}.information_schema.key_column_usage kcu
    ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
  WHERE tc.table_schema = '{CFG.schema}' AND lower(tc.table_name) = 'supplier_risk'
  GROUP BY ALL"""))

display(spark.table(CFG.fq("SUPPLIER_RISK")).orderBy("LIFNR", "VALID_FROM").limit(10))

# COMMAND ----------

dbutils.notebook.exit(f"02d_supplier_risk: SUCCESS — pipeline {pipeline_id} update {update_id} "
                      f"COMPLETED, {cnt} gold rows.")
