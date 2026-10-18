# Databricks notebook source
# MAGIC %md
# MAGIC # 10b — Sync the risk worklist into Lakebase (and snapshot the cases back)
# MAGIC
# MAGIC The bridge between the analytical and the operational world. It is the last task
# MAGIC of the Lakeflow Job (`08a`) and also runs once in `00_run_all`.
# MAGIC
# MAGIC 1. **Lakehouse → Lakebase.** Rebuild the governed Delta table `RISK_WORKLIST` (one
# MAGIC    row per single-sourced material+plant whose sole supplier is at delivery risk
# MAGIC    today) and push it to Lakebase through a **synced table** (`TRIGGERED`,
# MAGIC    incremental via Change Data Feed). The App reads it from Postgres in milliseconds.
# MAGIC 2. **Lakebase → Lakehouse.** Append a snapshot of all mitigation cases (read live via
# MAGIC    the UC catalog over Lakebase) to the Delta table `MITIGATION_CASE_HISTORY`, so
# MAGIC    case progress can be analysed over time.
# MAGIC
# MAGIC > **Pre-requisite:** `10a_create_lakebase` has run once (project, database, catalog).

# COMMAND ----------

# MAGIC %run ../00_config

# COMMAND ----------

import time

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
NS = CFG.namespace
BRANCH = f"projects/{CFG.lakebase_project}/branches/production"
SYNC_NS = f"{CFG.catalog}.{CFG.sync_schema}"
SYNCED_ID = f"{SYNC_NS}.risk_worklist"
CASES = f"{CFG.lakebase_catalog}.{CFG.lakebase_pg_schema}.mitigation_case"


def _do(method, path, body=None, params=None):
    return w.api_client.do(method, path, body=body, query=params)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1) Rebuild the governed `RISK_WORKLIST` Delta table
# MAGIC The table keeps its identity (`INSERT OVERWRITE`, not `CREATE OR REPLACE`) so the
# MAGIC synced table can follow it incrementally through the Change Data Feed.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {NS}.RISK_WORKLIST (
  MATNR STRING NOT NULL COMMENT 'Material number.',
  WERKS STRING NOT NULL COMMENT 'Plant.',
  LIFNR STRING NOT NULL COMMENT 'Sole valid vendor of the material at this plant.',
  MAKTX STRING COMMENT 'Material description (EN).',
  MATKL STRING COMMENT 'Material group (ELEC, MECH, CHEM, ELAST, FAST).',
  SUPPLIER_NAME STRING COMMENT 'Vendor name.',
  SUPPLIER_COUNTRY STRING COMMENT 'Vendor country ISO code.',
  RISK_CATEGORY STRING COMMENT 'Category of the active delivery-risk assessment.',
  RISK_SCORE INT COMMENT 'Risk score 0-100 of the active assessment.',
  RISK_VALID_TO DATE COMMENT 'End of the active risk window.',
  OPEN_PO_LINES INT COMMENT 'PO lines with this vendor for this material not yet received.',
  AS_OF DATE COMMENT 'Reference date the risk was evaluated for.',
  REFRESHED_AT TIMESTAMP COMMENT 'When this worklist was rebuilt.',
  CONSTRAINT risk_worklist_pk PRIMARY KEY (MATNR, WERKS))
COMMENT 'Sourcing-risk worklist: one row per single-sourced material+plant whose sole supplier is at delivery risk today. Synced to Lakebase for the App.'
TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

spark.sql(f"""
INSERT OVERWRITE {NS}.RISK_WORKLIST
SELECT r.MATNR, r.WERKS, r.LIFNR, m.MAKTX, m.MATKL, s.NAME1, s.LAND1,
       k.RISK_CATEGORY, r.RISK_SCORE, k.VALID_TO,
       CAST(COALESCE(po.open_lines, 0) AS INT), current_date(), current_timestamp()
FROM {NS}.single_sourced_risky_today() r
JOIN {NS}.MATERIAL m ON m.MATNR = r.MATNR AND m.WERKS = r.WERKS
LEFT JOIN (SELECT LIFNR, MAX(NAME1) AS NAME1, MAX(LAND1) AS LAND1
           FROM {NS}.SUPPLIER GROUP BY LIFNR) s ON s.LIFNR = r.LIFNR
LEFT JOIN (SELECT LIFNR, MAX_BY(RISK_CATEGORY, RISK_SCORE) AS RISK_CATEGORY, MAX(VALID_TO) AS VALID_TO
           FROM {NS}.SUPPLIER_RISK
           WHERE DELIVERY_RISK_FLAG = 'X' AND current_date() BETWEEN VALID_FROM AND VALID_TO
           GROUP BY LIFNR) k ON k.LIFNR = r.LIFNR
LEFT JOIN (SELECT MATNR, LIFNR, COUNT(*) AS open_lines
           FROM {NS}.PURCHASE_ORDER WHERE GR_DATE IS NULL GROUP BY MATNR, LIFNR) po
       ON po.MATNR = r.MATNR AND po.LIFNR = r.LIFNR
""")
n_worklist = spark.table(f"{NS}.RISK_WORKLIST").count()
print(f"  ✓ RISK_WORKLIST rebuilt: {n_worklist} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2) Push it to Lakebase through a synced table
# MAGIC First run: create the synced table (initial snapshot). Later runs: trigger an
# MAGIC incremental update of its pipeline and wait for it to complete.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SYNC_NS} COMMENT 'Synced tables Delta -> Lakebase for the SAP purchasing App.'")


def get_synced():
    try:
        return _do("GET", f"/api/2.0/postgres/synced_tables/{SYNCED_ID}")
    except Exception:
        return None


st = get_synced()
# A synced table pointing at another project (or a failed one) can never come online
# again, so recreate it. (GET returns the project under status, not the spec.)
if st and (st.get("status", {}).get("project") != f"projects/{CFG.lakebase_project}"
           or "FAILED" in st.get("status", {}).get("detailed_state", "")):
    print(f"  · removing stale synced table ({st.get('status', {}).get('detailed_state')})")
    _do("DELETE", f"/api/2.0/postgres/synced_tables/{SYNCED_ID}")
    time.sleep(10)
    st = None

if st is None:
    _do("POST", "/api/2.0/postgres/synced_tables", params={"synced_table_id": SYNCED_ID}, body={"spec": {
        "source_table_full_name": f"{NS}.RISK_WORKLIST",
        "branch": BRANCH,
        "primary_key_columns": ["MATNR", "WERKS"],
        "scheduling_policy": "TRIGGERED",
        "postgres_database": CFG.lakebase_database,
        "create_database_objects_if_missing": True,
    }})
    print(f"  ✓ synced table {SYNCED_ID} created (initial snapshot running)")
    for attempt in range(60):
        state = (get_synced() or {}).get("status", {}).get("detailed_state", "")
        if "ONLINE" in state:
            break
        if "FAILED" in state:
            raise RuntimeError(f"Synced table failed: {state}")
        print(f"  … {state}")
        time.sleep(15)
    else:
        raise TimeoutError("Synced table did not come online in 15 minutes.")
else:
    pipeline_id = st["status"]["pipeline_id"]
    update_id = w.pipelines.start_update(pipeline_id=pipeline_id).update_id
    print(f"  ✓ triggered incremental sync (pipeline {pipeline_id}, update {update_id})")
    for attempt in range(60):
        state = str(w.pipelines.get_update(pipeline_id=pipeline_id, update_id=update_id).update.state).split(".")[-1]
        if state in ("COMPLETED", "FAILED", "CANCELED"):
            break
        time.sleep(15)
    if state != "COMPLETED":
        raise RuntimeError(f"Synced table update ended in state {state}")

state = get_synced()["status"]["detailed_state"]
print(f"  ✓ synced table state: {state}")
print(f"    Postgres: {CFG.lakebase_database}.{CFG.sync_schema}.risk_worklist")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3) Snapshot the live cases back into the Lakehouse
# MAGIC Read through the UC catalog over Lakebase (no extract job, no credentials in code)
# MAGIC and append to `MITIGATION_CASE_HISTORY`. Each run adds one snapshot.

# COMMAND ----------

from pyspark.sql import functions as F

cases = spark.table(CASES).withColumn("SNAPSHOT_TS", F.current_timestamp())
cases.write.format("delta").mode("append").option("mergeSchema", "true") \
    .saveAsTable(f"{NS}.MITIGATION_CASE_HISTORY")
spark.sql(f"COMMENT ON TABLE {NS}.MITIGATION_CASE_HISTORY IS "
          "'Snapshots of the Lakebase mitigation cases. One row per case per snapshot (SNAPSHOT_TS).'")
n_cases = cases.count()
n_snap = spark.table(f"{NS}.MITIGATION_CASE_HISTORY").select("SNAPSHOT_TS").distinct().count()
print(f"  ✓ appended {n_cases} cases to MITIGATION_CASE_HISTORY ({n_snap} snapshots so far)")

# COMMAND ----------

dbutils.notebook.exit(f"10b_sync_worklist: SUCCESS — worklist={n_worklist} rows ({state}), cases snapshot={n_cases}")
