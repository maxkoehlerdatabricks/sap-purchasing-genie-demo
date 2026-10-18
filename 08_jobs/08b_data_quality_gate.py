# Databricks notebook source
# MAGIC %md
# MAGIC # 08b — Data-quality gate for the gold layer
# MAGIC
# MAGIC A task in the Lakeflow Job (`08a`). Runs after the gold tables are built and
# MAGIC decides whether the semantic layer and the Lakebase sync may be refreshed:
# MAGIC
# MAGIC * every gold table has rows,
# MAGIC * primary keys are unique and not NULL,
# MAGIC * every SOURCING row points to an existing MATERIAL (MATNR + WERKS),
# MAGIC * every PURCHASE_ORDER vendor exists in SUPPLIER.
# MAGIC
# MAGIC Results are appended to `_DQ_RESULTS` and published as the task value
# MAGIC `dq_passed` (`"true"` / `"false"`). The job's condition task reads it, so a failed
# MAGIC check **skips** the downstream refresh instead of publishing bad numbers.

# COMMAND ----------

# MAGIC %run ../00_config

# COMMAND ----------

from datetime import datetime, timezone

from pyspark.sql import Row

NS = CFG.namespace

PKS = {
    "MATERIAL": ["MATNR", "WERKS"],
    "SUPPLIER": ["LIFNR", "EKORG"],
    "SOURCING": ["MATNR", "WERKS", "LIFNR"],
    "PURCHASE_ORDER": ["EBELN", "EBELP"],
}

checks = []  # (check, table, failing_rows)

for t in GOLD_TABLES:
    n = spark.table(f"{NS}.{t}").count()
    checks.append(("has_rows", t, 0 if n > 0 else 1))

for t, keys in PKS.items():
    k = ", ".join(keys)
    nulls = " OR ".join(f"{c} IS NULL" for c in keys)
    dup = spark.sql(f"SELECT count(*) AS n FROM (SELECT {k} FROM {NS}.{t} GROUP BY {k} HAVING count(*) > 1)").first()["n"]
    nul = spark.sql(f"SELECT count(*) AS n FROM {NS}.{t} WHERE {nulls}").first()["n"]
    checks.append(("pk_unique", t, dup))
    checks.append(("pk_not_null", t, nul))

orphan_src = spark.sql(f"""SELECT count(*) AS n FROM {NS}.SOURCING s
    LEFT ANTI JOIN {NS}.MATERIAL m ON s.MATNR = m.MATNR AND s.WERKS = m.WERKS""").first()["n"]
checks.append(("fk_sourcing_material", "SOURCING", orphan_src))
orphan_po = spark.sql(f"""SELECT count(*) AS n FROM {NS}.PURCHASE_ORDER p
    LEFT ANTI JOIN (SELECT DISTINCT LIFNR FROM {NS}.SUPPLIER) s ON p.LIFNR = s.LIFNR""").first()["n"]
checks.append(("fk_po_supplier", "PURCHASE_ORDER", orphan_po))

# COMMAND ----------

ts = datetime.now(timezone.utc)
rows = [Row(CHECKED_AT=ts, CHECK=c, TABLE_NAME=t, FAILING_ROWS=int(f), PASSED=(f == 0)) for c, t, f in checks]
spark.createDataFrame(rows).write.format("delta").mode("append").saveAsTable(f"{NS}._DQ_RESULTS")
display(spark.createDataFrame(rows))

passed = all(r.PASSED for r in rows)
failed = [f"{r.CHECK}:{r.TABLE_NAME}" for r in rows if not r.PASSED]
print(f"Data quality {'PASSED' if passed else 'FAILED'} — {len(rows)} checks, failing: {failed or 'none'}")

# Publish the verdict for the job's condition task (no-op outside a job run).
try:
    dbutils.jobs.taskValues.set(key="dq_passed", value="true" if passed else "false")
except Exception as e:
    print(f"(task value not set outside a job: {e})")

# COMMAND ----------

dbutils.notebook.exit(f"08b_data_quality_gate: {'PASSED' if passed else 'FAILED'} — {len(rows)} checks, failing={failed}")
