# Databricks notebook source
# MAGIC %md
# MAGIC # 02b — Workflow B → SUPPLIER (grain: LIFNR + EKORG)
# MAGIC
# MAGIC Consolidates vendor master into one row per vendor + purchasing org.
# MAGIC
# MAGIC **Steps (spec §4, Workflow B):**
# MAGIC 1. Start from `RAW_LFM1` (already at LIFNR+EKORG grain; carries SPERM block flag).
# MAGIC 2. Left-join `RAW_LFA1` on LIFNR, broadcasting NAME1/LAND1 to every EKORG row.
# MAGIC 3. **Drop `RAW_LFB1`** (company-code grain) — not needed for purchasing questions.
# MAGIC 4. Output: one row per LIFNR+EKORG with NAME1, LAND1, SPERM.

# COMMAND ----------

# MAGIC %run ../00_config

# COMMAND ----------

lfm1 = spark.table(CFG.fq("RAW_LFM1"))   # LIFNR, EKORG, SPERM  (target grain)
lfa1 = spark.table(CFG.fq("RAW_LFA1"))   # LIFNR, NAME1, LAND1

supplier = (
    lfm1
    .join(lfa1, on="LIFNR", how="left")   # broadcast NAME1/LAND1 to every EKORG row
    .select("LIFNR", "EKORG", "NAME1", "LAND1", "SPERM")
)
# Note: RAW_LFB1 (company-code grain) is intentionally NOT joined (spec).

(supplier.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(CFG.fq("SUPPLIER")))

cnt = spark.table(CFG.fq("SUPPLIER")).count()
dupes = (spark.table(CFG.fq("SUPPLIER"))
         .groupBy("LIFNR", "EKORG").count().filter("count > 1").count())
print(f"SUPPLIER rows: {cnt:,} | grain violations (LIFNR+EKORG): {dupes}")
assert dupes == 0, "SUPPLIER grain violated — LIFNR+EKORG not unique!"
display(spark.table(CFG.fq("SUPPLIER")).limit(10))

# COMMAND ----------

dbutils.notebook.exit(f"02b_supplier: SUCCESS — {cnt} rows.")
