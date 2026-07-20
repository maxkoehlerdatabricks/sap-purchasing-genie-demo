# Databricks notebook source
# MAGIC %md
# MAGIC # 02c — Workflow C → SOURCING (grain: MATNR + WERKS + LIFNR)
# MAGIC
# MAGIC The heart of the sourcing-risk model: which vendors may supply which material at
# MAGIC which plant, plus their quota split.
# MAGIC
# MAGIC **Steps (spec §4, Workflow C):**
# MAGIC 1. Start from `RAW_EORD` (already at the target grain MATNR+WERKS+LIFNR).
# MAGIC 2. Join `RAW_EQUK` to `RAW_EQUP` on EQUK_ID to flatten quota to MATNR+WERKS+LIFNR.
# MAGIC 3. Left-join the flattened quota onto `RAW_EORD` using MATNR+WERKS+LIFNR.
# MAGIC 4. **Allow QUOTA to be NULL** where a vendor is source-listed but not quota-split —
# MAGIC    a deliberate, meaningful gap (NOT a data error).
# MAGIC 5. Output: one row per MATNR+WERKS+LIFNR with VDATU, BDATU, NOTKZ, FLIFN, QUOTA.
# MAGIC
# MAGIC We also derive a convenience boolean `IS_VALID_SOURCE` (non-blocked source-list
# MAGIC entry) — this is what "valid, non-blocked vendor" means in the business questions.

# COMMAND ----------

# MAGIC %run ../00_config

# COMMAND ----------

from pyspark.sql import functions as F

eord = spark.table(CFG.fq("RAW_EORD"))   # MATNR, WERKS, LIFNR, VDATU, BDATU, NOTKZ, FLIFN
equk = spark.table(CFG.fq("RAW_EQUK"))   # MATNR, WERKS, EQUK_ID
equp = spark.table(CFG.fq("RAW_EQUP"))   # EQUK_ID, LIFNR, QUOTA

# Step 2: flatten quota to MATNR+WERKS+LIFNR grain via the EQUK_ID bridge.
quota_flat = (
    equk.join(equp, on="EQUK_ID", how="inner")
        .select("MATNR", "WERKS", "LIFNR", "QUOTA")
)

# Step 3+4: left-join quota; QUOTA stays NULL for source-listed-but-not-quota vendors.
sourcing = (
    eord
    .join(quota_flat, on=["MATNR", "WERKS", "LIFNR"], how="left")
    .withColumn(
        # A source is "valid & non-blocked" when NOTKZ is not 'X'.
        "IS_VALID_SOURCE",
        F.when(F.coalesce(F.col("NOTKZ"), F.lit("")) != F.lit("X"), F.lit(True))
         .otherwise(F.lit(False)),
    )
    .select(
        "MATNR", "WERKS", "LIFNR", "VDATU", "BDATU", "NOTKZ", "FLIFN",
        "QUOTA", "IS_VALID_SOURCE",
    )
)

(sourcing.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(CFG.fq("SOURCING")))

cnt = spark.table(CFG.fq("SOURCING")).count()
dupes = (spark.table(CFG.fq("SOURCING"))
         .groupBy("MATNR", "WERKS", "LIFNR").count().filter("count > 1").count())
nullq = spark.table(CFG.fq("SOURCING")).filter("QUOTA IS NULL").count()
print(f"SOURCING rows: {cnt:,} | grain violations: {dupes} | NULL-quota (intended gaps): {nullq:,}")
assert dupes == 0, "SOURCING grain violated — MATNR+WERKS+LIFNR not unique!"
display(spark.table(CFG.fq("SOURCING")).limit(10))

# COMMAND ----------

dbutils.notebook.exit(f"02c_sourcing: SUCCESS — {cnt} rows.")
