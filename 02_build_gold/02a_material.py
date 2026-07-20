# Databricks notebook source
# MAGIC %md
# MAGIC # 02a — Workflow A → MATERIAL (grain: MATNR + WERKS)
# MAGIC
# MAGIC Consolidates material master data into one row per material + plant.
# MAGIC
# MAGIC **Steps (spec §4, Workflow A):**
# MAGIC 1. Start from `RAW_MARC` (already at MATNR+WERKS grain).
# MAGIC 2. Left-join `RAW_MARA` on MATNR, broadcasting MTART/MATKL to every plant row.
# MAGIC 3. Filter `RAW_MAKT` to a single language (EN) **before** joining on MATNR, to
# MAGIC    avoid duplicating rows across languages.
# MAGIC 4. Output: one row per MATNR+WERKS with MAKTX, MTART, MATKL, DISPO, SOBSL.

# COMMAND ----------

# MAGIC %run ../00_config

# COMMAND ----------

from pyspark.sql import functions as F

marc = spark.table(CFG.fq("RAW_MARC"))   # MATNR, WERKS, DISPO, SOBSL  (target grain)
mara = spark.table(CFG.fq("RAW_MARA"))   # MATNR, MTART, MATKL
makt = spark.table(CFG.fq("RAW_MAKT"))   # MATNR, SPRAS, MAKTX

# Step 3: filter descriptions to English FIRST so the join stays 1:1 on MATNR.
makt_en = makt.filter(F.col("SPRAS") == F.lit("EN")).select("MATNR", "MAKTX")

material = (
    marc
    .join(mara, on="MATNR", how="left")       # broadcast MTART/MATKL to every plant row
    .join(makt_en, on="MATNR", how="left")    # attach EN description
    .select(
        "MATNR", "WERKS", "MAKTX", "MTART", "MATKL", "DISPO", "SOBSL",
    )
)

(material.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(CFG.fq("MATERIAL")))

cnt = spark.table(CFG.fq("MATERIAL")).count()
# Grain assertion: MATNR+WERKS must be unique.
dupes = (spark.table(CFG.fq("MATERIAL"))
         .groupBy("MATNR", "WERKS").count().filter("count > 1").count())
print(f"MATERIAL rows: {cnt:,} | grain violations (MATNR+WERKS): {dupes}")
assert dupes == 0, "MATERIAL grain violated — MATNR+WERKS not unique!"
display(spark.table(CFG.fq("MATERIAL")).limit(10))

# COMMAND ----------

dbutils.notebook.exit(f"02a_material: SUCCESS — {cnt} rows.")
