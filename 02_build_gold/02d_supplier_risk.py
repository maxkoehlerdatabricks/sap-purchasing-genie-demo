# Databricks notebook source
# MAGIC %md
# MAGIC # 02d — Workflow D → SUPPLIER_RISK (grain: LIFNR, time-sliced)
# MAGIC
# MAGIC Enrichment layer joined at query time to flag delivery risk. Time-sliced so a PO
# MAGIC placed on a given date can be evaluated against the risk window that was active
# MAGIC *at that date* (drives Q8).
# MAGIC
# MAGIC **Steps (spec §4, Workflow D):**
# MAGIC 1. Take `RAW_SUPPLIER_RISK` as-is (already synthetic & self-contained).
# MAGIC 2. Validate LIFNR values exist in Workflow B's SUPPLIER output (referential
# MAGIC    integrity) — we keep only vendors that exist in SUPPLIER.
# MAGIC 3. No merging — this table stands alone as an enrichment layer.

# COMMAND ----------

# MAGIC %run ../00_config

# COMMAND ----------

from pyspark.sql import functions as F

raw_risk = spark.table(CFG.fq("RAW_SUPPLIER_RISK"))
supplier_lifnr = spark.table(CFG.fq("SUPPLIER")).select("LIFNR").distinct()

# Step 2: referential-integrity validation against SUPPLIER's LIFNR set.
valid_risk = raw_risk.join(supplier_lifnr, on="LIFNR", how="inner")

dropped = raw_risk.count() - valid_risk.count()

supplier_risk = valid_risk.select(
    "LIFNR", "RISK_CATEGORY", "DELIVERY_RISK_FLAG", "RISK_SCORE",
    "VALID_FROM", "VALID_TO",
)

(supplier_risk.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(CFG.fq("SUPPLIER_RISK")))

cnt = spark.table(CFG.fq("SUPPLIER_RISK")).count()
active = (spark.table(CFG.fq("SUPPLIER_RISK"))
          .filter("DELIVERY_RISK_FLAG = 'X' AND VALID_FROM <= current_date() "
                  "AND VALID_TO >= current_date()")
          .select("LIFNR").distinct().count())
print(f"SUPPLIER_RISK rows: {cnt:,} | dropped for RI: {dropped} | "
      f"vendors with ACTIVE delivery-risk flag today: {active}")
display(spark.table(CFG.fq("SUPPLIER_RISK")).orderBy("LIFNR", "VALID_FROM").limit(10))

# COMMAND ----------

dbutils.notebook.exit(f"02d_supplier_risk: SUCCESS — {cnt} rows.")
