# Databricks notebook source
# MAGIC %md
# MAGIC # 02e — Workflow E → PURCHASE_ORDER (grain: EBELN + EBELP)
# MAGIC
# MAGIC Consolidates the PO header/item/history into one analysis-ready PO line table.
# MAGIC
# MAGIC **Steps (spec §4, Workflow E):**
# MAGIC 1. Join `RAW_EKPO` to `RAW_EKKO` on EBELN, pulling LIFNR and EKORG onto the line.
# MAGIC 2. Aggregate `RAW_EKBE` per EBELN+EBELP: take the **earliest goods-receipt**
# MAGIC    EVENT_DATE as GR_DATE (filter VGABE to the goods-receipt event type '1').
# MAGIC 3. Derive `ON_TIME_FLAG` by comparing GR_DATE against the requested delivery date.
# MAGIC 4. Output: one row per EBELN+EBELP with MATNR, LIFNR, EKORG, NETPR, GR_DATE,
# MAGIC    ON_TIME_FLAG (plus ORDER_DATE + REQUESTED_DATE, useful for OTD & Q8).

# COMMAND ----------

# MAGIC %run ../00_config

# COMMAND ----------

from pyspark.sql import functions as F

ekko = spark.table(CFG.fq("RAW_EKKO"))   # EBELN, LIFNR, EKORG, AEDAT
ekpo = spark.table(CFG.fq("RAW_EKPO"))   # EBELN, EBELP, MATNR, NETPR, EINDT
ekbe = spark.table(CFG.fq("RAW_EKBE"))   # EBELN, EBELP, VGABE, BEWTP, EVENT_DATE

VGABE_GOODS_RECEIPT = "1"   # goods-receipt event type

# Step 2: earliest GR date per PO line (filter to goods-receipt events only).
gr = (
    ekbe.filter(F.col("VGABE") == F.lit(VGABE_GOODS_RECEIPT))
        .groupBy("EBELN", "EBELP")
        .agg(F.min("EVENT_DATE").alias("GR_DATE"))
)

# Step 1: item joined to header for LIFNR/EKORG/order date.
po = (
    ekpo.join(ekko, on="EBELN", how="left")
        .join(gr, on=["EBELN", "EBELP"], how="left")
        .withColumnRenamed("AEDAT", "ORDER_DATE")
        .withColumnRenamed("EINDT", "REQUESTED_DATE")
        # Step 3: on-time when GR_DATE <= requested date. NULL GR_DATE = not yet received.
        .withColumn(
            "ON_TIME_FLAG",
            F.when(F.col("GR_DATE").isNull(), F.lit(None).cast("boolean"))
             .when(F.col("GR_DATE") <= F.col("REQUESTED_DATE"), F.lit(True))
             .otherwise(F.lit(False)),
        )
        .select(
            "EBELN", "EBELP", "MATNR", "LIFNR", "EKORG", "NETPR",
            "ORDER_DATE", "REQUESTED_DATE", "GR_DATE", "ON_TIME_FLAG",
        )
)

(po.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(CFG.fq("PURCHASE_ORDER")))

cnt = spark.table(CFG.fq("PURCHASE_ORDER")).count()
dupes = (spark.table(CFG.fq("PURCHASE_ORDER"))
         .groupBy("EBELN", "EBELP").count().filter("count > 1").count())
otd = spark.table(CFG.fq("PURCHASE_ORDER")).filter("ON_TIME_FLAG = true").count()
late = spark.table(CFG.fq("PURCHASE_ORDER")).filter("ON_TIME_FLAG = false").count()
print(f"PURCHASE_ORDER rows: {cnt:,} | grain violations: {dupes} | on-time: {otd:,} | late: {late:,}")
assert dupes == 0, "PURCHASE_ORDER grain violated — EBELN+EBELP not unique!"
display(spark.table(CFG.fq("PURCHASE_ORDER")).limit(10))

# COMMAND ----------

dbutils.notebook.exit(f"02e_purchase_order: SUCCESS — {cnt} rows.")
