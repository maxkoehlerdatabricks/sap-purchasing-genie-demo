# Databricks notebook source
# MAGIC %md
# MAGIC # 03b — UC Metric Views (governed business KPIs)
# MAGIC
# MAGIC Metric views define **reusable, governed** measures separated from dimensions, so
# MAGIC the same KPI (OTD %, price variance, risk exposure, sourcing coverage) is
# MAGIC computed consistently across Genie, dashboards, and the App. Genie queries these
# MAGIC natively.
# MAGIC
# MAGIC We create 4 metric views:
# MAGIC * `mv_sourcing_coverage`  — vendor coverage / multi-sourcing by material & plant.
# MAGIC * `mv_supplier_risk`       — risk exposure by category / flag / country.
# MAGIC * `mv_delivery_performance`— on-time delivery % by supplier / plant / month.
# MAGIC * `mv_price_analytics`     — net price stats by material / vendor.
# MAGIC
# MAGIC > Requires DBR / serverless with Metric Views (YAML v1.1) support.

# COMMAND ----------

# MAGIC %run ../00_config

# COMMAND ----------

NS = CFG.namespace


def run(sql):
    spark.sql(sql)
    print("  ✓ created")


# COMMAND ----------

# MAGIC %md
# MAGIC ## mv_sourcing_coverage — multi-sourcing coverage
# MAGIC Dimensions: plant, material group, material type, valid-source flag.
# MAGIC Measures: sourcing rows, distinct vendors, valid vendors, materials.

# COMMAND ----------

run(f"""
CREATE OR REPLACE VIEW {NS}.mv_sourcing_coverage
WITH METRICS
LANGUAGE YAML
COMMENT 'Sourcing coverage & multi-sourcing KPIs by material, plant, group.'
AS $$
version: 1.1
comment: "Sourcing coverage and multi-sourcing metrics"
source: {NS}.SOURCING
joins:
  - name: mat
    source: {NS}.MATERIAL
    on: source.MATNR = mat.MATNR AND source.WERKS = mat.WERKS
dimensions:
  - name: Plant
    expr: source.WERKS
    comment: "Plant (WERKS)"
  - name: Material
    expr: source.MATNR
  - name: Material Group
    expr: mat.MATKL
  - name: Material Type
    expr: mat.MTART
  - name: Is Valid Source
    expr: source.IS_VALID_SOURCE
measures:
  - name: Sourcing Records
    expr: COUNT(1)
  - name: Distinct Vendors
    expr: COUNT(DISTINCT source.LIFNR)
  - name: Valid Vendors
    expr: COUNT(DISTINCT CASE WHEN source.IS_VALID_SOURCE THEN source.LIFNR END)
  - name: Distinct Materials
    expr: COUNT(DISTINCT source.MATNR)
$$
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## mv_supplier_risk — risk exposure
# MAGIC Dimensions: risk category, delivery-risk flag, vendor country.
# MAGIC Measures: assessments, risky vendors, avg/max risk score.

# COMMAND ----------

run(f"""
CREATE OR REPLACE VIEW {NS}.mv_supplier_risk
WITH METRICS
LANGUAGE YAML
COMMENT 'Supplier risk exposure KPIs by category, flag, country.'
AS $$
version: 1.1
comment: "Supplier risk exposure metrics"
source: {NS}.SUPPLIER_RISK
joins:
  - name: sup
    source: {NS}.SUPPLIER
    on: source.LIFNR = sup.LIFNR
dimensions:
  - name: Risk Category
    expr: source.RISK_CATEGORY
  - name: Delivery Risk Flag
    expr: source.DELIVERY_RISK_FLAG
  - name: Vendor Country
    expr: sup.LAND1
  - name: Valid From
    expr: source.VALID_FROM
measures:
  - name: Risk Assessments
    expr: COUNT(1)
  - name: Distinct Vendors
    expr: COUNT(DISTINCT source.LIFNR)
  - name: Delivery Risk Vendors
    expr: COUNT(DISTINCT CASE WHEN source.DELIVERY_RISK_FLAG = 'X' THEN source.LIFNR END)
  - name: Avg Risk Score
    expr: AVG(source.RISK_SCORE)
  - name: Max Risk Score
    expr: MAX(source.RISK_SCORE)
$$
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## mv_delivery_performance — on-time delivery
# MAGIC Dimensions: supplier, plant (via purchasing org proxy), receipt month.
# MAGIC Measures: deliveries, on-time count, OTD rate.

# COMMAND ----------

run(f"""
CREATE OR REPLACE VIEW {NS}.mv_delivery_performance
WITH METRICS
LANGUAGE YAML
COMMENT 'On-time delivery KPIs by supplier and month.'
AS $$
version: 1.1
comment: "Delivery performance metrics"
source: {NS}.PURCHASE_ORDER
filter: GR_DATE IS NOT NULL
dimensions:
  - name: Supplier
    expr: LIFNR
  - name: Purchasing Org
    expr: EKORG
  - name: Receipt Month
    expr: DATE_TRUNC('MONTH', GR_DATE)
  - name: Order Month
    expr: DATE_TRUNC('MONTH', ORDER_DATE)
measures:
  - name: Deliveries
    expr: COUNT(1)
  - name: On Time Deliveries
    expr: SUM(CASE WHEN ON_TIME_FLAG THEN 1 ELSE 0 END)
  - name: OTD Rate
    expr: AVG(CASE WHEN ON_TIME_FLAG THEN 1.0 ELSE 0.0 END)
$$
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## mv_price_analytics — net price analytics
# MAGIC Dimensions: material, supplier, purchasing org.
# MAGIC Measures: PO lines, avg/min/max net price.

# COMMAND ----------

run(f"""
CREATE OR REPLACE VIEW {NS}.mv_price_analytics
WITH METRICS
LANGUAGE YAML
COMMENT 'Net price analytics KPIs by material and vendor.'
AS $$
version: 1.1
comment: "Price analytics metrics"
source: {NS}.PURCHASE_ORDER
dimensions:
  - name: Material
    expr: MATNR
  - name: Supplier
    expr: LIFNR
  - name: Purchasing Org
    expr: EKORG
measures:
  - name: PO Lines
    expr: COUNT(1)
  - name: Avg Net Price
    expr: AVG(NETPR)
  - name: Min Net Price
    expr: MIN(NETPR)
  - name: Max Net Price
    expr: MAX(NETPR)
  - name: Total Spend
    expr: SUM(NETPR)
$$
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify & smoke-test the metric views

# COMMAND ----------

for mv in ["mv_sourcing_coverage", "mv_supplier_risk", "mv_delivery_performance", "mv_price_analytics"]:
    print(f"===== {mv} =====")
    spark.sql(f"DESCRIBE {NS}.{mv}").show(truncate=False)

# Sample measure queries.
print("OTD by supplier (sample):")
spark.sql(f"""
  SELECT Supplier, MEASURE(`OTD Rate`) AS otd, MEASURE(`Deliveries`) AS n
  FROM {NS}.mv_delivery_performance GROUP BY ALL ORDER BY otd LIMIT 5
""").show()

print("Delivery-risk vendors by country:")
spark.sql(f"""
  SELECT `Vendor Country`, MEASURE(`Delivery Risk Vendors`) AS risky
  FROM {NS}.mv_supplier_risk GROUP BY ALL ORDER BY risky DESC
""").show()

# COMMAND ----------

dbutils.notebook.exit("03b_metric_views: SUCCESS — 4 metric views created.")
