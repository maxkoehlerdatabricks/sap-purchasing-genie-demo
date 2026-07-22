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
    comment: "Plant the sourcing record applies to (SAP WERKS)."
  - name: Material
    expr: source.MATNR
    comment: "Material number being sourced (SAP MATNR)."
  - name: Material Group
    expr: mat.MATKL
    comment: "Purchasing material group the material belongs to (SAP MATKL), joined from MATERIAL."
  - name: Material Type
    expr: mat.MTART
    comment: "Material type such as ROH=raw / HALB=semi-finished (SAP MTART), joined from MATERIAL."
  - name: Is Valid Source
    expr: source.IS_VALID_SOURCE
    comment: "True when the source-list entry is currently valid and the vendor is not blocked."
measures:
  - name: Sourcing Records
    expr: COUNT(1)
    comment: "Count of source-list rows (material+plant+vendor combinations)."
  - name: Distinct Vendors
    expr: COUNT(DISTINCT source.LIFNR)
    comment: "Number of distinct vendors appearing in the sourcing records (any status)."
  - name: Valid Vendors
    expr: COUNT(DISTINCT CASE WHEN source.IS_VALID_SOURCE THEN source.LIFNR END)
    comment: "Number of distinct valid, non-blocked vendors — the basis for multi-sourcing."
  - name: Distinct Materials
    expr: COUNT(DISTINCT source.MATNR)
    comment: "Number of distinct materials that have at least one sourcing record."
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
    comment: "Risk classification bucket assigned to the supplier (e.g. financial, delivery, quality)."
  - name: Delivery Risk Flag
    expr: source.DELIVERY_RISK_FLAG
    comment: "'X' when the supplier carries an active delivery-risk flag; blank/NULL otherwise."
  - name: Vendor Country
    expr: sup.LAND1
    comment: "Vendor's country key (SAP LAND1), joined from SUPPLIER."
  - name: Valid From
    expr: source.VALID_FROM
    comment: "Start date of the risk assessment's validity window (time-slice start)."
measures:
  - name: Risk Assessments
    expr: COUNT(1)
    comment: "Count of supplier risk-assessment rows (time-sliced records)."
  - name: Distinct Vendors
    expr: COUNT(DISTINCT source.LIFNR)
    comment: "Number of distinct vendors that have at least one risk assessment."
  - name: Delivery Risk Vendors
    expr: COUNT(DISTINCT CASE WHEN source.DELIVERY_RISK_FLAG = 'X' THEN source.LIFNR END)
    comment: "Number of distinct vendors currently flagged with delivery risk (DELIVERY_RISK_FLAG='X')."
  - name: Avg Risk Score
    expr: AVG(source.RISK_SCORE)
    comment: "Average numeric risk score across the selected risk assessments."
  - name: Max Risk Score
    expr: MAX(source.RISK_SCORE)
    comment: "Highest (worst) numeric risk score across the selected risk assessments."
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
    comment: "Vendor/supplier that delivered the PO line (SAP LIFNR)."
  - name: Purchasing Org
    expr: EKORG
    comment: "Purchasing organization responsible for the order (SAP EKORG)."
  - name: Receipt Month
    expr: DATE_TRUNC('MONTH', GR_DATE)
    comment: "Calendar month of goods receipt (GR_DATE truncated to month)."
  - name: Order Month
    expr: DATE_TRUNC('MONTH', ORDER_DATE)
    comment: "Calendar month the PO was placed (ORDER_DATE truncated to month)."
measures:
  - name: Deliveries
    expr: COUNT(1)
    comment: "Count of received PO lines (rows where GR_DATE is not null, per the view filter)."
  - name: On Time Deliveries
    expr: SUM(CASE WHEN ON_TIME_FLAG THEN 1 ELSE 0 END)
    comment: "Count of received lines that arrived on or before the requested date (ON_TIME_FLAG=true)."
  - name: OTD Rate
    expr: AVG(CASE WHEN ON_TIME_FLAG THEN 1.0 ELSE 0.0 END)
    comment: "On-time delivery rate = on-time lines / received lines (0..1)."
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
    comment: "Material number the PO line is for (SAP MATNR)."
  - name: Supplier
    expr: LIFNR
    comment: "Vendor/supplier on the PO line (SAP LIFNR)."
  - name: Purchasing Org
    expr: EKORG
    comment: "Purchasing organization that placed the order (SAP EKORG)."
measures:
  - name: PO Lines
    expr: COUNT(1)
    comment: "Count of purchase-order line items in scope."
  - name: Avg Net Price
    expr: AVG(NETPR)
    comment: "Average net unit price across the selected PO lines (SAP NETPR)."
  - name: Min Net Price
    expr: MIN(NETPR)
    comment: "Lowest net unit price observed across the selected PO lines."
  - name: Max Net Price
    expr: MAX(NETPR)
    comment: "Highest net unit price observed across the selected PO lines."
  - name: Total Spend
    expr: SUM(NETPR)
    comment: "Sum of net prices across the selected PO lines (proxy for spend)."
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
