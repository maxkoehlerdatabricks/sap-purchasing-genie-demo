# Databricks notebook source
# MAGIC %md
# MAGIC # 03a — SQL Functions as Parameterized Trusted Assets
# MAGIC
# MAGIC These Unity Catalog SQL UDFs encapsulate the demo's core sourcing-risk logic as
# MAGIC **governed, reusable, parameterized** assets. They are ideal for Genie because:
# MAGIC * They are **certified answers** — Genie can call a trusted function instead of
# MAGIC   synthesizing fragile SQL for a nuanced question (e.g. "materials without
# MAGIC   multi-sourcing").
# MAGIC * They are **parameterized** — the analyst can vary `min_vendors`, `as_of_date`,
# MAGIC   `months`, etc. right from Genie / SQL.
# MAGIC * They carry **comments** so their intent is discoverable in the catalog.
# MAGIC
# MAGIC Each function maps to one or more of the 11 business questions. All read only from
# MAGIC the 5 gold tables.

# COMMAND ----------

# MAGIC %run ../00_config

# COMMAND ----------

NS = CFG.namespace


def run(sql):
    print("→ creating function")
    spark.sql(sql)


# COMMAND ----------

# MAGIC %md
# MAGIC ## fn `materials_without_multisourcing(min_vendors INT)`  → Q1
# MAGIC Materials (per plant) with fewer than `min_vendors` **valid, non-blocked** vendors.
# MAGIC A table-valued function so it can be `SELECT * FROM fn(...)`.

# COMMAND ----------

run(f"""
CREATE OR REPLACE FUNCTION {NS}.materials_without_multisourcing(min_vendors INT COMMENT 'Minimum valid vendors required to be considered multi-sourced. The standard threshold is 2.')
RETURNS TABLE (MATNR STRING, WERKS STRING, valid_vendor_count INT)
COMMENT 'Q1: materials with no multi-sourcing strategy — material+plant combinations with fewer than min_vendors valid, non-blocked vendors. The standard threshold is 2 (i.e. single-sourced or unsourced materials lack a multi-sourcing strategy).'
RETURN
  SELECT MATNR, WERKS, CAST(COUNT(*) AS INT) AS valid_vendor_count
  FROM {NS}.SOURCING
  WHERE IS_VALID_SOURCE = true
  GROUP BY MATNR, WERKS
  HAVING COUNT(*) < min_vendors
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## fn `suppliers_with_delivery_risk(as_of_date DATE)`  → Q2 support
# MAGIC Distinct suppliers whose delivery-risk flag is active on `as_of_date`.

# COMMAND ----------

run(f"""
CREATE OR REPLACE FUNCTION {NS}.suppliers_with_delivery_risk(as_of_date DATE COMMENT 'Date at which the delivery-risk window must be active')
RETURNS TABLE (LIFNR STRING, RISK_CATEGORY STRING, RISK_SCORE INT, VALID_FROM DATE, VALID_TO DATE)
COMMENT 'Suppliers with an ACTIVE delivery-risk flag on the given date (VALID_FROM<=date<=VALID_TO and DELIVERY_RISK_FLAG=X).'
RETURN
  SELECT LIFNR, RISK_CATEGORY, RISK_SCORE, VALID_FROM, VALID_TO
  FROM {NS}.SUPPLIER_RISK
  WHERE DELIVERY_RISK_FLAG = 'X'
    AND as_of_date BETWEEN VALID_FROM AND VALID_TO
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## fn `materials_linked_to_risky_supplier(as_of_date DATE)`  → Q2
# MAGIC Materials source-listed to a supplier with an active delivery-risk flag.

# COMMAND ----------

run(f"""
CREATE OR REPLACE FUNCTION {NS}.materials_linked_to_risky_supplier(as_of_date DATE COMMENT 'Date at which supplier delivery risk must be active')
RETURNS TABLE (MATNR STRING, WERKS STRING, LIFNR STRING, RISK_SCORE INT)
COMMENT 'Q2: materials (per plant) linked via a valid source list to a supplier flagged with active delivery risk.'
RETURN
  SELECT s.MATNR, s.WERKS, s.LIFNR, r.RISK_SCORE
  FROM {NS}.SOURCING s
  JOIN {NS}.SUPPLIER_RISK r ON s.LIFNR = r.LIFNR
  WHERE s.IS_VALID_SOURCE = true
    AND r.DELIVERY_RISK_FLAG = 'X'
    AND as_of_date BETWEEN r.VALID_FROM AND r.VALID_TO
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## fn `single_sourced_risky(as_of_date DATE)`  → Q3
# MAGIC Materials that are single-sourced AND whose sole valid supplier has an active
# MAGIC delivery-risk flag — the highest-priority supply-continuity risk.

# COMMAND ----------

run(f"""
CREATE OR REPLACE FUNCTION {NS}.single_sourced_risky(as_of_date DATE COMMENT 'Date at which supplier delivery risk must be active')
RETURNS TABLE (MATNR STRING, WERKS STRING, LIFNR STRING, RISK_SCORE INT)
COMMENT 'Q3: materials that are single-sourced (exactly one valid vendor) AND whose sole supplier has an active delivery-risk flag.'
RETURN
  WITH valid_counts AS (
    SELECT MATNR, WERKS, COUNT(*) AS vc, MAX(LIFNR) AS sole_lifnr
    FROM {NS}.SOURCING
    WHERE IS_VALID_SOURCE = true
    GROUP BY MATNR, WERKS
    HAVING COUNT(*) = 1
  )
  SELECT vc.MATNR, vc.WERKS, vc.sole_lifnr AS LIFNR, r.RISK_SCORE
  FROM valid_counts vc
  JOIN {NS}.SUPPLIER_RISK r ON vc.sole_lifnr = r.LIFNR
  WHERE r.DELIVERY_RISK_FLAG = 'X'
    AND as_of_date BETWEEN r.VALID_FROM AND r.VALID_TO
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## fn `vendor_count_per_material()`  → Q4
# MAGIC Vendor count per material at each plant (all source-listed vendors).

# COMMAND ----------

run(f"""
CREATE OR REPLACE FUNCTION {NS}.vendor_count_per_material()
RETURNS TABLE (MATNR STRING, WERKS STRING, vendor_count INT, valid_vendor_count INT)
COMMENT 'Q4: number of vendors per material at each plant (total source-listed and valid non-blocked).'
RETURN
  SELECT MATNR, WERKS,
         CAST(COUNT(*) AS INT) AS vendor_count,
         CAST(SUM(CASE WHEN IS_VALID_SOURCE THEN 1 ELSE 0 END) AS INT) AS valid_vendor_count
  FROM {NS}.SOURCING
  GROUP BY MATNR, WERKS
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## fn `blocked_suppliers_with_active_sourcing()`  → Q5
# MAGIC Suppliers that hold a purchasing block (SPERM='X') but still appear in valid,
# MAGIC active source lists — a governance red flag.

# COMMAND ----------

run(f"""
CREATE OR REPLACE FUNCTION {NS}.blocked_suppliers_with_active_sourcing()
RETURNS TABLE (LIFNR STRING, EKORG STRING, MATNR STRING, WERKS STRING)
COMMENT 'Q5: suppliers holding a purchasing block (SPERM=X) that nonetheless have active, valid sourcing records.'
RETURN
  SELECT DISTINCT sup.LIFNR, sup.EKORG, src.MATNR, src.WERKS
  FROM {NS}.SUPPLIER sup
  JOIN {NS}.SOURCING src ON sup.LIFNR = src.LIFNR
  WHERE sup.SPERM = 'X'
    AND src.IS_VALID_SOURCE = true
    AND current_date() BETWEEN src.VDATU AND src.BDATU
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## fn `quota_concentration(threshold_pct INT)`  → Q6
# MAGIC Materials where a single vendor holds more than `threshold_pct`% of the quota.

# COMMAND ----------

run(f"""
CREATE OR REPLACE FUNCTION {NS}.quota_concentration(threshold_pct INT COMMENT 'Quota percentage threshold, e.g. 80')
RETURNS TABLE (MATNR STRING, WERKS STRING, LIFNR STRING, QUOTA INT)
COMMENT 'Q6: materials with a quota arrangement where one vendor holds more than threshold_pct percent of the quota.'
RETURN
  SELECT MATNR, WERKS, LIFNR, QUOTA
  FROM {NS}.SOURCING
  WHERE QUOTA IS NOT NULL AND QUOTA > threshold_pct
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## fn `otd_rate_per_supplier(months INT)`  → Q7
# MAGIC On-time delivery rate per supplier over the last `months` months.

# COMMAND ----------

run(f"""
CREATE OR REPLACE FUNCTION {NS}.otd_rate_per_supplier(months INT COMMENT 'Look-back window in months, e.g. 12')
RETURNS TABLE (LIFNR STRING, deliveries BIGINT, on_time BIGINT, otd_rate DOUBLE)
COMMENT 'Q7: on-time delivery rate per supplier over the last N months (based on received PO lines).'
RETURN
  SELECT LIFNR,
         COUNT(*) AS deliveries,
         SUM(CASE WHEN ON_TIME_FLAG THEN 1 ELSE 0 END) AS on_time,
         ROUND(AVG(CASE WHEN ON_TIME_FLAG THEN 1.0 ELSE 0.0 END), 4) AS otd_rate
  FROM {NS}.PURCHASE_ORDER
  WHERE GR_DATE IS NOT NULL
    AND GR_DATE >= add_months(current_date(), -months)
  GROUP BY LIFNR
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## fn `pos_under_active_risk()`  → Q8
# MAGIC PO lines placed with a supplier while that supplier had an active delivery-risk
# MAGIC flag **at the order date** (time-sliced join).

# COMMAND ----------

run(f"""
CREATE OR REPLACE FUNCTION {NS}.pos_under_active_risk()
RETURNS TABLE (EBELN STRING, EBELP STRING, MATNR STRING, LIFNR STRING, ORDER_DATE DATE, RISK_SCORE INT)
COMMENT 'Q8: purchase-order lines placed with a supplier while a delivery-risk flag was active at the order date.'
RETURN
  SELECT po.EBELN, po.EBELP, po.MATNR, po.LIFNR, po.ORDER_DATE, r.RISK_SCORE
  FROM {NS}.PURCHASE_ORDER po
  JOIN {NS}.SUPPLIER_RISK r ON po.LIFNR = r.LIFNR
  WHERE r.DELIVERY_RISK_FLAG = 'X'
    AND po.ORDER_DATE BETWEEN r.VALID_FROM AND r.VALID_TO
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## fn `suppliers_multi_material_group()`  → Q9
# MAGIC Suppliers that provide materials across more than one material group (MATKL).

# COMMAND ----------

run(f"""
CREATE OR REPLACE FUNCTION {NS}.suppliers_multi_material_group()
RETURNS TABLE (LIFNR STRING, material_group_count INT)
COMMENT 'Q9: suppliers that supply materials spanning more than one material group (MATKL).'
RETURN
  SELECT s.LIFNR, CAST(COUNT(DISTINCT m.MATKL) AS INT) AS material_group_count
  FROM {NS}.SOURCING s
  JOIN {NS}.MATERIAL m ON s.MATNR = m.MATNR AND s.WERKS = m.WERKS
  GROUP BY s.LIFNR
  HAVING COUNT(DISTINCT m.MATKL) > 1
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## fn `materials_without_valid_sourcing()`  → Q10
# MAGIC Materials with no valid sourcing record at all (either no source list, or all
# MAGIC source-list entries blocked).

# COMMAND ----------

run(f"""
CREATE OR REPLACE FUNCTION {NS}.materials_without_valid_sourcing()
RETURNS TABLE (MATNR STRING, WERKS STRING)
COMMENT 'Q10: materials (per plant) with no valid, non-blocked sourcing record at all.'
RETURN
  SELECT m.MATNR, m.WERKS
  FROM {NS}.MATERIAL m
  LEFT JOIN (
    SELECT DISTINCT MATNR, WERKS FROM {NS}.SOURCING WHERE IS_VALID_SOURCE = true
  ) v ON m.MATNR = v.MATNR AND m.WERKS = v.WERKS
  WHERE v.MATNR IS NULL
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## fn `net_price_variance_per_material()`  → Q11
# MAGIC Average net-price variance across vendors for the same material (spread of
# MAGIC per-vendor average prices).

# COMMAND ----------

run(f"""
CREATE OR REPLACE FUNCTION {NS}.net_price_variance_per_material()
RETURNS TABLE (MATNR STRING, vendor_count INT, min_avg_price DOUBLE, max_avg_price DOUBLE, price_spread DOUBLE, price_stddev DOUBLE)
COMMENT 'Q11: net-price variance across vendors for the same material (spread and stddev of per-vendor average NETPR).'
RETURN
  WITH per_vendor AS (
    SELECT MATNR, LIFNR, AVG(NETPR) AS avg_price
    FROM {NS}.PURCHASE_ORDER
    GROUP BY MATNR, LIFNR
  )
  SELECT MATNR,
         CAST(COUNT(*) AS INT) AS vendor_count,
         ROUND(MIN(avg_price), 2) AS min_avg_price,
         ROUND(MAX(avg_price), 2) AS max_avg_price,
         ROUND(MAX(avg_price) - MIN(avg_price), 2) AS price_spread,
         ROUND(STDDEV(avg_price), 2) AS price_stddev
  FROM per_vendor
  GROUP BY MATNR
  HAVING COUNT(*) > 1
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameterless default / "today" variants for Genie CERTIFIED ANSWERS
# MAGIC Genie certified answers are most reliable when the function takes **no
# MAGIC arguments** (and Genie certified-answer functions **do not support DATE
# MAGIC arguments** at all). So we expose no-arg wrappers that bake in the standard
# MAGIC defaults:
# MAGIC * `materials_without_multisourcing_default()` → threshold of 2 (Q1).
# MAGIC * `otd_rate_per_supplier_last_12m()` → 12-month look-back (Q7).
# MAGIC * the `_today()` risk wrappers → default the date to `current_date()`.
# MAGIC
# MAGIC The parameterized versions above remain available as trusted assets for ad-hoc
# MAGIC SQL and time-travel questions.

# COMMAND ----------

# Q1 no-arg wrapper — bakes in the standard multi-sourcing threshold of 2 so Genie can
# call it reliably as a certified answer for "which materials have no multi-sourcing
# strategy?".
run(f"""
CREATE OR REPLACE FUNCTION {NS}.materials_without_multisourcing_default()
RETURNS TABLE (MATNR STRING, WERKS STRING, valid_vendor_count INT)
COMMENT 'Q1 (default threshold): materials with no multi-sourcing strategy — fewer than the standard 2 valid, non-blocked vendors. No-arg wrapper over materials_without_multisourcing(2) for reliable Genie certified answers.'
RETURN SELECT * FROM {NS}.materials_without_multisourcing(2)
""")

# Q7 no-arg wrapper — bakes in the standard 12-month look-back window so Genie can call
# it reliably as a certified answer for "on-time delivery rate per supplier".
run(f"""
CREATE OR REPLACE FUNCTION {NS}.otd_rate_per_supplier_last_12m()
RETURNS TABLE (LIFNR STRING, deliveries BIGINT, on_time BIGINT, otd_rate DOUBLE)
COMMENT 'Q7 (last 12 months): on-time delivery rate per supplier over the standard 12-month look-back. No-arg wrapper over otd_rate_per_supplier(12) for reliable Genie certified answers.'
RETURN SELECT * FROM {NS}.otd_rate_per_supplier(12)
""")

run(f"""
CREATE OR REPLACE FUNCTION {NS}.materials_linked_to_risky_supplier_today()
RETURNS TABLE (MATNR STRING, WERKS STRING, LIFNR STRING, RISK_SCORE INT)
COMMENT 'Q2 (today): materials linked via a valid source list to a supplier with an ACTIVE delivery-risk flag as of today.'
RETURN SELECT * FROM {NS}.materials_linked_to_risky_supplier(current_date())
""")

run(f"""
CREATE OR REPLACE FUNCTION {NS}.single_sourced_risky_today()
RETURNS TABLE (MATNR STRING, WERKS STRING, LIFNR STRING, RISK_SCORE INT)
COMMENT 'Q3 (today): single-sourced materials whose sole supplier has an ACTIVE delivery-risk flag as of today.'
RETURN SELECT * FROM {NS}.single_sourced_risky(current_date())
""")

run(f"""
CREATE OR REPLACE FUNCTION {NS}.suppliers_with_delivery_risk_today()
RETURNS TABLE (LIFNR STRING, RISK_CATEGORY STRING, RISK_SCORE INT, VALID_FROM DATE, VALID_TO DATE)
COMMENT 'Suppliers with an ACTIVE delivery-risk flag as of today.'
RETURN SELECT * FROM {NS}.suppliers_with_delivery_risk(current_date())
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify & smoke-test the functions

# COMMAND ----------

funcs = spark.sql(f"SHOW USER FUNCTIONS IN {NS}").collect()
print("Functions created:")
for f in funcs:
    print("  ", f[0])

print("\nSmoke tests:")
print("Q1 materials_without_multisourcing(2):",
      spark.sql(f"SELECT count(*) c FROM {NS}.materials_without_multisourcing(2)").first()["c"])
print("Q1 materials_without_multisourcing_default():",
      spark.sql(f"SELECT count(*) c FROM {NS}.materials_without_multisourcing_default()").first()["c"])
print("Q7 otd_rate_per_supplier_last_12m():",
      spark.sql(f"SELECT count(*) c FROM {NS}.otd_rate_per_supplier_last_12m()").first()["c"])
print("Q3 single_sourced_risky(current_date):",
      spark.sql(f"SELECT count(*) c FROM {NS}.single_sourced_risky(current_date())").first()["c"])
print("Q6 quota_concentration(80):",
      spark.sql(f"SELECT count(*) c FROM {NS}.quota_concentration(80)").first()["c"])
print("Q7 otd_rate_per_supplier(12):",
      spark.sql(f"SELECT count(*) c FROM {NS}.otd_rate_per_supplier(12)").first()["c"])
print("Q8 pos_under_active_risk():",
      spark.sql(f"SELECT count(*) c FROM {NS}.pos_under_active_risk()").first()["c"])
print("Q10 materials_without_valid_sourcing():",
      spark.sql(f"SELECT count(*) c FROM {NS}.materials_without_valid_sourcing()").first()["c"])

# COMMAND ----------

dbutils.notebook.exit("03a_sql_functions: SUCCESS — trusted SQL functions created.")
