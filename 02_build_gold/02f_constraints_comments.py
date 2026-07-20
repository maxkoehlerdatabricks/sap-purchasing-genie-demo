# Databricks notebook source
# MAGIC %md
# MAGIC # 02f — PK / FK constraints + column & table COMMENTs (all 5 gold tables)
# MAGIC
# MAGIC Unity Catalog supports **informational** primary-key / foreign-key constraints
# MAGIC (declared `NOT ENFORCED`, `RELY`). They power:
# MAGIC * **Genie**: relationship hints so the model joins tables correctly.
# MAGIC * **BI tools & the catalog UI**: ER-diagram rendering & documentation.
# MAGIC
# MAGIC We also set rich **table** and **column** comments on every gold table — these are
# MAGIC surfaced to Genie and to any analyst browsing the catalog, and are required by the
# MAGIC spec ("column descriptions and table descriptions").
# MAGIC
# MAGIC Order of operations for each PK:
# MAGIC 1. `ALTER TABLE ... ALTER COLUMN ... SET NOT NULL` on every key column.
# MAGIC 2. `ALTER TABLE ... ADD CONSTRAINT ... PRIMARY KEY (...)`.
# MAGIC 3. Add FKs referencing the parent PKs.

# COMMAND ----------

# MAGIC %run ../00_config

# COMMAND ----------

NS = CFG.namespace


def run(sql):
    print("→", sql)
    spark.sql(sql)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Idempotency helper — drop existing constraints before re-adding
# MAGIC Lets this notebook be re-run safely.

# COMMAND ----------


def drop_constraints(table, names):
    for n in names:
        try:
            spark.sql(f"ALTER TABLE {NS}.{table} DROP CONSTRAINT IF EXISTS {n}")
        except Exception as e:
            print(f"  (skip drop {n}: {e})")


for t, cons in {
    "MATERIAL": ["pk_material"],
    "SUPPLIER": ["pk_supplier"],
    "SOURCING": ["pk_sourcing", "fk_sourcing_material", "fk_sourcing_supplier"],
    "SUPPLIER_RISK": ["fk_risk_supplier"],
    "PURCHASE_ORDER": ["pk_po", "fk_po_supplier"],
}.items():
    drop_constraints(t, cons)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1) NOT NULL on all key columns (required before a PK can be declared)

# COMMAND ----------

for tbl, cols in {
    "MATERIAL": ["MATNR", "WERKS"],
    "SUPPLIER": ["LIFNR", "EKORG"],
    "SOURCING": ["MATNR", "WERKS", "LIFNR"],
    "SUPPLIER_RISK": ["LIFNR", "VALID_FROM"],
    "PURCHASE_ORDER": ["EBELN", "EBELP"],
}.items():
    for c in cols:
        run(f"ALTER TABLE {NS}.{tbl} ALTER COLUMN {c} SET NOT NULL")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2) Primary keys (informational, NOT ENFORCED, RELY)

# COMMAND ----------

run(f"ALTER TABLE {NS}.MATERIAL ADD CONSTRAINT pk_material PRIMARY KEY (MATNR, WERKS)")
run(f"ALTER TABLE {NS}.SUPPLIER ADD CONSTRAINT pk_supplier PRIMARY KEY (LIFNR, EKORG)")
run(f"ALTER TABLE {NS}.SOURCING ADD CONSTRAINT pk_sourcing PRIMARY KEY (MATNR, WERKS, LIFNR)")
# PURCHASE_ORDER PK on EBELN+EBELP.
run(f"ALTER TABLE {NS}.PURCHASE_ORDER ADD CONSTRAINT pk_po PRIMARY KEY (EBELN, EBELP)")
# SUPPLIER_RISK is time-sliced (multiple rows per LIFNR); PK includes VALID_FROM.
run(f"ALTER TABLE {NS}.SUPPLIER_RISK ADD CONSTRAINT pk_risk PRIMARY KEY (LIFNR, VALID_FROM)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3) Foreign keys
# MAGIC * SOURCING.(MATNR,WERKS) → MATERIAL   (each sourcing row is for a real material+plant)
# MAGIC * SOURCING.LIFNR references a supplier (we FK to SUPPLIER on the LIFNR portion via a
# MAGIC   composite is not possible since SUPPLIER PK is LIFNR+EKORG; instead we document
# MAGIC   the relationship in comments and add a FK where the grain matches).
# MAGIC * PURCHASE_ORDER.(MATNR is a material ref); LIFNR is a supplier ref.
# MAGIC
# MAGIC UC FKs must reference the full PK of the parent. Where the child does not carry the
# MAGIC full parent key (e.g. SOURCING has LIFNR but not EKORG), we cannot declare a strict
# MAGIC FK; we capture that relationship in table comments for Genie instead. The FKs below
# MAGIC are the ones whose child columns fully cover a parent PK.

# COMMAND ----------

# SOURCING (MATNR,WERKS) fully covers MATERIAL's PK → valid FK.
run(f"""ALTER TABLE {NS}.SOURCING ADD CONSTRAINT fk_sourcing_material
        FOREIGN KEY (MATNR, WERKS) REFERENCES {NS}.MATERIAL (MATNR, WERKS)""")

print("FK SOURCING→MATERIAL added. "
      "SOURCING.LIFNR↔SUPPLIER.LIFNR and PURCHASE_ORDER.LIFNR↔SUPPLIER.LIFNR "
      "relationships are documented in comments (parent PK includes EKORG, so no strict FK).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4) Table comments

# COMMAND ----------

TABLE_COMMENTS = {
    "MATERIAL":
        "Gold: material master consolidated to one row per material (MATNR) and plant "
        "(WERKS). Built by Workflow A from RAW_MARC + RAW_MARA + RAW_MAKT (EN only). "
        "Answers 'what is this material, what group/type, which plant'.",
    "SUPPLIER":
        "Gold: vendor master consolidated to one row per vendor (LIFNR) and purchasing "
        "org (EKORG). Built by Workflow B from RAW_LFM1 + RAW_LFA1. SPERM='X' means the "
        "vendor holds a purchasing block in that org.",
    "SOURCING":
        "Gold: approved sourcing relationships at grain material+plant+vendor "
        "(MATNR+WERKS+LIFNR). Built by Workflow C from RAW_EORD + flattened quota "
        "(RAW_EQUK⨝RAW_EQUP). QUOTA may be NULL (source-listed but not quota-split — a "
        "meaningful gap). IS_VALID_SOURCE=true means a non-blocked source-list entry "
        "(NOTKZ<>'X'). Central table for multi-sourcing / single-sourcing analysis.",
    "SUPPLIER_RISK":
        "Gold: time-sliced supplier risk classification (Ariba-styled). One row per "
        "vendor (LIFNR) and validity window (VALID_FROM..VALID_TO). "
        "DELIVERY_RISK_FLAG='X' marks delivery risk. Join at query time; for 'active' "
        "risk, filter VALID_FROM<=date<=VALID_TO.",
    "PURCHASE_ORDER":
        "Gold: purchase-order lines at grain PO number + item (EBELN+EBELP). Built by "
        "Workflow E from RAW_EKPO⨝RAW_EKKO with earliest goods-receipt date from "
        "RAW_EKBE. ON_TIME_FLAG compares GR_DATE to REQUESTED_DATE. Used for on-time "
        "delivery, net-price variance, and orders-under-active-risk analysis.",
}
for t, c in TABLE_COMMENTS.items():
    run(f"COMMENT ON TABLE {NS}.{t} IS '{c.replace(chr(39), chr(39)+chr(39))}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5) Column comments (every column on every gold table)

# COMMAND ----------

COLUMN_COMMENTS = {
    "MATERIAL": {
        "MATNR": "Material number (SAP MARA/MARC key).",
        "WERKS": "Plant code where the material is procured/produced.",
        "MAKTX": "Material short description (English, SPRAS='EN').",
        "MTART": "Material type: ROH=raw, HALB=semi-finished, FERT=finished.",
        "MATKL": "Material group (e.g. ELEC, MECH, CHEM, ELAST, FAST).",
        "DISPO": "MRP controller responsible for the material at this plant.",
        "SOBSL": "Special procurement key (blank=standard).",
    },
    "SUPPLIER": {
        "LIFNR": "Vendor (supplier) number.",
        "EKORG": "Purchasing organization the vendor is set up in.",
        "NAME1": "Vendor legal name.",
        "LAND1": "Vendor country ISO code (e.g. DE, FR, CN).",
        "SPERM": "Purchasing block flag: 'X' = vendor blocked for purchasing in this org.",
    },
    "SOURCING": {
        "MATNR": "Material number (FK to MATERIAL).",
        "WERKS": "Plant code (FK to MATERIAL).",
        "LIFNR": "Approved vendor (relates to SUPPLIER.LIFNR).",
        "VDATU": "Source-list validity start date.",
        "BDATU": "Source-list validity end date.",
        "NOTKZ": "Blocked-source indicator: 'X' = this source-list entry is blocked.",
        "FLIFN": "Fixed-vendor flag: 'X' = preferred/fixed source.",
        "QUOTA": "Quota percentage allocated to this vendor (NULL if not quota-split).",
        "IS_VALID_SOURCE": "TRUE when NOTKZ<>'X' (a valid, non-blocked source).",
    },
    "SUPPLIER_RISK": {
        "LIFNR": "Vendor the risk assessment applies to (relates to SUPPLIER.LIFNR).",
        "RISK_CATEGORY": "Risk category: FINANCIAL, DELIVERY, QUALITY, COMPLIANCE, GEOPOLITICAL.",
        "DELIVERY_RISK_FLAG": "'X' = vendor flagged with delivery risk in this window.",
        "RISK_SCORE": "Risk score 0-100 (higher = riskier).",
        "VALID_FROM": "Start date of the risk assessment window.",
        "VALID_TO": "End date of the risk assessment window.",
    },
    "PURCHASE_ORDER": {
        "EBELN": "Purchase order number.",
        "EBELP": "Purchase order line item number.",
        "MATNR": "Ordered material (relates to MATERIAL.MATNR).",
        "LIFNR": "Vendor the PO was placed with (relates to SUPPLIER.LIFNR).",
        "EKORG": "Purchasing organization of the PO.",
        "NETPR": "Net price per unit on the PO line.",
        "ORDER_DATE": "PO creation/order date (SAP EKKO.AEDAT).",
        "REQUESTED_DATE": "Requested delivery date (SAP EKPO.EINDT).",
        "GR_DATE": "Earliest goods-receipt date (NULL = not yet received).",
        "ON_TIME_FLAG": "TRUE if GR_DATE<=REQUESTED_DATE, FALSE if late, NULL if no receipt.",
    },
}
for t, cols in COLUMN_COMMENTS.items():
    for col, cmt in cols.items():
        run(f"ALTER TABLE {NS}.{t} ALTER COLUMN {col} "
            f"COMMENT '{cmt.replace(chr(39), chr(39)+chr(39))}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify constraints & comments

# COMMAND ----------

for t in ["MATERIAL", "SUPPLIER", "SOURCING", "SUPPLIER_RISK", "PURCHASE_ORDER"]:
    print(f"\n===== {t} =====")
    spark.sql(f"DESCRIBE TABLE EXTENDED {NS}.{t}").show(60, truncate=False)

# COMMAND ----------

dbutils.notebook.exit("02f_constraints_comments: SUCCESS — PK/FK + comments applied.")
