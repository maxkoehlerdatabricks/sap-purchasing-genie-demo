# Databricks notebook source
# MAGIC %md
# MAGIC # 03c — Value Dictionaries (code → human label lookups)
# MAGIC
# MAGIC SAP data is full of cryptic codes (MTART='ROH', SPERM='X', VGABE='1', ...). Value
# MAGIC dictionaries map each coded value to a plain-language label + description. They are
# MAGIC critical for Genie: they let an analyst ask "show me *raw* materials" and have
# MAGIC Genie translate that to `MTART='ROH'`.
# MAGIC
# MAGIC We materialize them as small governed Delta tables (with comments) and also emit a
# MAGIC single consolidated `VALUE_DICTIONARY` table that Genie can be pointed at. Genie
# MAGIC instructions (05_genie) reference these values explicitly.

# COMMAND ----------

# MAGIC %run ../00_config

# COMMAND ----------

from pyspark.sql import Row

NS = CFG.namespace

# Each entry: (domain, code, label, description)
DICT = [
    # Material type (MARA.MTART)
    ("MTART", "ROH", "Raw material", "Externally procured raw material."),
    ("MTART", "HALB", "Semi-finished", "Semi-finished product / assembly."),
    ("MTART", "FERT", "Finished good", "Finished product sold to customers."),
    # Material group (MARA.MATKL)
    ("MATKL", "ELEC", "Electronics", "Electronic components and assemblies."),
    ("MATKL", "MECH", "Mechanical", "Mechanical parts."),
    ("MATKL", "CHEM", "Chemicals", "Chemical inputs."),
    ("MATKL", "ELAST", "Elastomers", "Rubber and elastomer parts."),
    ("MATKL", "FAST", "Fasteners", "Screws, bolts, fasteners."),
    # Purchasing block (LFM1.SPERM)
    ("SPERM", "X", "Blocked", "Vendor blocked for purchasing in this org."),
    ("SPERM", "", "Not blocked", "Vendor is not blocked for purchasing."),
    # Source-list block (EORD.NOTKZ)
    ("NOTKZ", "X", "Blocked source", "Source-list entry is blocked."),
    ("NOTKZ", "", "Valid source", "Source-list entry is valid (non-blocked)."),
    # Fixed vendor (EORD.FLIFN)
    ("FLIFN", "X", "Fixed vendor", "Preferred / fixed source of supply."),
    ("FLIFN", "", "Not fixed", "Vendor is not marked as fixed source."),
    # Delivery risk flag (SUPPLIER_RISK.DELIVERY_RISK_FLAG)
    ("DELIVERY_RISK_FLAG", "X", "Delivery risk", "Supplier flagged with delivery risk."),
    ("DELIVERY_RISK_FLAG", "", "No delivery risk", "No delivery-risk flag."),
    # Risk category (SUPPLIER_RISK.RISK_CATEGORY)
    ("RISK_CATEGORY", "FINANCIAL", "Financial risk", "Financial stability risk."),
    ("RISK_CATEGORY", "DELIVERY", "Delivery risk", "Delivery / supply-continuity risk."),
    ("RISK_CATEGORY", "QUALITY", "Quality risk", "Product quality risk."),
    ("RISK_CATEGORY", "COMPLIANCE", "Compliance risk", "Regulatory / compliance risk."),
    ("RISK_CATEGORY", "GEOPOLITICAL", "Geopolitical risk", "Geopolitical / regional risk."),
    # Country (LFA1.LAND1)
    ("LAND1", "DE", "Germany", "Vendor located in Germany."),
    ("LAND1", "FR", "France", "Vendor located in France."),
    ("LAND1", "IT", "Italy", "Vendor located in Italy."),
    ("LAND1", "CN", "China", "Vendor located in China."),
    ("LAND1", "US", "United States", "Vendor located in the United States."),
    ("LAND1", "PL", "Poland", "Vendor located in Poland."),
    # PO history event (EKBE.VGABE)
    ("VGABE", "1", "Goods receipt", "Goods-receipt event."),
    ("VGABE", "2", "Invoice receipt", "Invoice-receipt event."),
]

rows = [Row(DOMAIN=d, CODE=c, LABEL=l, DESCRIPTION=desc) for (d, c, l, desc) in DICT]
df = spark.createDataFrame(rows)

(df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
   .saveAsTable(CFG.fq("VALUE_DICTIONARY")))

spark.sql(f"""COMMENT ON TABLE {NS}.VALUE_DICTIONARY IS
  'Value dictionary: maps coded SAP values (DOMAIN, CODE) to human-readable LABEL and DESCRIPTION. Used by Genie to translate business language into coded filters (e.g. "raw materials" -> MTART=ROH, "blocked vendor" -> SPERM=X).'""")
for col, cmt in {
    "DOMAIN": "The field/domain the code belongs to (e.g. MTART, RISK_CATEGORY).",
    "CODE": "The raw coded value as stored in the data.",
    "LABEL": "Human-readable label for the code.",
    "DESCRIPTION": "Longer description of what the code means.",
}.items():
    spark.sql(f"ALTER TABLE {NS}.VALUE_DICTIONARY ALTER COLUMN {col} COMMENT '{cmt}'")

print(f"VALUE_DICTIONARY rows: {spark.table(CFG.fq('VALUE_DICTIONARY')).count()}")
display(spark.table(CFG.fq("VALUE_DICTIONARY")).orderBy("DOMAIN", "CODE"))

# COMMAND ----------

dbutils.notebook.exit("03c_value_dictionaries: SUCCESS — VALUE_DICTIONARY created.")
