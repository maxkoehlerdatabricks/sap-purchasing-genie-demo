# Databricks notebook source
# MAGIC %md
# MAGIC # 00_config — Single Source of Truth
# MAGIC
# MAGIC This notebook defines **every** configuration value used across the whole demo:
# MAGIC catalog, schema, warehouse, the LLM model, and the names of the Genie spaces,
# MAGIC the Multi-Agent Supervisor (MAS) endpoint and the Databricks App.
# MAGIC
# MAGIC ## How parameterization works (read this!)
# MAGIC * The demo is designed to be **parameterized in exactly one place**: the top-level
# MAGIC   orchestrator `00_run_all`. That orchestrator sets `catalog` / `schema` as plain
# MAGIC   Python variables **once** (`CATALOG = ...`, `SCHEMA = ...`) and forwards those
# MAGIC   variables into every child notebook when it runs them.
# MAGIC * Every notebook (including this one) `%run`s or imports these values by reading
# MAGIC   its own widgets. When a notebook is run standalone, the widget defaults below
# MAGIC   kick in, so you can also run any notebook on its own for debugging.
# MAGIC * The **catalog is taken as-is** (never created — it already exists in the
# MAGIC   workspace). The **schema is created** by the pipeline.
# MAGIC
# MAGIC Run this notebook with `%run ./00_config` from any other notebook to import the
# MAGIC `CFG` object and the helper functions.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widget defaults
# MAGIC These defaults are used when a notebook runs standalone. When launched from
# MAGIC `00_run_all`, the orchestrator overrides `catalog` and `schema`.

# COMMAND ----------

# We wrap widget creation in try/except so this file can also be imported in a
# plain Python context (e.g. unit-style testing) where dbutils is not present.
try:
    dbutils.widgets.text("catalog", "max_kohler_demo_catalog", "Unity Catalog (existing, taken as-is)")
    dbutils.widgets.text("schema", "purchasing_demo", "Schema (created by the demo)")
    dbutils.widgets.text("warehouse_id", "d7a3d1cfbba73c0a", "Serverless SQL Warehouse ID")
except NameError:
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## The `CFG` configuration object
# MAGIC A small dataclass-like container that every notebook uses. It reads the widgets
# MAGIC (falling back to defaults) so there is a single, consistent view of configuration.

# COMMAND ----------


class DemoConfig:
    """Central configuration for the SAP Purchasing Genie demo.

    All names are derived from ``catalog`` + ``schema`` so that pointing the demo at
    a different catalog/schema requires **no** other edits. Only the orchestrator
    (``00_run_all``) sets the widgets; everything else reads from this object.
    """

    def __init__(self):
        # ---- Read widgets (with safe fallbacks for standalone execution) ----
        try:
            self.catalog = dbutils.widgets.get("catalog")
            self.schema = dbutils.widgets.get("schema")
            self.warehouse_id = dbutils.widgets.get("warehouse_id")
        except Exception:
            self.catalog = "max_kohler_demo_catalog"
            self.schema = "purchasing_demo"
            self.warehouse_id = "d7a3d1cfbba73c0a"

        # ---- Derived, fully-qualified namespace ----
        self.namespace = f"{self.catalog}.{self.schema}"

        # ---- The LLM used by the MAS orchestrator and the App chat ----
        self.model = "databricks-claude-sonnet-4-5"

        # ---- Asset names (Genie spaces, MAS endpoint, App) ----
        # Genie space 1: Sourcing & Supplier Risk domain
        self.genie_space_1_name = "SAP Purchasing — Sourcing & Supplier Risk"
        # Genie space 2: Purchase Order Analytics domain
        self.genie_space_2_name = "SAP Purchasing — Purchase Order Analytics"
        # Agent Bricks Multi-Agent Supervisor
        self.mas_name = "sap_purchasing_supervisor"
        self.mas_endpoint_name = "sap-purchasing-mas"
        # Databricks App
        self.app_name = "sap-purchasing-app"
        # Serverless data-prep Job
        self.job_name = "sap_purchasing_data_prep"

    # ---- Convenience helpers ----
    def fq(self, table: str) -> str:
        """Return the fully-qualified name ``catalog.schema.table``."""
        return f"{self.namespace}.{table}"

    def __repr__(self):
        return (
            "DemoConfig(\n"
            f"  catalog       = {self.catalog!r}\n"
            f"  schema        = {self.schema!r}\n"
            f"  namespace     = {self.namespace!r}\n"
            f"  warehouse_id  = {self.warehouse_id!r}\n"
            f"  model         = {self.model!r}\n"
            f"  genie_space_1 = {self.genie_space_1_name!r}\n"
            f"  genie_space_2 = {self.genie_space_2_name!r}\n"
            f"  mas_endpoint  = {self.mas_endpoint_name!r}\n"
            f"  app_name      = {self.app_name!r}\n"
            ")"
        )


# The single global instance that other notebooks reference after `%run ./00_config`.
CFG = DemoConfig()

# COMMAND ----------

# MAGIC %md
# MAGIC ## The five gold tables and thirteen raw tables (canonical names)
# MAGIC Kept here so every notebook refers to the same identifiers.

# COMMAND ----------

# 13 raw (simulated) SAP-style source tables.
RAW_TABLES = [
    "RAW_MARA",           # Material master, general data
    "RAW_MAKT",           # Material descriptions
    "RAW_MARC",           # Material master, plant data
    "RAW_LFA1",           # Vendor master, general data
    "RAW_LFB1",           # Vendor master, company code data
    "RAW_LFM1",           # Vendor master, purchasing org data
    "RAW_EORD",           # Source list
    "RAW_EQUK",           # Quota arrangement header
    "RAW_EQUP",           # Quota arrangement item
    "RAW_SUPPLIER_RISK",  # Supplier risk classification (Ariba-styled)
    "RAW_EKKO",           # PO header
    "RAW_EKPO",           # PO line item
    "RAW_EKBE",           # PO history (GR/invoice events)
]

# 5 analysis-ready gold tables.
GOLD_TABLES = [
    "MATERIAL",        # grain: MATNR + WERKS
    "SUPPLIER",        # grain: LIFNR + EKORG
    "SOURCING",        # grain: MATNR + WERKS + LIFNR
    "SUPPLIER_RISK",   # grain: LIFNR (time-sliced)
    "PURCHASE_ORDER",  # grain: EBELN + EBELP
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ensure the schema exists
# MAGIC The catalog is taken as-is; the schema is created here (idempotent). This runs
# MAGIC only when Spark is available (i.e. inside a Databricks notebook).

# COMMAND ----------


def ensure_schema():
    """Create the target schema if it does not exist. Catalog is never created."""
    try:
        spark.sql(f"USE CATALOG {CFG.catalog}")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CFG.namespace}")
        spark.sql(f"USE SCHEMA {CFG.schema}")
        print(f"[00_config] Using namespace: {CFG.namespace}")
    except NameError:
        print("[00_config] spark not available — skipping schema creation (import mode).")


# Auto-run when executed as a notebook so downstream `%run ./00_config` callers land
# in the right catalog/schema context.
try:
    ensure_schema()
except Exception as e:  # pragma: no cover
    print(f"[00_config] ensure_schema skipped: {e}")

# COMMAND ----------

print(CFG)