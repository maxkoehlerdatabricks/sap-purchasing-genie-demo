# Databricks notebook source
# MAGIC %md
# MAGIC # 04a — Resources (SEPARATE notebook) — warehouse & permissions via Python SDK
# MAGIC
# MAGIC Per the spec, infrastructure resources (SQL warehouse, permissions) live in a
# MAGIC **separate** notebook from the data pipeline. This notebook uses the **Databricks
# MAGIC Python SDK** to:
# MAGIC 1. Ensure a **serverless** SQL warehouse exists and is usable (reuse the given one,
# MAGIC    or create a small serverless warehouse if it is missing).
# MAGIC 2. Grant the required Unity Catalog privileges on the demo schema so Genie, the
# MAGIC    supervisor agent, and the App can read the gold tables, functions and metric views.
# MAGIC
# MAGIC It is safe to re-run (idempotent).

# COMMAND ----------

# MAGIC %run ../00_config

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import EndpointInfoWarehouseType, CreateWarehouseRequestWarehouseType

w = WorkspaceClient()
print(f"Workspace: {w.config.host}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1) Ensure a serverless SQL warehouse
# MAGIC We first try the configured `warehouse_id`. If it is missing, we create a small
# MAGIC **serverless** warehouse named after the demo. Serverless is preferred for DBSQL.

# COMMAND ----------

warehouse_id = CFG.warehouse_id
wh = None
try:
    wh = w.warehouses.get(id=warehouse_id)
    print(f"✓ Using existing warehouse: {wh.name} (id={wh.id}, state={wh.state})")
except Exception as e:
    print(f"Configured warehouse {warehouse_id} not found ({e}); creating a serverless one.")

if wh is None:
    created = w.warehouses.create_and_wait(
        name="sap_purchasing_serverless_wh",
        cluster_size="2X-Small",
        max_num_clusters=1,
        auto_stop_mins=10,
        enable_serverless_compute=True,
        warehouse_type=CreateWarehouseRequestWarehouseType.PRO,
    )
    warehouse_id = created.id
    print(f"✓ Created serverless warehouse id={warehouse_id}")
    print(f"  NOTE: update 00_config warehouse_id widget default to {warehouse_id} if you want this to be the demo default.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2) Grant Unity Catalog privileges on the demo schema
# MAGIC We grant USE + SELECT + EXECUTE so consumers (Genie/supervisor/App and analysts) can read
# MAGIC tables, run the trusted SQL functions and query metric views. Adjust the principal
# MAGIC to your workspace group as needed. By default we grant to `account users` (broad,
# MAGIC good for a demo). For a locked-down demo, replace with a specific group.

# COMMAND ----------

PRINCIPAL = "account users"  # change to a specific group for a locked-down demo

grants = [
    (f"GRANT USE CATALOG ON CATALOG {CFG.catalog} TO `{PRINCIPAL}`"),
    (f"GRANT USE SCHEMA ON SCHEMA {CFG.namespace} TO `{PRINCIPAL}`"),
    (f"GRANT SELECT ON SCHEMA {CFG.namespace} TO `{PRINCIPAL}`"),
    (f"GRANT EXECUTE ON SCHEMA {CFG.namespace} TO `{PRINCIPAL}`"),
]
for g in grants:
    try:
        spark.sql(g)
        print("✓", g)
    except Exception as e:
        print("⚠", g, "->", e)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3) Summary — show current grants on the schema

# COMMAND ----------

display(spark.sql(f"SHOW GRANTS ON SCHEMA {CFG.namespace}"))

# COMMAND ----------

dbutils.notebook.exit(f"04a_resources: SUCCESS — warehouse_id={warehouse_id}, grants applied.")
