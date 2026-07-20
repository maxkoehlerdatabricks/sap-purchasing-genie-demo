# Databricks notebook source
# MAGIC %md
# MAGIC # app/deploy_app — Create & deploy the Databricks App (Python SDK)
# MAGIC
# MAGIC Creates the App (if missing), wires its resources (SQL warehouse + MAS serving
# MAGIC endpoint), and deploys the source from the workspace. Run this after `00_run_all`,
# MAGIC `05a` (Genie) and `06a` (MAS) have completed.
# MAGIC
# MAGIC > The frontend is shipped pre-built in `frontend/dist/` (no Node build required).
# MAGIC > To use the React source instead, run `npm run build` in `app/frontend` first.

# COMMAND ----------

# MAGIC %run ../00_config

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.apps import App, AppResource, AppResourceSqlWarehouse, \
    AppResourceServingEndpoint, AppResourceSqlWarehouseSqlWarehousePermission, \
    AppResourceServingEndpointServingEndpointPermission

w = WorkspaceClient()

APP_NAME = CFG.app_name
APP_SOURCE = "/Workspace/Users/max.kohler@databricks.com/sap-purchasing-genie-demo/app"

# Look up the MAS endpoint recorded by 06a.
mas_row = spark.table(CFG.fq("_MAS_ENDPOINT")).first()
MAS_ENDPOINT = mas_row["endpoint_name"]
print(f"App: {APP_NAME}  | warehouse: {CFG.warehouse_id}  | MAS endpoint: {MAS_ENDPOINT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the app with resources (idempotent)

# COMMAND ----------

resources = [
    AppResource(
        name="sql-warehouse",
        sql_warehouse=AppResourceSqlWarehouse(
            id=CFG.warehouse_id,
            permission=AppResourceSqlWarehouseSqlWarehousePermission.CAN_USE,
        ),
    ),
    AppResource(
        name="mas-endpoint",
        serving_endpoint=AppResourceServingEndpoint(
            name=MAS_ENDPOINT,
            permission=AppResourceServingEndpointServingEndpointPermission.CAN_QUERY,
        ),
    ),
]

existing = None
try:
    existing = w.apps.get(name=APP_NAME)
except Exception:
    pass

if existing is None:
    print("Creating app…")
    w.apps.create_and_wait(app=App(name=APP_NAME, description="SAP Purchasing sourcing-risk & PO analytics demo.",
                                   resources=resources))
else:
    print("App exists; updating resources…")
    w.apps.update(name=APP_NAME, app=App(name=APP_NAME,
                                         description="SAP Purchasing sourcing-risk & PO analytics demo.",
                                         resources=resources))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy the source code

# COMMAND ----------

from databricks.sdk.service.apps import AppDeployment

dep = w.apps.deploy_and_wait(app_name=APP_NAME,
                             app_deployment=AppDeployment(source_code_path=APP_SOURCE))
print(f"Deployment status: {dep.status.state if dep.status else 'unknown'}")

app = w.apps.get(name=APP_NAME)
print(f"App URL: {app.url}")

# COMMAND ----------

dbutils.notebook.exit(f"deploy_app: SUCCESS — {APP_NAME} deployed.")
