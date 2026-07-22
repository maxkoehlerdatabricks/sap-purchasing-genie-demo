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

# MAGIC %md
# MAGIC ## Ensure a recent Databricks SDK
# MAGIC The serverless notebook runtime can ship an older `databricks-sdk` that predates
# MAGIC the Apps API (`databricks.sdk.service.apps`). Upgrade it in-notebook so the deploy
# MAGIC works on a blank workspace, then restart Python to pick up the new version.

# COMMAND ----------

# MAGIC %pip install --upgrade "databricks-sdk>=0.33.0"

# COMMAND ----------

dbutils.library.restartPython()

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

# MAGIC %md
# MAGIC ## Grant the App's service principal CAN_RUN on the Genie spaces
# MAGIC The MAS/Genie spaces run as `VIEWER`, so the **caller's** identity is used for the
# MAGIC query. When the chat tab runs, the caller is the App's service principal — it must
# MAGIC have at least CAN_RUN on each Genie space, otherwise the chat returns a permission
# MAGIC error. (Schema SELECT/EXECUTE are already granted in 04a via `account users`.)

# COMMAND ----------

sp_client_id = app.service_principal_client_id
print(f"App service principal: {sp_client_id}")

space_ids = [r["space_id"] for r in spark.table(CFG.fq("_GENIE_SPACES")).collect()]
for sid in space_ids:
    try:
        # Resolve the numeric permissions object id, then grant CAN_RUN to the App SP.
        g = w.api_client.do("GET", f"/api/2.0/permissions/genie/{sid}")
        numeric = g["object_id"].split("/")[-1]
        w.api_client.do("PATCH", f"/api/2.0/permissions/genie/{numeric}",
                        body={"access_control_list": [
                            {"service_principal_name": sp_client_id, "permission_level": "CAN_RUN"}]})
        print(f"  ✓ granted CAN_RUN on Genie space {sid}")
    except Exception as e:
        print(f"  ⚠ could not grant on {sid}: {e}")

# COMMAND ----------

dbutils.notebook.exit(f"deploy_app: SUCCESS — {APP_NAME} deployed and Genie access granted.")
