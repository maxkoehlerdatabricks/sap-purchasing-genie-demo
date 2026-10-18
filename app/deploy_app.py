# Databricks notebook source
# MAGIC %md
# MAGIC # app/deploy_app — Create & deploy the Databricks App (Python SDK)
# MAGIC
# MAGIC Creates the App (if missing), wires its resources (SQL warehouse, MAS serving
# MAGIC endpoint, Lakebase database), grants its service principal access to Lakebase and
# MAGIC the Genie spaces, and deploys the source from this demo folder. Runs inside
# MAGIC `00_run_all` after `05a` (Genie), `06a` (MAS) and `10a`/`10b` (Lakebase).
# MAGIC
# MAGIC All runtime settings (catalog, schema, Lakebase coordinates) are passed as
# MAGIC deployment environment variables from `00_config`, so `app.yaml` never needs edits.
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

# MAGIC %pip install --upgrade databricks-sdk "psycopg[binary]>=3.1" -q

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../00_config

# COMMAND ----------

import time

import psycopg
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.apps import App, AppResource, AppResourceSqlWarehouse, \
    AppResourceServingEndpoint, AppResourceSqlWarehouseSqlWarehousePermission, \
    AppResourceServingEndpointServingEndpointPermission, AppResourcePostgres, \
    AppResourcePostgresPostgresPermission, EnvVar

w = WorkspaceClient()

APP_NAME = CFG.app_name
APP_SOURCE = f"{demo_root()}/app"

# Look up the MAS endpoint recorded by 06a and the Lakebase coordinates recorded by 10a.
mas_row = spark.table(CFG.fq("_MAS_ENDPOINT")).first()
MAS_ENDPOINT = mas_row["endpoint_name"]
LB = spark.table(CFG.fq("_LAKEBASE")).first().asDict()
print(f"App: {APP_NAME}  | source: {APP_SOURCE}")
print(f"warehouse: {CFG.warehouse_id}  | MAS endpoint: {MAS_ENDPOINT}  | Lakebase: {LB['database_resource']}")

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
    # Lakebase database for the mitigation cases (creates the SP's Postgres role).
    AppResource(
        name="lakebase",
        postgres=AppResourcePostgres(
            branch=LB["branch"],
            database=LB["database_resource"],
            permission=AppResourcePostgresPostgresPermission.CAN_CONNECT_AND_CREATE,
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
# MAGIC ## Grant the App's service principal access to the Lakebase data
# MAGIC The `postgres` resource lets the SP connect. Table privileges are granted here by
# MAGIC the schema owner: read/write on the case tables, read on the synced worklist.
# MAGIC The SP also gets UC `SELECT` on the Lakebase catalog, because the chat tab runs
# MAGIC Genie as the SP and the case functions read that catalog.

# COMMAND ----------

app = w.apps.get(name=APP_NAME)
for _ in range(30):
    if app.service_principal_client_id:
        break
    time.sleep(5)
    app = w.apps.get(name=APP_NAME)
SP = app.service_principal_client_id
print(f"App service principal: {SP}")

token = w.api_client.do("POST", "/api/2.0/postgres/credentials", body={"endpoint": LB["endpoint"]})["token"]
ME = w.current_user.me().user_name
OPS, SYNC = LB["pg_schema"], CFG.sync_schema
with psycopg.connect(host=LB["host"], dbname=LB["database"], user=ME, password=token,
                     sslmode="require", autocommit=True) as c:
    has_role = c.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (SP,)).fetchone()
    if not has_role:  # fallback if the resource has not provisioned the role yet
        c.execute("CREATE EXTENSION IF NOT EXISTS databricks_auth")
        c.execute("SELECT databricks_create_role(%s, 'service_principal')", (SP,))
        print("  ✓ created Postgres role for the SP")
    for g in [f'GRANT USAGE ON SCHEMA {OPS} TO "{SP}"',
              f'GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA {OPS} TO "{SP}"',
              f'GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA {OPS} TO "{SP}"',
              f'GRANT USAGE ON SCHEMA {SYNC} TO "{SP}"',
              f'GRANT SELECT ON ALL TABLES IN SCHEMA {SYNC} TO "{SP}"']:
        c.execute(g)
        print("  ✓", g)

for g in [f"GRANT USE CATALOG ON CATALOG {CFG.lakebase_catalog} TO `{SP}`",
          f"GRANT USE SCHEMA ON CATALOG {CFG.lakebase_catalog} TO `{SP}`",
          f"GRANT SELECT ON CATALOG {CFG.lakebase_catalog} TO `{SP}`"]:
    spark.sql(g)
    print("  ✓", g)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy the source code with the runtime environment from `00_config`

# COMMAND ----------

from databricks.sdk.service.apps import AppDeployment

env = [
    EnvVar(name="DATABRICKS_WAREHOUSE_ID", value_from="sql-warehouse"),
    EnvVar(name="MAS_ENDPOINT_NAME", value_from="mas-endpoint"),
    EnvVar(name="DEMO_CATALOG", value=CFG.catalog),
    EnvVar(name="DEMO_SCHEMA", value=CFG.schema),
    EnvVar(name="LAKEBASE_HOST", value=LB["host"]),
    EnvVar(name="LAKEBASE_ENDPOINT", value=LB["endpoint"]),
    EnvVar(name="LAKEBASE_DATABASE", value=LB["database"]),
    EnvVar(name="LAKEBASE_SCHEMA", value=LB["pg_schema"]),
    EnvVar(name="LAKEBASE_SYNC_SCHEMA", value=CFG.sync_schema),
]
dep = w.apps.deploy_and_wait(app_name=APP_NAME,
                             app_deployment=AppDeployment(source_code_path=APP_SOURCE, env_vars=env))
print(f"Deployment status: {dep.status.state if dep.status else 'unknown'}")

app = w.apps.get(name=APP_NAME)
print(f"App URL: {app.url}")

# COMMAND ----------

# MAGIC ## Grant the App's service principal CAN_RUN on the Genie spaces
# MAGIC The MAS/Genie spaces run as `VIEWER`, so the **caller's** identity is used for the
# MAGIC query. When the chat tab runs, the caller is the App's service principal — it must
# MAGIC have at least CAN_RUN on each Genie space, otherwise the chat returns a permission
# MAGIC error. (Schema SELECT/EXECUTE are already granted in 04a via `account users`.)

# COMMAND ----------

sp_client_id = SP

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

dbutils.notebook.exit(f"deploy_app: SUCCESS — {APP_NAME} deployed ({app.url}), Lakebase + Genie access granted.")
