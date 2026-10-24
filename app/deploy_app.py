# Databricks notebook source
# MAGIC %md
# MAGIC # app/deploy_app — Create & deploy the Databricks App (Python SDK)
# MAGIC
# MAGIC Creates the App (if missing), wires its resources (SQL warehouse, Lakebase
# MAGIC database), grants its service principal access to Lakebase and CAN_USE on the
# MAGIC supervisor agent app, and deploys the source from this demo folder. Runs inside
# MAGIC `00_run_all` after `05a` (Genie), `06a` (supervisor) and `10a`/`10b` (Lakebase).
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
    AppResourceSqlWarehouseSqlWarehousePermission, AppResourcePostgres, \
    AppResourcePostgresPostgresPermission, EnvVar

w = WorkspaceClient()

APP_NAME = CFG.app_name
APP_SOURCE = f"{demo_root()}/app"

# Look up the supervisor app recorded by 06a and the Lakebase coordinates recorded by 10a.
SUPERVISOR = spark.table(CFG.fq("_SUPERVISOR")).first().asDict()
LB = spark.table(CFG.fq("_LAKEBASE")).first().asDict()
print(f"App: {APP_NAME}  | source: {APP_SOURCE}")
print(f"warehouse: {CFG.warehouse_id}  | supervisor: {SUPERVISOR['url']}  | Lakebase: {LB['database_resource']}")

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

from databricks.sdk.errors import NotFound

existing = None
try:
    existing = w.apps.get(name=APP_NAME)
except Exception:
    pass

# An app whose service principal was deleted (e.g. by a workspace cleanup) can never
# get database or Genie access again. Recreate it so it gets a fresh SP.
if existing is not None and existing.service_principal_id:
    try:
        w.service_principals.get(id=str(existing.service_principal_id))
    except NotFound:
        print(f"App SP {existing.service_principal_client_id} no longer exists; recreating the app…")
        w.apps.delete(name=APP_NAME)
        for _ in range(60):
            try:
                w.apps.get(name=APP_NAME)
                time.sleep(5)
            except Exception:
                break
        existing = None

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
# MAGIC The chat tab does not need UC grants here: the supervisor agent asks the Genie
# MAGIC Agents as its own service principal, which 06a grants the Lakebase catalog.

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy the source code with the runtime environment from `00_config`

# COMMAND ----------

from databricks.sdk.service.apps import AppDeployment

env = [
    EnvVar(name="DATABRICKS_WAREHOUSE_ID", value_from="sql-warehouse"),
    EnvVar(name="SUPERVISOR_URL", value=SUPERVISOR["url"]),
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

# MAGIC %md
# MAGIC ## Let the App's service principal call the supervisor agent
# MAGIC The chat tab calls `agent-bricks-sap-supervisor` with the App SP's OAuth token, so
# MAGIC the SP needs CAN_USE on that app.

# COMMAND ----------

w.api_client.do("PATCH", f"/api/2.0/permissions/apps/{SUPERVISOR['app_name']}",
                body={"access_control_list": [{"service_principal_name": SP, "permission_level": "CAN_USE"}]})
print(f"  ✓ CAN_USE on {SUPERVISOR['app_name']} for {SP}")

# COMMAND ----------

dbutils.notebook.exit(f"deploy_app: SUCCESS — {APP_NAME} deployed ({app.url}), Lakebase + supervisor access granted.")
