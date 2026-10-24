# Databricks notebook source
# MAGIC %md
# MAGIC # 06a — Supervisor agent over the 2 Genie Agents (Agent Bricks CLI)
# MAGIC
# MAGIC Deploys the code-first supervisor in `06_agent/supervisor` with the **Agent Bricks
# MAGIC CLI**. It replaces the legacy Agent Bricks Supervisor Agent (MAS): Supervisor Agent
# MAGIC is a legacy offering and the Supervisor API reached end of life on Sep 30, 2026.
# MAGIC Databricks' recommended path for coordinating several Genie Agents is a custom
# MAGIC agent built with the Agent Bricks CLI.
# MAGIC
# MAGIC * **Tool `sourcing_supplier_risk`** → Genie Agent 1 (Sourcing & Supplier Risk).
# MAGIC * **Tool `purchase_order_analytics`** → Genie Agent 2 (Purchase Order Analytics).
# MAGIC * The routing rules are the agent's system prompt (`supervisor/agent/agent.py`).
# MAGIC
# MAGIC What this notebook runs, in a temp copy of the project:
# MAGIC ```
# MAGIC agentbricks tools add genie-agent <space 1> --name sourcing_supplier_risk --auth app
# MAGIC agentbricks tools add genie-agent <space 2> --name purchase_order_analytics --auth app
# MAGIC agentbricks deploy sap-supervisor --source <copy>
# MAGIC ```
# MAGIC The agent runs on **Agent Runtime** (`DurableAgentServer`) as the Databricks App
# MAGIC `agent-bricks-sap-supervisor`, with MLflow tracing. Idempotent: a re-run deploys new
# MAGIC source to the same app.

# COMMAND ----------

# MAGIC %run ../00_config

# COMMAND ----------

# MAGIC %run ./_agentbricks_helpers

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

spaces = {r["space_key"]: r["space_id"] for r in spark.table(CFG.fq("_GENIE_SPACES")).collect()}
print(f"Genie Agent 1 (sourcing/risk): {spaces['sourcing_supplier_risk']}")
print(f"Genie Agent 2 (PO analytics):  {spaces['purchase_order_analytics']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install the CLIs and bind the two Genie Agents as tools

# COMMAND ----------

env = ensure_agentbricks_cli()
PROJECT = prepare_supervisor_project(f"{demo_root()}/06_agent/supervisor", f"{WORK_DIR}/sap_supervisor_project",
                                     spaces, CFG.model, env=env)
print(open(f"{PROJECT}/agent.toml").read())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy to Agent Runtime
# MAGIC Creates (or updates) the app, provisions its Runtime Store, binds the tracing
# MAGIC experiment and grants the app's service principal CAN_RUN on both Genie Agents.
# MAGIC Takes a few minutes on the first run.

# COMMAND ----------

result = agentbricks("deploy", CFG.supervisor_name, "--source", PROJECT, env=env, cwd=PROJECT)
print(json.dumps(result, indent=1) if isinstance(result, (dict, list)) else result)

app = w.api_client.do("GET", f"/api/2.0/apps/{CFG.supervisor_app_name}")
SP = app["service_principal_client_id"]
URL = app["url"]
print(f"✓ {CFG.supervisor_app_name}: {URL}  (service principal {SP})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grant the supervisor's service principal the data behind the Genie Agents
# MAGIC The Genie tools use `auth = "app"`, and the Genie Agents run as `VIEWER`, so their
# MAGIC SQL runs as this service principal. `agentbricks deploy` only grants what
# MAGIC `agent.toml` names (the Genie Agents). The demo schema is already readable by
# MAGIC `account users` (04a). The Lakebase catalog (live mitigation cases) and the warehouse
# MAGIC are granted here.

# COMMAND ----------

for g in [f"GRANT USE CATALOG ON CATALOG {CFG.lakebase_catalog} TO `{SP}`",
          f"GRANT USE SCHEMA ON CATALOG {CFG.lakebase_catalog} TO `{SP}`",
          f"GRANT SELECT ON CATALOG {CFG.lakebase_catalog} TO `{SP}`"]:
    spark.sql(g)
    print("  ✓", g)

w.api_client.do("PATCH", f"/api/2.0/permissions/warehouses/{CFG.warehouse_id}",
                body={"access_control_list": [{"service_principal_name": SP, "permission_level": "CAN_USE"}]})
print(f"  ✓ CAN_USE on warehouse {CFG.warehouse_id}")

for key, sid in spaces.items():
    perms = w.api_client.do("GET", f"/api/2.0/permissions/genie/{sid}")
    levels = [p["permission_level"] for a in perms.get("access_control_list", [])
              if a.get("service_principal_name") == SP for p in a.get("all_permissions", [])]
    print(f"  ✓ Genie Agent {key}: {levels or 'no direct grant'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Persist the supervisor coordinates for the App, benchmark and teardown

# COMMAND ----------

from pyspark.sql import Row

spark.createDataFrame([Row(
    supervisor_name=CFG.supervisor_name, app_name=CFG.supervisor_app_name, url=URL,
    service_principal=SP, experiment=CFG.supervisor_experiment, model=CFG.model,
)]).write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(CFG.fq("_SUPERVISOR"))
display(spark.table(CFG.fq("_SUPERVISOR")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Try it
# MAGIC Apps only accept **OAuth** tokens, so call the agent from a laptop (or from the App,
# MAGIC which uses its own service principal), not from this notebook:
# MAGIC ```
# MAGIC agentbricks -p <profile> endpoint invoke agent-bricks-sap-supervisor --path /api/invocations \
# MAGIC   --json '{"id":"<uuid>","session_id":"<uuid>","input":{"messages":[{"role":"user","content":"Which materials have no multi-sourcing strategy?"}]}}'
# MAGIC ```
# MAGIC 07a runs the same agent code in-process for the accuracy benchmark.

# COMMAND ----------

dbutils.notebook.exit(f"06a_deploy_supervisor: SUCCESS — {CFG.supervisor_app_name} at {URL}")
