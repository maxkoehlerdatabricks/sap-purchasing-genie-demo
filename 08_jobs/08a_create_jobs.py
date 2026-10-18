# Databricks notebook source
# MAGIC %md
# MAGIC # 08a — Create the Lakeflow Job: raw → gold → quality gate → semantics → Lakebase
# MAGIC
# MAGIC Registers one **Lakeflow Job** (serverless) that refreshes everything the business
# MAGIC sees, as a graph of tasks instead of one big notebook:
# MAGIC
# MAGIC ```
# MAGIC simulate_raw_sap
# MAGIC   ├─ gold_material ─┬─ gold_sourcing ─────────┐
# MAGIC   └─ gold_supplier ─┼─ gold_supplier_risk ────┼─ constraints_comments ─ data_quality_gate ─ dq_passed?
# MAGIC                     └─ gold_purchase_order ───┘                                             │ true
# MAGIC                     ┌──────────────────────────────┬────────────────────────────────────────┤
# MAGIC              sql_functions                  metric_views                          value_dictionaries
# MAGIC                     └──────────────────────────────┴──────── sync_lakebase_worklist ────────┘
# MAGIC ```
# MAGIC
# MAGIC What it shows:
# MAGIC * **Job parameters** (`catalog`, `schema`, `warehouse_id`) flow into every task.
# MAGIC * **Parallel fan-out / fan-in** along the real dependencies of the gold build.
# MAGIC * A **data-quality gate** (`08b`) plus a **condition task**: if a check fails, the
# MAGIC   semantic layer and Lakebase are not refreshed (downstream tasks are skipped).
# MAGIC * The last task lands the results where the business acts: the **metric views** and
# MAGIC   the **Lakebase** risk worklist (`10b`).
# MAGIC * A daily **schedule** (created paused) and serverless compute (no cluster config).
# MAGIC
# MAGIC Idempotent: a job with the same name is reset, not duplicated. The earlier
# MAGIC single-task job `sap_purchasing_data_prep` is removed if present.

# COMMAND ----------

# MAGIC %run ../00_config

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

w = WorkspaceClient()

# Run the job once after creating it and wait for the result (validates the graph).
RUN_NOW = True

ROOT = demo_root()
print(f"Demo folder: {ROOT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define the task graph

# COMMAND ----------


def nb(key, path, depends_on=(), outcome=None, timeout=3600):
    """A serverless notebook task (no compute block) reading the job parameters."""
    deps = [jobs.TaskDependency(task_key=d, outcome=outcome) for d in depends_on]
    return jobs.Task(task_key=key, depends_on=deps or None, timeout_seconds=timeout,
                     notebook_task=jobs.NotebookTask(notebook_path=f"{ROOT}/{path}", source=jobs.Source.WORKSPACE))


tasks = [
    nb("simulate_raw_sap", "_helper/01_simulate_raw_tables"),
    nb("gold_material", "02_build_gold/02a_material", ["simulate_raw_sap"]),
    nb("gold_supplier", "02_build_gold/02b_supplier", ["simulate_raw_sap"]),
    nb("gold_sourcing", "02_build_gold/02c_sourcing", ["gold_material", "gold_supplier"]),
    nb("gold_supplier_risk", "02_build_gold/02d_supplier_risk", ["gold_supplier"]),
    nb("gold_purchase_order", "02_build_gold/02e_purchase_order", ["gold_material", "gold_supplier"]),
    nb("constraints_comments", "02_build_gold/02f_constraints_comments",
       ["gold_sourcing", "gold_supplier_risk", "gold_purchase_order"]),
    nb("data_quality_gate", "08_jobs/08b_data_quality_gate", ["constraints_comments"]),
    jobs.Task(task_key="dq_passed", depends_on=[jobs.TaskDependency(task_key="data_quality_gate")],
              condition_task=jobs.ConditionTask(op=jobs.ConditionTaskOp.EQUAL_TO,
                                                left="{{tasks.data_quality_gate.values.dq_passed}}",
                                                right="true")),
    nb("sql_functions", "03_semantics/03a_sql_functions", ["dq_passed"], outcome="true"),
    nb("metric_views", "03_semantics/03b_metric_views", ["dq_passed"], outcome="true"),
    nb("value_dictionaries", "03_semantics/03c_value_dictionaries", ["dq_passed"], outcome="true"),
    nb("sync_lakebase_worklist", "10_lakebase/10b_sync_worklist",
       ["sql_functions", "metric_views", "value_dictionaries"]),
]

settings = dict(
    name=CFG.job_name,
    description=("SAP purchasing demo pipeline: simulate SAP extracts, build the governed gold layer, "
                 "gate on data quality, refresh SQL functions + metric views, and sync the sourcing-risk "
                 "worklist into Lakebase for the App."),
    tasks=tasks,
    parameters=[jobs.JobParameterDefinition(name="catalog", default=CFG.catalog),
                jobs.JobParameterDefinition(name="schema", default=CFG.schema),
                jobs.JobParameterDefinition(name="warehouse_id", default=CFG.warehouse_id)],
    schedule=jobs.CronSchedule(quartz_cron_expression="0 0 6 * * ?", timezone_id="Europe/Berlin",
                               pause_status=jobs.PauseStatus.PAUSED),
    queue=jobs.QueueSettings(enabled=True),
    max_concurrent_runs=1,
    tags={"demo": "sap-purchasing-genie"},
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create or update the job (idempotent by name)

# COMMAND ----------

existing = list(w.jobs.list(name=CFG.job_name))
if existing:
    job_id = existing[0].job_id
    w.jobs.reset(job_id=job_id, new_settings=jobs.JobSettings(**settings))
    print(f"✓ Updated job '{CFG.job_name}' (job_id={job_id})")
else:
    job_id = w.jobs.create(**settings).job_id
    print(f"✓ Created job '{CFG.job_name}' (job_id={job_id})")

# The earlier single-task job wrapped 00_run_all; the Lakeflow job replaces it.
for old in w.jobs.list(name=CFG.legacy_job_name):
    w.jobs.delete(job_id=old.job_id)
    print(f"✓ Removed legacy job '{CFG.legacy_job_name}' (job_id={old.job_id})")

print(f"Open it in the UI: {w.config.host}/jobs/{job_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run it once and wait
# MAGIC Gives the demo a completed run to show (task graph, durations, the DQ branch).

# COMMAND ----------

result = "not run"
if RUN_NOW:
    run = w.jobs.run_now_and_wait(job_id=job_id, job_parameters={
        "catalog": CFG.catalog, "schema": CFG.schema, "warehouse_id": CFG.warehouse_id})
    states = {t.task_key: str(t.state.result_state).split(".")[-1] for t in run.tasks}
    for k, v in states.items():
        print(f"  {k:<24} {v}")
    result = str(run.state.result_state).split(".")[-1]
    print(f"Run {run.run_id}: {result}  → {run.run_page_url}")
    if result != "SUCCESS":
        raise RuntimeError(f"Lakeflow job run ended with {result}: {states}")

# COMMAND ----------

dbutils.notebook.exit(f"08a_create_jobs: SUCCESS — job_id={job_id}, run={result}")
