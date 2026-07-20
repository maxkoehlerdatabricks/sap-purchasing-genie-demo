# Databricks notebook source
# MAGIC %md
# MAGIC # 08a — Create the serverless data-prep Job (SEPARATE notebook, Python SDK)
# MAGIC
# MAGIC Registers a Databricks **Job** that runs the whole data-prep pipeline
# MAGIC (raw → gold → semantics) on **serverless** compute. The job simply invokes the
# MAGIC top-level `00_run_all` orchestrator notebook, so the job and the manual run stay
# MAGIC in lock-step — one source of truth.
# MAGIC
# MAGIC Uses the Databricks Python SDK. Idempotent: if a job with the same name exists it
# MAGIC is updated (reset) rather than duplicated.

# COMMAND ----------

# MAGIC %run ../00_config

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

w = WorkspaceClient()

# Absolute workspace path to the orchestrator notebook.
BASE = "/Workspace/Users/max.kohler@databricks.com/sap-purchasing-genie-demo"
ORCHESTRATOR = f"{BASE}/00_run_all"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define the serverless notebook task
# MAGIC No `existing_cluster_id` / `new_cluster` is set → the task runs on **serverless**
# MAGIC compute. Parameters are forwarded so the job is fully parameterized too.

# COMMAND ----------

task = jobs.Task(
    task_key="data_prep",
    notebook_task=jobs.NotebookTask(
        notebook_path=ORCHESTRATOR,
        base_parameters={
            "catalog": CFG.catalog,
            "schema": CFG.schema,
            "warehouse_id": CFG.warehouse_id,
        },
    ),
    # No compute block → serverless.
    timeout_seconds=7200,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create or update the job (idempotent by name)

# COMMAND ----------

existing = [j for j in w.jobs.list(name=CFG.job_name)]
if existing:
    job_id = existing[0].job_id
    w.jobs.reset(job_id=job_id, new_settings=jobs.JobSettings(
        name=CFG.job_name,
        tasks=[task],
        tags={"demo": "sap-purchasing-genie"},
        max_concurrent_runs=1,
    ))
    print(f"✓ Updated existing job '{CFG.job_name}' (job_id={job_id})")
else:
    created = w.jobs.create(
        name=CFG.job_name,
        tasks=[task],
        tags={"demo": "sap-purchasing-genie"},
        max_concurrent_runs=1,
    )
    job_id = created.job_id
    print(f"✓ Created job '{CFG.job_name}' (job_id={job_id})")

print(f"Open it in the UI: {w.config.host}/jobs/{job_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## (Optional) Trigger a run now
# MAGIC Uncomment to kick off the data-prep job immediately and wait for completion.

# COMMAND ----------

# run = w.jobs.run_now_and_wait(job_id=job_id)
# print(f"Run finished with state: {run.state.result_state}")

# COMMAND ----------

dbutils.notebook.exit(f"08a_create_jobs: SUCCESS — job_id={job_id}")
