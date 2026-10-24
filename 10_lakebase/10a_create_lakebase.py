# Databricks notebook source
# MAGIC %md
# MAGIC # 10a — Lakebase: the operational layer for mitigation cases
# MAGIC
# MAGIC The analytics answer *"which materials are at risk?"*. This notebook adds the
# MAGIC place where the business **acts** on that answer: a **Lakebase** (managed Postgres)
# MAGIC database that the App writes **mitigation cases** to, with millisecond reads and
# MAGIC writes, constraints and an audit trail.
# MAGIC
# MAGIC It creates (idempotent):
# MAGIC 1. A **Lakebase Autoscaling project** `sap-purchasing-ops` and the database `purchasing_ops`.
# MAGIC 2. The **OLTP schema** `ops`: `buyer`, `action_type`, `mitigation_case`, `case_event`.
# MAGIC 3. **Seed datasets** from `10_lakebase/data/*.csv` (buyers + action catalog) and three
# MAGIC    demo cases on today's single-sourced and risky materials.
# MAGIC 4. A **Unity Catalog catalog** `sap_purchasing_ops` registered over the database, so
# MAGIC    the SQL warehouse, Genie and Spark read the cases **live** (no copy).
# MAGIC 5. Two **trusted SQL functions** in the demo schema that join the governed gold layer
# MAGIC    with the live cases: `open_mitigation_cases()` and
# MAGIC    `single_sourced_risky_without_open_case()`.
# MAGIC
# MAGIC > **Demo reset:** re-running this notebook resets the cases to the 3 seed cases.
# MAGIC >
# MAGIC > **Pre-requisites:** gold tables + SQL functions (steps 1–4 in `00_run_all`).

# COMMAND ----------

# MAGIC %pip install "psycopg[binary]>=3.1" -q

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../00_config

# COMMAND ----------

import csv
import os
import time

import psycopg
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
NS = CFG.namespace
PROJECT = CFG.lakebase_project
BRANCH = f"projects/{PROJECT}/branches/production"
DB = CFG.lakebase_database
PG_SCHEMA = CFG.lakebase_pg_schema
LB_CATALOG = CFG.lakebase_catalog
ME = w.current_user.me().user_name

print(f"Lakebase project: {PROJECT}  database: {DB}  schema: {PG_SCHEMA}")
print(f"UC catalog over Lakebase: {LB_CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1) Ensure the Lakebase Autoscaling project (and wait for its endpoint)

# COMMAND ----------


def _do(method, path, body=None, params=None):
    return w.api_client.do(method, path, body=body, query=params)


try:
    _do("GET", f"/api/2.0/postgres/projects/{PROJECT}")
    print(f"  • project '{PROJECT}' already exists")
except Exception:
    # A just-purged project can briefly block re-creation; retry for a few minutes.
    for attempt in range(20):
        try:
            _do("POST", "/api/2.0/postgres/projects", params={"project_id": PROJECT},
                body={"spec": {"display_name": "SAP Purchasing Ops (mitigation cases)"}})
            print(f"  ✓ project '{PROJECT}' creation started")
            break
        except Exception as e:
            if "already exists" in str(e).lower() and attempt < 19:
                print(f"  … project name still reserved, retrying ({attempt + 1}/20)")
                time.sleep(15)
            else:
                raise

for attempt in range(60):
    try:
        eps = _do("GET", f"/api/2.0/postgres/{BRANCH}/endpoints").get("endpoints", [])
    except Exception:
        eps = []
    state = eps[0].get("status", {}).get("current_state") if eps else None
    if state == "ACTIVE":
        break
    print(f"  … endpoint state = {state}")
    time.sleep(15)
else:
    raise TimeoutError("Lakebase endpoint did not become ACTIVE in 15 minutes.")

ENDPOINT = eps[0]["name"]
PG_HOST = eps[0]["status"]["hosts"]["host"]
print(f"  ✓ endpoint ACTIVE: {PG_HOST}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2) Connect with a short-lived OAuth credential and create the database

# COMMAND ----------


def pg_connect(dbname=DB):
    """Open a Postgres connection as the current user with a fresh OAuth token."""
    token = _do("POST", "/api/2.0/postgres/credentials", body={"endpoint": ENDPOINT})["token"]
    return psycopg.connect(host=PG_HOST, dbname=dbname, user=ME, password=token,
                           sslmode="require", autocommit=True)


with pg_connect("databricks_postgres") as c:
    exists = c.execute("SELECT 1 FROM pg_database WHERE datname = %s", (DB,)).fetchone()
    if exists:
        print(f"  • database '{DB}' already exists")
    else:
        c.execute(f'CREATE DATABASE "{DB}"')
        print(f"  ✓ created database '{DB}'")

# The App's Postgres resource needs the database's resource name (generated id).
DB_RESOURCE = None
for _ in range(20):
    for d in _do("GET", f"/api/2.0/postgres/{BRANCH}/databases").get("databases", []):
        if d.get("status", {}).get("postgres_database") == DB:
            DB_RESOURCE = d["name"]
    if DB_RESOURCE:
        break
    time.sleep(5)
print(f"  ✓ database resource: {DB_RESOURCE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3) Create the OLTP schema
# MAGIC Real transactional design: foreign keys to the reference tables, CHECK constraints
# MAGIC on status/priority, and a **partial unique index** so a material+plant can have at
# MAGIC most one *active* case. Every status change is written to `case_event` (audit trail).

# COMMAND ----------

DDL = f"""
CREATE SCHEMA IF NOT EXISTS {PG_SCHEMA};

CREATE TABLE IF NOT EXISTS {PG_SCHEMA}.buyer (
  buyer_id        TEXT PRIMARY KEY,
  full_name       TEXT NOT NULL,
  email           TEXT NOT NULL,
  ekgrp           TEXT NOT NULL,
  material_group  TEXT NOT NULL,
  role            TEXT
);
COMMENT ON TABLE {PG_SCHEMA}.buyer IS 'Purchasing buyers who own mitigation cases. One row per buyer. material_group=ALL means lead buyer.';

CREATE TABLE IF NOT EXISTS {PG_SCHEMA}.action_type (
  action_code            TEXT PRIMARY KEY,
  label                  TEXT NOT NULL,
  description            TEXT,
  typical_lead_time_days INT
);
COMMENT ON TABLE {PG_SCHEMA}.action_type IS 'Catalog of mitigation actions a buyer can take on a sourcing risk.';

CREATE TABLE IF NOT EXISTS {PG_SCHEMA}.mitigation_case (
  case_id      BIGSERIAL PRIMARY KEY,
  matnr        TEXT NOT NULL,
  werks        TEXT NOT NULL,
  lifnr        TEXT NOT NULL,
  risk_score   INT,
  action_code  TEXT NOT NULL REFERENCES {PG_SCHEMA}.action_type(action_code),
  buyer_id     TEXT NOT NULL REFERENCES {PG_SCHEMA}.buyer(buyer_id),
  status       TEXT NOT NULL DEFAULT 'OPEN'
               CHECK (status IN ('OPEN','IN_PROGRESS','DONE','CANCELLED')),
  priority     TEXT NOT NULL DEFAULT 'MEDIUM' CHECK (priority IN ('HIGH','MEDIUM','LOW')),
  due_date     DATE,
  note         TEXT,
  created_by   TEXT NOT NULL,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE {PG_SCHEMA}.mitigation_case IS 'One row per mitigation case opened on a sourcing-risk material (MATNR+WERKS). Active = OPEN or IN_PROGRESS.';
CREATE UNIQUE INDEX IF NOT EXISTS ux_case_active_material
  ON {PG_SCHEMA}.mitigation_case (matnr, werks) WHERE status IN ('OPEN','IN_PROGRESS');
CREATE INDEX IF NOT EXISTS ix_case_buyer_status ON {PG_SCHEMA}.mitigation_case (buyer_id, status);

CREATE TABLE IF NOT EXISTS {PG_SCHEMA}.case_event (
  event_id     BIGSERIAL PRIMARY KEY,
  case_id      BIGINT NOT NULL REFERENCES {PG_SCHEMA}.mitigation_case(case_id) ON DELETE CASCADE,
  event_type   TEXT NOT NULL,
  from_status  TEXT,
  to_status    TEXT,
  actor        TEXT NOT NULL,
  note         TEXT,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE {PG_SCHEMA}.case_event IS 'Audit trail: one row per change on a mitigation case.';
"""

with pg_connect() as c:
    c.execute(DDL)
print(f"  ✓ schema '{PG_SCHEMA}' with 4 tables ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4) Load the seed datasets (buyers + action catalog)
# MAGIC The CSVs live next to this notebook in `data/`. Upserts keep it idempotent.

# COMMAND ----------

nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
DATA_DIR = f"/Workspace{nb_path.rsplit('/', 1)[0]}/data"
if not os.path.isdir(DATA_DIR):  # notebook path may already start with /Workspace
    DATA_DIR = f"{nb_path.rsplit('/', 1)[0]}/data"


def read_csv(name):
    with open(f"{DATA_DIR}/{name}", newline="") as f:
        return list(csv.DictReader(f))


buyers = read_csv("buyers.csv")
actions = read_csv("action_types.csv")

with pg_connect() as c:
    with c.cursor() as cur:
        cur.executemany(
            f"""INSERT INTO {PG_SCHEMA}.buyer VALUES (%(buyer_id)s,%(full_name)s,%(email)s,%(ekgrp)s,%(material_group)s,%(role)s)
                ON CONFLICT (buyer_id) DO UPDATE SET full_name=EXCLUDED.full_name, email=EXCLUDED.email,
                  ekgrp=EXCLUDED.ekgrp, material_group=EXCLUDED.material_group, role=EXCLUDED.role""",
            buyers)
        cur.executemany(
            f"""INSERT INTO {PG_SCHEMA}.action_type VALUES (%(action_code)s,%(label)s,%(description)s,%(typical_lead_time_days)s)
                ON CONFLICT (action_code) DO UPDATE SET label=EXCLUDED.label, description=EXCLUDED.description,
                  typical_lead_time_days=EXCLUDED.typical_lead_time_days""",
            actions)
print(f"  ✓ loaded {len(buyers)} buyers and {len(actions)} action types from {DATA_DIR}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5) Reset the cases to the demo seed state
# MAGIC Three cases on today's single-sourced and risky materials, owned by the buyer for
# MAGIC the material group. Everything else on the worklist is still **without** a case,
# MAGIC which is what the closed-loop Genie question surfaces.

# COMMAND ----------

risky = spark.sql(f"""
    SELECT r.MATNR, r.WERKS, r.LIFNR, r.RISK_SCORE, m.MATKL
    FROM {NS}.single_sourced_risky_today() r
    JOIN {NS}.MATERIAL m ON m.MATNR = r.MATNR AND m.WERKS = r.WERKS
    ORDER BY r.RISK_SCORE DESC, r.MATNR, r.WERKS
""").collect()
print(f"Single-sourced and risky today: {len(risky)} material/plant combinations")

buyer_by_group = {b["material_group"]: b["buyer_id"] for b in buyers}
SEED = [  # (status, action, priority, due in days, note)
    ("IN_PROGRESS", "QUALIFY_SECOND_SOURCE", "HIGH", 75,
     "Second vendor identified. PPAP samples requested."),
    ("OPEN", "RAISE_SAFETY_STOCK", "HIGH", 7,
     "Bridge supply until the vendor's risk review is closed."),
    ("OPEN", "SUPPLIER_DEVELOPMENT", "MEDIUM", 45,
     "Capacity audit with the supplier scheduled."),
]

with pg_connect() as c:
    c.execute(f"TRUNCATE {PG_SCHEMA}.case_event, {PG_SCHEMA}.mitigation_case RESTART IDENTITY")
    for r, (status, action, prio, due, note) in zip(risky, SEED):
        buyer = buyer_by_group.get(r["MATKL"], "B006")
        case_id = c.execute(
            f"""INSERT INTO {PG_SCHEMA}.mitigation_case
                  (matnr, werks, lifnr, risk_score, action_code, buyer_id, status, priority, due_date, note, created_by, created_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s, current_date + %s, %s, 'seed', now() - interval '3 days')
                RETURNING case_id""",
            (r["MATNR"], r["WERKS"], r["LIFNR"], r["RISK_SCORE"], action, buyer, status, prio, due, note),
        ).fetchone()[0]
        c.execute(f"""INSERT INTO {PG_SCHEMA}.case_event (case_id, event_type, to_status, actor, note, created_at)
                      VALUES (%s, 'CREATED', 'OPEN', 'seed', %s, now() - interval '3 days')""", (case_id, note))
        if status != "OPEN":
            c.execute(f"""INSERT INTO {PG_SCHEMA}.case_event (case_id, event_type, from_status, to_status, actor)
                          VALUES (%s, 'STATUS_CHANGE', 'OPEN', %s, 'seed')""", (case_id, status))
    n = c.execute(f"SELECT count(*) FROM {PG_SCHEMA}.mitigation_case").fetchone()[0]
print(f"  ✓ {n} seed cases written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6) Register the Lakebase database as a Unity Catalog catalog
# MAGIC This is what closes the loop: the warehouse (and therefore Genie and the supervisor agent) can
# MAGIC query `sap_purchasing_ops.ops.mitigation_case` **live**, governed by UC.

# COMMAND ----------

try:
    _do("GET", f"/api/2.0/postgres/catalogs/{LB_CATALOG}")
    print(f"  • UC catalog '{LB_CATALOG}' already registered")
except Exception:
    _do("POST", "/api/2.0/postgres/catalogs", params={"catalog_id": LB_CATALOG},
        body={"spec": {"postgres_database": DB, "branch": BRANCH}})
    print(f"  ✓ UC catalog '{LB_CATALOG}' registration started")

for attempt in range(40):
    try:
        spark.sql(f"SELECT count(*) FROM {LB_CATALOG}.{PG_SCHEMA}.mitigation_case").collect()
        break
    except Exception as e:
        if attempt == 39:
            raise
        print(f"  … waiting for catalog to become queryable ({str(e)[:80]})")
        time.sleep(15)
print(f"  ✓ {LB_CATALOG}.{PG_SCHEMA}.mitigation_case is queryable from Spark/SQL")

for g in [f"GRANT USE CATALOG ON CATALOG {LB_CATALOG} TO `account users`",
          f"GRANT USE SCHEMA ON CATALOG {LB_CATALOG} TO `account users`",
          f"GRANT SELECT ON CATALOG {LB_CATALOG} TO `account users`"]:
    try:
        spark.sql(g)
        print("  ✓", g)
    except Exception as e:
        print("  ⚠", g, "->", str(e)[:150])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7) Trusted SQL functions over gold + live cases
# MAGIC Registered as certified answers in Genie space 1 (see `05a`). They read the cases
# MAGIC through the UC catalog, so a case created in the App is visible to Genie at once.

# COMMAND ----------

CASES = f"{LB_CATALOG}.{PG_SCHEMA}.mitigation_case"

spark.sql(f"""
CREATE OR REPLACE FUNCTION {NS}.open_mitigation_cases()
RETURNS TABLE (case_id BIGINT, MATNR STRING, WERKS STRING, LIFNR STRING, action_code STRING,
               buyer_id STRING, status STRING, priority STRING, due_date DATE, days_open INT)
COMMENT 'Active mitigation cases (status OPEN or IN_PROGRESS), read live from Lakebase. One row per case.'
RETURN
  SELECT case_id, matnr, werks, lifnr, action_code, buyer_id, status, priority, due_date,
         CAST(datediff(current_date(), CAST(created_at AS DATE)) AS INT)
  FROM {CASES}
  WHERE status IN ('OPEN', 'IN_PROGRESS')
""")

spark.sql(f"""
CREATE OR REPLACE FUNCTION {NS}.single_sourced_risky_without_open_case()
RETURNS TABLE (MATNR STRING, WERKS STRING, LIFNR STRING, RISK_SCORE INT)
COMMENT 'Single-sourced materials whose sole supplier is at delivery risk today AND that have no active mitigation case (OPEN or IN_PROGRESS) in Lakebase. The unmitigated sourcing risk.'
RETURN
  SELECT r.MATNR, r.WERKS, r.LIFNR, r.RISK_SCORE
  FROM {NS}.single_sourced_risky_today() r
  WHERE NOT EXISTS (
    SELECT 1 FROM {CASES} c
    WHERE c.matnr = r.MATNR AND c.werks = r.WERKS AND c.status IN ('OPEN', 'IN_PROGRESS'))
""")

for fn in ["open_mitigation_cases", "single_sourced_risky_without_open_case"]:
    n = spark.sql(f"SELECT count(*) AS n FROM {NS}.{fn}()").first()["n"]
    print(f"  ✓ {fn}() → {n} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8) Record the Lakebase coordinates for the App deploy and teardown

# COMMAND ----------

from pyspark.sql import Row

spark.createDataFrame([Row(project=PROJECT, branch=BRANCH, endpoint=ENDPOINT, host=PG_HOST,
                           database=DB, database_resource=DB_RESOURCE, pg_schema=PG_SCHEMA,
                           uc_catalog=LB_CATALOG)]) \
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(CFG.fq("_LAKEBASE"))
display(spark.table(CFG.fq("_LAKEBASE")))

dbutils.notebook.exit(f"10a_create_lakebase: SUCCESS — {PROJECT}/{DB}, UC catalog {LB_CATALOG}, {n} unmitigated risks")
