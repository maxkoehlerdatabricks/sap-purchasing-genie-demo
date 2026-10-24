# Databricks notebook source
# MAGIC %md
# MAGIC # 07a — Benchmark: representative questions + accuracy score
# MAGIC
# MAGIC Runs a benchmark of representative business questions (the 11 spec questions +
# MAGIC extras) with **known-good SQL** and computes an accuracy score.
# MAGIC
# MAGIC Two levels of evaluation:
# MAGIC 1. **Known-good SQL sanity** — every known-good SQL must execute and return a
# MAGIC    non-empty / valid result. This validates the gold layer + trusted functions.
# MAGIC 2. **Supervisor accuracy** *(optional, on by default)* — ask each NL question to
# MAGIC    the Agent Bricks supervisor (06a), take the SQL the Genie Agent ran, re-run it,
# MAGIC    and compare the result **signature** (row count or scalar) against the
# MAGIC    known-good SQL. Also checks **routing**: did the supervisor ask the Genie Agent
# MAGIC    that owns the question (`space` in the YAML)? Prints a per-question pass/fail
# MAGIC    table and an overall accuracy percentage.
# MAGIC
# MAGIC The supervisor runs **in-process** (same graph, system prompt, Genie tools and model
# MAGIC as the deployed app), under this notebook's identity. Calling the deployed URL is not
# MAGIC possible from a notebook: Databricks Apps only accept OAuth tokens.
# MAGIC
# MAGIC The question set lives in `benchmark_questions.yaml` (single source of truth).

# COMMAND ----------

# MAGIC %run ../00_config

# COMMAND ----------

# MAGIC %run ../06_agent/_agentbricks_helpers

# COMMAND ----------

import os

# Toggle the (slower) live supervisor evaluation. Known-good SQL always runs.
RUN_AGENT_EVAL = True

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load the benchmark question set (YAML)

# COMMAND ----------

try:
    import yaml
except ImportError:
    # Serverless usually has pyyaml; install just in case.
    os.system("pip install pyyaml -q")
    import yaml

# The YAML sits next to this notebook in the workspace.
nb_dir = os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())
yaml_path = f"/Workspace{nb_dir}/benchmark_questions.yaml"
with open(yaml_path) as f:
    QUESTIONS = yaml.safe_load(f)

print(f"Loaded {len(QUESTIONS)} benchmark questions from {yaml_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper: run SQL on the gold layer and compute a result signature

# COMMAND ----------

spark.sql(f"USE {CFG.namespace}")


def run_sql_signature(sql, signature):
    """Execute SQL and return (signature_value, rowcount, first_scalar)."""
    df = spark.sql(sql)
    rows = df.collect()
    rowcount = len(rows)
    scalar = rows[0][0] if rows and len(rows[0]) > 0 else None
    return (scalar if signature == "scalar" else rowcount), rowcount, scalar


# COMMAND ----------

# MAGIC %md
# MAGIC ## Level 1 — known-good SQL sanity check

# COMMAND ----------

known_good = {}
sql_ok = 0
for q in QUESTIONS:
    try:
        sig, rc, sc = run_sql_signature(q["known_good_sql"], q["signature"])
        known_good[q["id"]] = {"signature": q["signature"], "value": sig, "rowcount": rc, "scalar": sc}
        ok = (q["signature"] == "scalar") or (rc > 0)
        sql_ok += 1 if ok else 0
        print(f"  {q['id']:<5} sig={q['signature']:<8} value={sig}  ({'OK' if ok else 'EMPTY'})")
    except Exception as e:
        known_good[q["id"]] = {"error": str(e)}
        print(f"  {q['id']:<5} SQL ERROR: {e}")

print(f"\nKnown-good SQL: {sql_ok}/{len(QUESTIONS)} produced valid results.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ask the supervisor every question (in-process)
# MAGIC Prepares the same project copy 06a deploys (Genie Agent tools bound), installs its
# MAGIC dependencies into a separate folder and runs `eval/ask.py` in a subprocess with them. Each answer is
# MAGIC the turn's message list, the same shape as the deployed `/api/invocations` output.

# COMMAND ----------

import json
import subprocess
import sys

answers = {}
if RUN_AGENT_EVAL:
    spaces = {r["space_key"]: r["space_id"] for r in spark.table(CFG.fq("_GENIE_SPACES")).collect()}
    env = ensure_agentbricks_cli()
    project = prepare_supervisor_project(f"{demo_root()}/06_agent/supervisor", f"{WORK_DIR}/sap_supervisor_eval",
                                         spaces, CFG.model, env=env)
    pythonpath = ensure_agent_pkgs(project)
    env = dict(env, SUPERVISOR_MODEL=CFG.model, AGENTBRICKS_PROJECT_ROOT=project, PYTHONPATH=pythonpath)
    proc = subprocess.run([sys.executable, "eval/ask.py"], input=json.dumps([q["question"] for q in QUESTIONS]),
                          capture_output=True, text=True, cwd=project, env=env, timeout=3600)
    for line in proc.stdout.splitlines():
        if line.startswith('{"question"'):
            rec = json.loads(line)
            answers[rec["question"]] = rec
    print(f"Supervisor answered {len(answers)}/{len(QUESTIONS)} questions (rc={proc.returncode}).")
    if proc.returncode != 0 or not answers:
        print(proc.stderr[-4000:])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helpers: read routing, Genie SQL and the answer from the message list

# COMMAND ----------

import re

GENIE_TOOLS = ("sourcing_supplier_risk", "purchase_order_analytics")


def parse_turn(output):
    """Return (agents asked, Genie SQL statements in order, final answer text)."""
    agents, sqls, answer = [], [], ""
    for m in output:
        if m.get("type") == "ai":
            for tc in m.get("tool_calls") or []:
                agent = next((g for g in GENIE_TOOLS if tc.get("name", "").startswith(g)), None)
                if agent and agent not in agents:
                    agents.append(agent)
            content = m.get("content")
            if isinstance(content, list):
                content = "".join(c.get("text", "") for c in content if isinstance(c, dict))
            if content and not m.get("tool_calls"):
                answer = content
        elif m.get("type") == "tool":
            try:
                result = json.loads(m.get("content") or "{}")
            except Exception:
                continue
            for att in result.get("attachments") or []:
                q = (att.get("query") or {}).get("query")
                if q:
                    sqls.append(q)
    return agents, sqls, answer


def agent_signature(sqls, answer, signature):
    """Re-run the last SQL the Genie Agent executed; fall back to the answer text."""
    if sqls:
        try:
            rows = spark.sql(sqls[-1]).collect()
            if signature == "rowcount":
                return len(rows), "genie_sql"
            if len(rows) == 1:
                nums = [v for v in rows[0] if isinstance(v, (int, float)) or type(v).__name__ == "Decimal"]
                if nums:
                    return float(nums[0]), "genie_sql"
        except Exception as e:
            print(f"    (could not re-run Genie SQL: {str(e)[:120]})")
    if signature == "rowcount":
        pipe_rows = [ln for ln in answer.splitlines() if ln.strip().startswith("|")]
        non_sep = [ln for ln in pipe_rows if not set(ln.replace("|", "").strip()) <= set("-: ")]
        return max(0, len(non_sep) - 1), "answer_table"
    m = re.findall(r"-?\d+\.?\d*", answer)
    return (float(m[0]) if m else None), "answer_text"


# COMMAND ----------

# MAGIC %md
# MAGIC ## Level 2 — supervisor accuracy vs known-good
# MAGIC Row counts must match exactly. If they differ but both are non-zero, the question
# MAGIC is counted as a **lenient** pass and flagged in `note`, so exact and lenient passes
# MAGIC can be reported separately. Scalars match within 5% (or 1.0), also accepting a
# MAGIC rate given as a percent instead of a fraction.

# COMMAND ----------

results = []
if answers:
    for q in QUESTIONS:
        kg = known_good.get(q["id"], {})
        rec = answers.get(q["question"], {"output": [], "error": "no answer"})
        agents, sqls, answer = parse_turn(rec["output"])
        row = {"id": q["id"], "question": q["question"], "signature": q["signature"],
               "known_good": kg.get("value"), "agent": None, "match": False, "exact": False,
               "routed_to": ",".join(agents), "routing_ok": q.get("space") in agents,
               "genie_sql": sqls[-1] if sqls else "", "note": rec.get("error") or ""}
        if not rec.get("error"):
            val, source = agent_signature(sqls, answer, q["signature"])
            row["agent"] = val
            if q["signature"] == "rowcount" and isinstance(kg.get("value"), int) and isinstance(val, int):
                row["exact"] = val == kg["value"]
                row["match"] = row["exact"] or (val > 0 and kg["value"] > 0)
                row["note"] = f"from {source}" + ("" if row["exact"] else "; lenient non-zero match")
            else:  # scalar
                def num(x):
                    m = re.findall(r"-?\d+\.?\d*", str(x))
                    return float(m[0]) if m else None

                def close(x, y):
                    return x is not None and y is not None and abs(x - y) <= max(1.0, 0.05 * abs(x))
                a, b = num(kg.get("value")), num(val)
                row["exact"] = close(a, b)
                row["match"] = bool(a is not None and b is not None and
                                    (close(a, b) or close(a, b / 100.0) or close(a, b * 100.0)))
                row["note"] = f"from {source}" + ("; percent/fraction normalized" if row["match"] and not row["exact"] else "")
        results.append(row)
        print(f"  {q['id']:<5} kg={row['known_good']} agent={row['agent']} "
              f"{'✓' if row['match'] else '✗'}{'' if row['exact'] else '~'} "
              f"routed={row['routed_to'] or '-'} {'✓' if row['routing_ok'] else '✗'}  {row['note']}")

    passed = sum(1 for r in results if r["match"])
    exact = sum(1 for r in results if r["exact"])
    routed = sum(1 for r in results if r["routing_ok"])
    accuracy = 100.0 * passed / len(results)
    print(f"\n===== SUPERVISOR ACCURACY: {passed}/{len(results)} = {accuracy:.1f}% "
          f"(exact {exact}/{len(results)}, routing {routed}/{len(results)}) =====")
else:
    print("Supervisor eval disabled or no answers — Level 1 (known-good SQL) only.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Accuracy score table (persist for the App & demo)

# COMMAND ----------

from pyspark.sql import Row

if results:
    score_rows = [Row(id=r["id"], question=r["question"], signature=r["signature"],
                      known_good=str(r["known_good"]), agent=str(r["agent"]),
                      match=bool(r["match"]), exact=bool(r["exact"]),
                      routed_to=r["routed_to"], routing_ok=bool(r["routing_ok"]),
                      genie_sql=r["genie_sql"], note=r["note"]) for r in results]
    (spark.createDataFrame(score_rows)
        .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        .saveAsTable(CFG.fq("_BENCHMARK_RESULTS")))
    display(spark.table(CFG.fq("_BENCHMARK_RESULTS")))
    dbutils.notebook.exit(f"07a_benchmark: SUCCESS — accuracy={accuracy:.1f}% ({passed}/{len(results)}, "
                          f"exact {exact}, routing {routed}) over {len(results)} questions.")
else:
    dbutils.notebook.exit(f"07a_benchmark: SUCCESS — {sql_ok}/{len(QUESTIONS)} known-good SQL valid (agent eval off).")
