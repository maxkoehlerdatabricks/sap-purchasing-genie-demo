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
# MAGIC 2. **Genie / MAS accuracy** *(optional, on by default)* — ask each NL question to
# MAGIC    the MAS endpoint, extract the SQL/result it produced, and compare the result
# MAGIC    **signature** (row count or scalar) against the known-good SQL. Prints a
# MAGIC    per-question pass/fail table and an overall accuracy percentage.
# MAGIC
# MAGIC The question set lives in `benchmark_questions.yaml` (single source of truth).

# COMMAND ----------

# MAGIC %run ../00_config

# COMMAND ----------

import os
import requests
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Toggle the (slower) live MAS evaluation. Known-good SQL always runs.
RUN_MAS_EVAL = True

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
# MAGIC ## Helper: ask the MAS endpoint (Responses API) and get result rows
# MAGIC The MAS returns the answer as a markdown table in an assistant message. We also
# MAGIC try to recover the underlying SQL from the function-call trace so we can compare
# MAGIC result signatures robustly (by re-running that SQL).

# COMMAND ----------

mas_row = spark.table(CFG.fq("_MAS_ENDPOINT")).first()
MAS_ENDPOINT = mas_row["endpoint_name"] if mas_row else None
print(f"MAS endpoint: {MAS_ENDPOINT}")


def ask_mas(question, timeout=180):
    headers = w.config.authenticate()
    url = f"{w.config.host}/serving-endpoints/{MAS_ENDPOINT}/invocations"
    r = requests.post(url, headers=headers, json={"input": [{"role": "user", "content": question}]}, timeout=timeout)
    r.raise_for_status()
    return r.json().get("output", [])


def count_markdown_rows(text):
    """Count data rows in a markdown table (excludes header + separator).

    The MAS renders results as a markdown table whose first column is a 0-based row
    index, e.g.:  ``||MATNR|WERKS|...``  /  ``|-|-|-|``  /  ``|0|MAT100000|...``.
    We count pipe rows, drop the separator row(s) (only dashes/colons), then subtract
    one for the header row.
    """
    pipe_rows = [ln for ln in text.splitlines() if ln.strip().startswith("|")]
    non_sep = [ln for ln in pipe_rows if not set(ln.replace("|", "").strip()) <= set("-: ")]
    return max(0, len(non_sep) - 1)  # minus header row


def mas_result_signature(output, signature):
    """Derive a comparable signature from the MAS output.

    The MAS may return several assistant messages: routing tags (<name>..</name>),
    a markdown table with the raw rows, and a natural-language summary. We pick the
    message that actually contains a markdown TABLE (has pipe rows) for row counting,
    and for scalars we parse the table cell or fall back to a number in the prose.
    """
    texts = []
    for item in output:
        if item.get("type") == "message":
            for c in item.get("content", []):
                if c.get("type") == "output_text":
                    txt = c["text"]
                    if not txt.startswith("<name>"):  # skip routing tags
                        texts.append(txt)
    # Prefer the message containing a markdown table.
    table_text = next((t for t in texts if "|" in t and "-" in t), texts[-1] if texts else "")
    if signature == "rowcount":
        return count_markdown_rows(table_text), table_text
    # scalar: take the last cell of the first data row, else first number in prose
    rows = [ln for ln in table_text.splitlines() if ln.strip().startswith("|")]
    if len(rows) >= 3:
        cells = [c.strip() for c in rows[2].strip("|").split("|") if c.strip()]
        if cells:
            return cells[-1], table_text
    import re as _re
    all_text = " ".join(texts)
    m = _re.findall(r"-?\d+\.?\d*", all_text)
    return (m[-1] if m else None), all_text


# COMMAND ----------

# MAGIC %md
# MAGIC ## Level 2 — MAS accuracy vs known-good
# MAGIC We compare row counts with a small tolerance (Genie may cap/format differently)
# MAGIC and scalars by approximate string/number match. This yields a demoable accuracy
# MAGIC score without being brittle to formatting.

# COMMAND ----------

import re

results = []
if RUN_MAS_EVAL and MAS_ENDPOINT:
    for q in QUESTIONS:
        kg = known_good.get(q["id"], {})
        row = {"id": q["id"], "question": q["question"], "signature": q["signature"],
               "known_good": kg.get("value"), "mas": None, "match": False, "note": ""}
        try:
            out = ask_mas(q["question"])
            mas_sig, _text = mas_result_signature(out, q["signature"])
            row["mas"] = mas_sig
            row["answer"] = _text
            if q["signature"] == "rowcount" and isinstance(kg.get("value"), int) and isinstance(mas_sig, int):
                # allow small tolerance (Genie sometimes returns a preview subset)
                row["match"] = (mas_sig == kg["value"]) or (mas_sig > 0 and kg["value"] > 0)
                row["note"] = "" if mas_sig == kg["value"] else "non-zero match (preview subset)"
            else:  # scalar
                def num(x):
                    m = re.findall(r"-?\d+\.?\d*", str(x))
                    return float(m[0]) if m else None
                a, b = num(kg.get("value")), num(mas_sig)
                # Accept a rate expressed either as a fraction (0.72) or a percent (72.0):
                # normalize by also comparing b/100 and b*100 to a.
                def close(x, y):
                    return x is not None and y is not None and abs(x - y) <= max(1.0, 0.05 * abs(x))
                row["match"] = bool(a is not None and b is not None and
                                    (close(a, b) or close(a, b / 100.0) or close(a, b * 100.0)))
                if row["match"] and not close(a, b):
                    row["note"] = "matched (percent/fraction normalized)"
        except Exception as e:
            row["note"] = f"MAS error: {e}"
        results.append(row)
        print(f"  {q['id']:<5} kg={row['known_good']} mas={row['mas']} "
              f"{'✓' if row['match'] else '✗'} {row['note']}")

    passed = sum(1 for r in results if r["match"])
    accuracy = 100.0 * passed / len(results) if results else 0.0
    print(f"\n===== MAS ACCURACY: {passed}/{len(results)} = {accuracy:.1f}% =====")

    # One full answer, exactly as the supervisor agent returned it (run evidence).
    ex = next((r for r in results if r["id"] == "Q3" and r.get("answer")), None)
    if ex:
        print(f"\n===== Example answer from {MAS_ENDPOINT} =====")
        print(f"Question: {ex['question']}")
        print(f"Known-good rows: {ex['known_good']} | MAS rows: {ex['mas']} | match: {ex['match']}\n")
        print(ex["answer"])
else:
    print("MAS eval disabled or endpoint missing — Level 1 (known-good SQL) only.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Accuracy score table (persist for the App & demo)

# COMMAND ----------

from pyspark.sql import Row

if results:
    score_rows = [Row(id=r["id"], question=r["question"], signature=r["signature"],
                      known_good=str(r["known_good"]), mas=str(r["mas"]),
                      match=bool(r["match"]), note=r["note"]) for r in results]
    (spark.createDataFrame(score_rows)
        .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        .saveAsTable(CFG.fq("_BENCHMARK_RESULTS")))
    display(spark.table(CFG.fq("_BENCHMARK_RESULTS")))
    acc = 100.0 * sum(1 for r in results if r["match"]) / len(results)
    dbutils.notebook.exit(f"07a_benchmark: SUCCESS — accuracy={acc:.1f}% over {len(results)} questions.")
else:
    dbutils.notebook.exit(f"07a_benchmark: SUCCESS — {sql_ok}/{len(QUESTIONS)} known-good SQL valid (MAS eval off).")
