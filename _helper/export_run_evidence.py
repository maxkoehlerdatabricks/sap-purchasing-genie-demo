"""Export executed notebook runs into Jupyter notebooks WITH their outputs (run evidence).

Run locally (not a Databricks notebook). For every task run of a Databricks job run, it
calls the Jobs export API, extracts the executed notebook (code + every cell's output:
stdout, tables, errors, the exit value) and writes it as ``evidence/notebooks/<nn>_<task>.ipynb``.

Usage:
    python3 _helper/export_run_evidence.py --profile <cli-profile> --run-id <job run id> \
        [--out evidence/notebooks] [--prefix 01]

The source notebooks stay plain ``.py`` files (runnable as-is); these ``.ipynb`` copies are
the committed proof that each one ran, with the real row counts, IDs and answers.
"""
import argparse
import base64
import html
import json
import os
import re
import urllib.parse

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

ANSI = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
MAX_ROWS = 60


def _text(s):
    return ANSI.sub("", s or "")


def _table_outputs(item):
    cols = [c.get("name") for c in item.get("schema", [])]
    rows = item.get("data") or []
    shown = rows[:MAX_ROWS]
    head = "".join(f"<th>{html.escape(str(c))}</th>" for c in cols)
    body = "".join("<tr>" + "".join(f"<td>{html.escape(str(v))}</td>" for v in r) + "</tr>" for r in shown)
    more = f"<p>… {len(rows) - MAX_ROWS} more rows</p>" if len(rows) > MAX_ROWS else ""
    plain = "\t".join(map(str, cols)) + "\n" + "\n".join("\t".join(map(str, r)) for r in shown)
    return {"output_type": "display_data", "metadata": {},
            "data": {"text/html": f"<table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>{more}",
                     "text/plain": plain}}


def _outputs(results):
    if not results:
        return []
    items = results.get("data") if results.get("type") == "listResults" else [results]
    out = []
    for it in items or []:
        t = it.get("type")
        if t == "ansi":
            out.append({"output_type": "stream", "name": it.get("name", "stdout"), "text": _text(it.get("data"))})
        elif t == "table":
            out.append(_table_outputs(it))
        elif t == "html" and len(str(it.get("data", ""))) < 50_000:
            out.append({"output_type": "display_data", "metadata": {}, "data": {"text/html": it.get("data")}})
        elif t == "raw":
            out.append({"output_type": "stream", "name": "stdout", "text": f"Notebook exited: {_text(str(it.get('data')))}\n"})
        elif t == "error":
            out.append({"output_type": "error", "ename": "Error", "evalue": _text(str(it.get("data")))[:4000], "traceback": []})
    return out


def notebook_from_export(content, meta):
    m = re.search(r"__DATABRICKS_NOTEBOOK_MODEL = '([^']+)'", content)
    model = json.loads(urllib.parse.unquote(base64.b64decode(m.group(1)).decode()))
    cells = []
    for c in sorted(model["commands"], key=lambda c: c.get("position", 0)):
        src = c.get("command", "")
        outs = _outputs(c.get("results"))
        if c.get("errorSummary") or c.get("error"):
            outs.append({"output_type": "error", "ename": "Error",
                         "evalue": _text(c.get("errorSummary") or c.get("error"))[:4000], "traceback": []})
        if not src.strip():
            # Cells pulled in by %run (e.g. 00_config): attach their output to the %run cell.
            if outs and cells and cells[-1]["cell_type"] == "code":
                cells[-1]["outputs"].extend(outs)
            continue
        if src.lstrip().startswith("%md"):
            cells.append({"cell_type": "markdown", "metadata": {},
                          "source": src.lstrip()[3:].lstrip("\n").splitlines(keepends=True)})
        else:
            cells.append({"cell_type": "code", "metadata": {}, "execution_count": None,
                          "source": src.splitlines(keepends=True), "outputs": outs})
    header = (f"**Run evidence** · task `{meta['task_key']}` · run `{meta['run_id']}` · "
              f"result **{meta['result']}** · executed {meta['start']} UTC on serverless.\n\n"
              f"Exported from the Databricks job run with `_helper/export_run_evidence.py`. "
              f"Source notebook: `{meta['source']}`.")
    cells.insert(0, {"cell_type": "markdown", "metadata": {}, "source": header.splitlines(keepends=True)})
    return {"cells": cells, "metadata": {"language_info": {"name": "python"}, "databricks_run": meta},
            "nbformat": 4, "nbformat_minor": 5}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", required=True)
    ap.add_argument("--run-id", type=int, required=True)
    ap.add_argument("--out", default="evidence/notebooks")
    ap.add_argument("--prefix", default="")
    a = ap.parse_args()

    import datetime as dt
    w = WorkspaceClient(profile=a.profile)
    run = w.jobs.get_run(a.run_id)
    os.makedirs(a.out, exist_ok=True)
    tasks = sorted(run.tasks, key=lambda t: t.start_time or 0)
    # Keep only the last attempt per task (serverless may retry a task).
    last = {}
    for t in tasks:
        last[t.task_key] = t
    for i, t in enumerate(sorted(last.values(), key=lambda t: t.start_time or 0), start=1):
        if not t.notebook_task:
            continue
        exp = w.jobs.export_run(t.run_id, views_to_export=jobs.ViewsToExport.CODE)
        meta = {"task_key": t.task_key, "run_id": t.run_id, "job_run_id": a.run_id,
                "result": str(t.state.result_state).split(".")[-1],
                "start": dt.datetime.fromtimestamp((t.start_time or 0) / 1000, dt.timezone.utc).strftime("%Y-%m-%d %H:%M"),
                "source": t.notebook_task.notebook_path.split("sap-purchasing-genie-demo/")[-1]}
        nb = notebook_from_export(exp.views[0].content, meta)
        name = f"{a.prefix}{i:02d}_{t.task_key}.ipynb"
        with open(os.path.join(a.out, name), "w") as f:
            json.dump(nb, f, indent=1, ensure_ascii=False)
        print(f"  {name:<45} {meta['result']}")


if __name__ == "__main__":
    main()
