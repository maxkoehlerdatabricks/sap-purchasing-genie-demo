# Databricks notebook source
# MAGIC %md
# MAGIC # _agentbricks_helpers — run the Agent Bricks CLI from a serverless notebook
# MAGIC
# MAGIC `%run` this from `06a_deploy_supervisor`, `07a_benchmark` and `99_teardown`.
# MAGIC
# MAGIC * The Agent Bricks CLI (`databricks-agentbricks`) shells out to the `databricks` CLI,
# MAGIC   so the CLI binary is downloaded from its GitHub release into `/tmp`.
# MAGIC * Python packages are installed with `pip install --target` into separate folders
# MAGIC   and only put on `PYTHONPATH` of the subprocesses, never into the notebook kernel:
# MAGIC   their dependencies (e.g. protobuf 7) break the kernel's Spark Connect client.
# MAGIC   (`python -m venv` is not available on serverless.)
# MAGIC * Auth is the notebook's own identity, passed as `DATABRICKS_HOST` / `DATABRICKS_TOKEN`.
# MAGIC   That is enough to deploy and delete. It is NOT accepted by an App's ingress (Apps
# MAGIC   only take OAuth tokens), which is why 07a runs the agent code in-process instead of
# MAGIC   calling the deployed URL.

# COMMAND ----------

import io
import json
import os
import platform
import shutil
import subprocess
import sys
import tempfile
import zipfile

import requests

# /tmp is shared between serverless runs (and their users), so every session works in its
# own fresh directory instead of fixed /tmp paths.
WORK_DIR = tempfile.mkdtemp(prefix="agentbricks_")
AB_PKGS = f"{WORK_DIR}/cli_pkgs"        # databricks-agentbricks (deploy, tools add, delete)
AGENT_PKGS = f"{WORK_DIR}/agent_pkgs"   # the supervisor's runtime deps (07a in-process runs)
DBCLI_DIR = f"{WORK_DIR}/databricks_cli"
# Only these are replaced by the notebook's own identity. The other DATABRICKS_* variables
# stay: mlflow needs DATABRICKS_RUNTIME_VERSION, or it tries to read /databricks/DBR_VERSION,
# which serverless does not allow.
_AUTH_VARS = ("DATABRICKS_HOST", "DATABRICKS_TOKEN", "DATABRICKS_CLIENT_ID", "DATABRICKS_CLIENT_SECRET",
              "DATABRICKS_CONFIG_PROFILE", "DATABRICKS_CONFIG_FILE", "DATABRICKS_AUTH_TYPE")
_AB_CMD = [sys.executable, "-c", "from databricks_agentbricks.cli import main; main()"]


def _notebook_auth_env():
    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    env = {k: v for k, v in os.environ.items() if k not in (*_AUTH_VARS, "PYTHONPATH", "VIRTUAL_ENV")}
    env.update(DATABRICKS_HOST="https://" + spark.conf.get("spark.databricks.workspaceUrl"),
               DATABRICKS_TOKEN=ctx.apiToken().get(),
               AGENTBRICKS_DISABLE_AGENT_HINT="1", NO_COLOR="1",
               PATH=f"{DBCLI_DIR}:" + os.environ.get("PATH", ""))
    return env


def _install_databricks_cli():
    if os.path.exists(f"{DBCLI_DIR}/databricks"):
        return
    ver = requests.get("https://api.github.com/repos/databricks/cli/releases/latest", timeout=60).json()["tag_name"].lstrip("v")
    arch = "arm64" if platform.machine() in ("aarch64", "arm64") else "amd64"
    url = f"https://github.com/databricks/cli/releases/download/v{ver}/databricks_cli_{ver}_linux_{arch}.zip"
    zipfile.ZipFile(io.BytesIO(requests.get(url, timeout=300).content)).extractall(DBCLI_DIR)
    os.chmod(f"{DBCLI_DIR}/databricks", 0o755)
    print(f"  ✓ databricks CLI {ver}")


def _pip_target(path, packages):
    r = subprocess.run([sys.executable, "-m", "pip", "install", "-q", "--upgrade", "--target", path, *packages],
                       capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"pip install {packages} failed:\n{r.stderr[-3000:]}")


def ensure_agentbricks_cli():
    """Install the databricks CLI + Agent Bricks CLI once per session; return the subprocess env."""
    _install_databricks_cli()
    if not os.path.exists(f"{AB_PKGS}/databricks_agentbricks"):
        _pip_target(AB_PKGS, ["databricks-agentbricks"])
    env = _notebook_auth_env()
    v = subprocess.run(_AB_CMD + ["--version"], capture_output=True, text=True, env=dict(env, PYTHONPATH=AB_PKGS))
    print(f"  ✓ {v.stdout.strip()}")
    return env


def agentbricks(*args, env=None, cwd=None, check=True):
    """Run `agentbricks <args>` (JSON output) and return the parsed result (or raw text)."""
    env = env or ensure_agentbricks_cli()
    r = subprocess.run(_AB_CMD + ["-o", "json", *args],
                       capture_output=True, text=True, env=dict(env, PYTHONPATH=AB_PKGS), cwd=cwd)
    if check and r.returncode != 0:
        raise RuntimeError(f"agentbricks {' '.join(args)} failed (rc={r.returncode}):\n{r.stdout[-3000:]}\n{r.stderr[-3000:]}")
    try:
        return json.loads(r.stdout)
    except Exception:
        return r.stdout + r.stderr


def prepare_supervisor_project(src, dest, space_ids, model, env=None):
    """Copy the supervisor project to `dest` and bind the two Genie Agents as tools.

    The committed agent.toml has no tools because the space IDs change on every 05a run.
    Edits happen in the copy only, so the (Git) source folder is never modified.
    """
    shutil.rmtree(dest, ignore_errors=True)
    shutil.copytree(src, dest, ignore=shutil.ignore_patterns("__pycache__", ".venv", "*.pyc"))
    for name in ("sourcing_supplier_risk", "purchase_order_analytics"):
        agentbricks("tools", "add", "genie-agent", space_ids[name], "--name", name,
                    "--auth", "app", "--source", dest, env=env)
    with open(f"{dest}/app.yaml", "a") as f:
        f.write(f'\nenv:\n  - name: SUPERVISOR_MODEL\n    value: "{model}"\n')
    print(f"  ✓ project prepared in {dest} (2 Genie Agent tools, model {model})")
    return dest


def ensure_agent_pkgs(project_dir):
    """Install the supervisor's runtime dependencies (from its pyproject) into AGENT_PKGS.

    Returns the PYTHONPATH to run the agent code with: the project first, then the deps.
    """
    # tomllib needs Python 3.11; child notebooks can run on 3.10, so read it with the tomli
    # that the Agent Bricks CLI installed.
    code = "import json,sys,tomli; print(json.dumps(tomli.load(open(sys.argv[1],'rb'))['project']['dependencies']))"
    deps = json.loads(subprocess.run([sys.executable, "-c", code, f"{project_dir}/pyproject.toml"], check=True,
                                     capture_output=True, text=True, env=dict(os.environ, PYTHONPATH=AB_PKGS)).stdout)
    if not os.path.exists(f"{AGENT_PKGS}/databricks_agentkit"):
        _pip_target(AGENT_PKGS, deps)
    print(f"  ✓ agent dependencies ready ({len(deps)} packages)")
    return f"{project_dir}:{AGENT_PKGS}"
