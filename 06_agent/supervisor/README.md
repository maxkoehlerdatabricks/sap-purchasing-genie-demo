# SAP purchasing supervisor (Agent Bricks CLI)

A LangGraph agent served by `DurableAgentServer` on Agent Runtime (a Databricks App
named `agent-bricks-sap-supervisor`). It replaces the legacy Agent Bricks Supervisor
Agent (MAS). It routes each question to one of two Genie Agents, or to both for
cross-domain questions:

| Tool id | Genie Agent | Owns |
| --- | --- | --- |
| `sourcing_supplier_risk` | SAP Purchasing — Sourcing & Supplier Risk | multi-sourcing, delivery risk, blocks, quotas, mitigation cases |
| `purchase_order_analytics` | SAP Purchasing — Purchase Order Analytics | OTD, net-price variance, spend, POs under risk |

The routing rules are the system prompt in `agent/agent.py`. Both Genie tools use
`auth = "app"`: the agent's service principal calls the Genie Agents, and
`06a_deploy_supervisor` grants it CAN_RUN on them plus the UC and warehouse access
their queries need.

## How it is deployed

`06_agent/06a_deploy_supervisor` (run by `00_run_all`) does all of this from a
serverless notebook:

```bash
pip install databricks-agentbricks          # plus the databricks CLI binary
cp -r 06_agent/supervisor /tmp/supervisor    # agent.toml is edited in the copy only
agentbricks tools add genie-agent <space 1> --name sourcing_supplier_risk --auth app
agentbricks tools add genie-agent <space 2> --name purchase_order_analytics --auth app
agentbricks deploy sap-supervisor --source /tmp/supervisor
```

## Calling it

In a browser: open the app URL. `runtime/ui.py` + `ui/` serve the Agent Bricks chat app
(no build step). Memory and Sessions show as off, because no stores are bound.

From code: `POST <app url>/api/invocations` with a Databricks **OAuth** token (Apps reject PATs and
notebook tokens):

```json
{"id": "<new uuid>", "session_id": "<uuid>",
 "input": {"messages": [{"role": "user", "content": "Which materials have no multi-sourcing strategy?"}]}}
```

The response `output` is the list of LangChain messages of the turn. The last `ai`
message holds the answer. The `tool` messages hold the Genie results, including the
SQL Genie ran. The Databricks App calls it with its own service principal (CAN_USE is
granted in `app/deploy_app`). From a laptop:
`agentbricks -p <profile> endpoint invoke agent-bricks-sap-supervisor --path /api/invocations --json '<body>'`.

See `AGENTKIT_CONTRACT.md` for the runtime contract (streaming, background, recovery).
