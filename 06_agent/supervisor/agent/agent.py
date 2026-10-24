"""SAP purchasing supervisor: one LangGraph agent that routes to two Genie Agents.

Replaces the legacy Agent Bricks Supervisor Agent (MAS). The routing rules that used to
live in the MAS ``instructions`` are the system prompt below; the two domain Genie Agents
are native Genie tools declared in ``agent.toml`` (added by 06a_deploy_supervisor).
"""

import os
from collections.abc import AsyncGenerator, Callable
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks_langchain import ChatDatabricks
from langchain.agents import create_agent

# Importing the tools package auto-registers every tool module.
from agent.tools import all_tools
from databricks_agentkit import workspace_client, workspace_headers
from databricks_agentkit.langgraph import (
    checkpointer,
    configure_tracing,
    genie_tools,
    start_trace,
    thread_config,
)
from databricks_agentkit.langgraph.session_store import (
    invocation_id_from_metadata,
    invocation_metadata,
)
from databricks_agentkit.runtime.tool_manifest import resolve_model_service

# A Unity Catalog AI Gateway model service from `system.ai` (`agentbricks experimental models list`).
MODEL = resolve_model_service() or os.getenv("SUPERVISOR_MODEL", "system.ai.claude-sonnet-4-5")

# The tool ids match the `--name` passed to `agentbricks tools add genie-agent` in 06a.
# Each Genie Agent gets three tools: <id>_ask, <id>_poll and <id>_query_result.
SYSTEM_PROMPT = """You are the SAP purchasing supervisor for an automotive OEM. You answer
questions by delegating to two Genie Agents. Never invent numbers: every figure in your
answer must come from a Genie Agent result.

Genie Agents (tools):
- sourcing_supplier_risk_* : sourcing and supply continuity. Source lists and approved
  vendors (including vendor counts per material and plant), multi-sourcing gaps,
  single-sourced materials, supplier delivery risk, purchasing blocks, quota
  concentration, and the live mitigation cases buyers opened on those risks (open cases,
  risks nobody is working on yet).
- purchase_order_analytics_* : purchase orders, i.e. what was actually ordered and
  delivered. On-time delivery (OTD) rates, net-price variance across vendors, total
  spend, and orders placed under active supplier delivery risk.

Rules:
1. Route each question to the one Genie Agent that owns it. For cross-domain questions
   (for example "did we place POs with single-sourced risky suppliers?"), ask the owning
   Genie Agent for the entities first, then ask the other one about exactly those
   entities (pass the supplier numbers LIFNR or material numbers MATNR in the question).
   Never answer a cross-domain question with an unfiltered result from one domain.
2. Pass the user's question to the Genie Agent in plain business language. Do not write
   SQL yourself.
3. If an ask result has timed_out = true, call the matching *_poll tool with the returned
   conversation_id and message_id. Do not resubmit the question.
4. When the question asks for a list or a count, call *_query_result with the query
   attachment's attachment_id and answer from those rows.
5. Answer concisely. Show result rows as a markdown table (at most 20 rows, and say how
   many rows there are in total). Name the Genie Agent(s) you used on the last line,
   for example "Source: purchase_order_analytics".
"""


class _RoutedChatDatabricks(ChatDatabricks):
    """Forward account-host workspace routing to the underlying OpenAI clients."""

    def _get_client_kwargs(self) -> dict[str, Any]:
        kwargs = super()._get_client_kwargs()
        if headers := workspace_headers():
            kwargs["default_headers"] = headers
        return kwargs


def configure() -> None:
    """Wire up global state; call once at server startup (not at import)."""
    workspace_client()  # fail fast if Databricks auth is not configured
    configure_tracing()


async def create_agent_graph(
    actor: str,
    model: str | None = None,
    *,
    workspace_client_for: Callable[[str], WorkspaceClient] | None = None,
):
    """Build the supervisor graph: the two Genie Agent tool sets plus any local tools."""
    tools = [*all_tools(), *genie_tools(workspace_client_for=workspace_client_for)]
    return create_agent(
        # use_ai_gateway routes to the Unity Catalog AI Gateway, so `model` is a
        # `system.ai.*` model service name rather than a serving-endpoint name.
        model=_RoutedChatDatabricks(
            endpoint=model or MODEL, workspace_client=workspace_client(), use_ai_gateway=True
        ),
        tools=tools,
        system_prompt=SYSTEM_PROMPT,
        checkpointer=checkpointer(),
    )


async def recovery_input(
    agent_input: Any,
    *,
    session_id: str,
    actor: str,
    invocation_id: str,
) -> Any:
    """Return the input to pass to ``run_agent`` after worker loss (see AGENTKIT_CONTRACT.md)."""
    checkpoint = await checkpointer().aget_tuple(thread_config(session_id, actor))
    current_invocation_checkpointed = bool(
        checkpoint and invocation_id_from_metadata(checkpoint.metadata) == invocation_id
    )
    return None if current_invocation_checkpointed else agent_input


async def run_agent(
    agent_input: Any,
    *,
    session_id: str,
    actor: str | None = None,
    model: str | None = None,
    invocation_id: str | None = None,
    workspace_client_for: Callable[[str], WorkspaceClient] | None = None,
    graph: Any = None,
) -> AsyncGenerator[Any, None]:
    """Run the agent and yield native LangGraph stream events.

    Framework-native entrypoint with no dependency on Agent Bricks request types, so
    07a_benchmark can call it in-process from a notebook.
    """
    actor = actor or session_id
    if graph is None:
        graph = await create_agent_graph(actor, model, workspace_client_for=workspace_client_for)
    config = thread_config(session_id, actor)
    if invocation_id:
        config["metadata"] = invocation_metadata(invocation_id)

    last_update: Any = None
    with start_trace(name="invoke", inputs=agent_input, session_id=session_id) as span:
        async for event in graph.astream(
            input=agent_input,
            config=config,
            stream_mode=["updates", "messages"],
            durability="sync",
        ):
            if event[0] == "updates":
                last_update = event[1]
            yield event
        if span is not None and last_update is not None:
            span.set_outputs(last_update)
