"""Ask the supervisor a batch of questions in-process (used by 07a_benchmark).

Runs the same graph, system prompt and Genie tools as the deployed agent, without the
HTTP server. Reads a JSON list of questions on stdin and writes one JSON object per
question to stdout: {"question", "output", "error"}. ``output`` is the turn's message
list as ``message.model_dump()``: the same shape as the deployed /api/invocations
``output``, so the benchmark and the App parse one format.

Run from the project root (so `agent` imports and agent.toml is found):
    echo '["Which materials have no multi-sourcing strategy?"]' | python eval/ask.py
"""

import asyncio
import json
import sys
import uuid

from agent.agent import configure, run_agent


async def ask(question: str) -> list[dict]:
    session = str(uuid.uuid4())
    output = []
    async for mode, payload in run_agent({"messages": [{"role": "user", "content": question}]},
                                         session_id=session):
        if mode != "updates":
            continue
        for node in payload.values():
            for message in (node.get("messages", []) if isinstance(node, dict) else []):
                output.append(message.model_dump())
    return output


async def main() -> None:
    configure()
    for question in json.load(sys.stdin):
        try:
            result = {"question": question, "output": await ask(question), "error": None}
        except Exception as e:  # keep going: one failing question must not stop the benchmark
            result = {"question": question, "output": [], "error": f"{type(e).__name__}: {e}"}
        print(json.dumps(result, default=str), flush=True)


if __name__ == "__main__":
    asyncio.run(main())
