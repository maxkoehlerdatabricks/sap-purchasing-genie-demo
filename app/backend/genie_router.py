"""Genie router — a lightweight supervisor over the two Genie spaces.

Used when the workspace cannot create an Agent Bricks Multi-Agent Supervisor (for example
when serverless compute access control blocks Supervisor Agents). It plays the same role:

  1. An LLM (Foundation Model API) routes the question to one or both domain agents,
     using the same agent descriptions and routing instructions as the MAS (06a).
  2. Each chosen agent is a Genie space, asked through the Genie Conversation API.
  3. The answer comes back in the MAS Responses-API shape (`output` items: a routing tag,
     a markdown table of the result rows, and the explanation), so the App chat and the
     benchmark (07a) handle both modes with the same parsing code.

Shared by the App backend (backend/main.py), 06a (smoke test) and 07a (benchmark).
"""
import json
import time

AGENTS = {
    "Sourcing_Supplier_Risk": (
        "Answers sourcing-risk and supply-continuity questions: multi-sourcing gaps, "
        "single-sourced materials, supplier delivery risk, purchasing blocks, quota "
        "concentration, and the live mitigation cases buyers opened on those risks (open "
        "cases, risks nobody is working on yet)."),
    "Purchase_Order_Analytics": (
        "Answers purchase-order questions: on-time delivery rates, net-price variance "
        "across vendors, total spend, and orders placed under active supplier delivery risk."),
}

INSTRUCTIONS = (
    "Route sourcing, multi-sourcing, single-sourcing, supplier-risk, purchasing-block "
    "and quota questions to Sourcing_Supplier_Risk. Route on-time delivery, net price, "
    "price variance, spend and orders-under-risk questions to Purchase_Order_Analytics. "
    "For cross-domain questions (e.g. 'did we place POs with single-sourced risky "
    "suppliers?'), consult both agents and combine the answers."
)

MAX_TABLE_ROWS = 1000


def route(w, llm_endpoint, question):
    """Return the list of agent names that should answer the question."""
    prompt = (
        "You are the supervisor of these agents:\n"
        + "\n".join(f"- {n}: {d}" for n, d in AGENTS.items())
        + f"\n\nRouting rules: {INSTRUCTIONS}\n"
        "Prefer exactly ONE agent. Supplier delivery-risk flags and single-sourcing are "
        "Sourcing_Supplier_Risk topics even when combined. Choose both agents only when the "
        "question needs purchase-order facts (orders, deliveries, prices, spend) AND "
        "sourcing facts, and then list the most relevant agent first.\n\n"
        "Reply with JSON only, for example {\"agents\": [\"Sourcing_Supplier_Risk\"]}.\n\n"
        f"Question: {question}"
    )
    try:
        r = w.api_client.do("POST", f"/serving-endpoints/{llm_endpoint}/invocations",
                            body={"messages": [{"role": "user", "content": prompt}],
                                  "max_tokens": 60, "temperature": 0})
        text = r["choices"][0]["message"]["content"]
        picked = json.loads(text[text.index("{"):text.rindex("}") + 1]).get("agents", [])
        picked = [a for a in picked if a in AGENTS]
        if picked:
            return picked
    except Exception:
        pass
    return ["Sourcing_Supplier_Risk"]


def ask_genie(w, space_id, question, timeout=300):
    """Ask one Genie space; return (explanation, sql, columns, rows)."""
    base = f"/api/2.0/genie/spaces/{space_id}"
    for attempt in range(5):
        try:
            m = w.api_client.do("POST", f"{base}/start-conversation", body={"content": question})
            break
        except Exception as e:  # Genie rate limits: back off and retry
            if attempt == 4 or "429" not in str(e) and "RESOURCE_EXHAUSTED" not in str(e):
                raise
            time.sleep(15 * (attempt + 1))
    cid, mid = m["conversation_id"], m["message_id"]
    deadline = time.time() + timeout
    while True:
        msg = w.api_client.do("GET", f"{base}/conversations/{cid}/messages/{mid}")
        if msg.get("status") in ("COMPLETED", "FAILED", "CANCELLED", "QUERY_RESULT_EXPIRED"):
            break
        if time.time() > deadline:
            raise TimeoutError(f"Genie did not answer within {timeout}s")
        time.sleep(3)
    if msg.get("status") != "COMPLETED":
        raise RuntimeError(f"Genie message {msg.get('status')}: {msg.get('error')}")

    explanation, sql, cols, rows = "", "", [], []
    for a in msg.get("attachments", []):
        if "text" in a:
            explanation = a["text"].get("content", "")
        if "query" in a:
            sql = a["query"].get("query", "")
            explanation = explanation or a["query"].get("description", "")
            res = w.api_client.do("GET", f"{base}/conversations/{cid}/messages/{mid}"
                                         f"/query-result/{a['attachment_id']}")
            st = res.get("statement_response", {})
            cols = [c["name"] for c in st.get("manifest", {}).get("schema", {}).get("columns", [])]
            rows = (st.get("result") or {}).get("data_array") or []
    return explanation, sql, cols, rows


def to_markdown(cols, rows):
    """Markdown table in the MAS format: a leading 0-based index column."""
    if not cols:
        return ""
    lines = ["||" + "|".join(cols) + "|", "|-" * (len(cols) + 1) + "|"]
    for i, r in enumerate(rows[:MAX_TABLE_ROWS]):
        lines.append(f"|{i}|" + "|".join("" if v is None else str(v) for v in r) + "|")
    return "\n".join(lines)


def ask(w, llm_endpoint, spaces, question):
    """Supervisor call. ``spaces`` maps agent name -> Genie space id.

    Returns ``(output, sql)``: ``output`` in the MAS Responses-API shape.
    """
    output, sqls = [], []

    def msg(text):
        output.append({"type": "message", "content": [{"type": "output_text", "text": text}]})

    for agent in route(w, llm_endpoint, question):
        explanation, sql, cols, rows = ask_genie(w, spaces[agent], question)
        msg(f"<name>{agent}</name>")
        if cols:
            msg(to_markdown(cols, rows))
        if explanation:
            msg(explanation)
        if sql:
            sqls.append(sql)
    return output, "\n\n".join(sqls)
