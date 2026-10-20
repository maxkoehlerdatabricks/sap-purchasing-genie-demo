import React, { useEffect, useState } from "react";
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid, Cell,
} from "recharts";

const api = (path, opts) => fetch(path, opts).then((r) => r.json());

const COLORS = ["#FF3621", "#1B3139", "#00A972", "#FFAB00", "#2272B4", "#8BCAE7"];

// ---------------------------------------------------------------------------
// KPI cards
// ---------------------------------------------------------------------------
function Kpis() {
  const [k, setK] = useState(null);
  useEffect(() => { api("/api/charts/kpis").then(setK).catch(() => {}); }, []);
  if (!k) return <div className="loading">Loading KPIs…</div>;
  const cards = [
    { label: "Materials w/o multi-sourcing", value: k.no_multisource, tone: "warn" },
    { label: "Single-sourced & risky", value: k.single_sourced_risky, tone: "bad" },
    { label: "Suppliers at delivery risk", value: k.risky_suppliers, tone: "bad" },
    { label: "Materials w/o valid sourcing", value: k.no_sourcing, tone: "warn" },
    { label: "Overall OTD rate", value: k.overall_otd != null ? (k.overall_otd * 100).toFixed(1) + "%" : "—", tone: "ok" },
  ];
  return (
    <div className="kpis">
      {cards.map((c) => (
        <div className={`kpi ${c.tone}`} key={c.label}>
          <div className="kpi-value">{c.value}</div>
          <div className="kpi-label">{c.label}</div>
        </div>
      ))}
    </div>
  );
}

function Chart({ title, endpoint, xKey, barKey, subtitle }) {
  const [data, setData] = useState([]);
  const [err, setErr] = useState(null);
  useEffect(() => {
    api(endpoint).then((d) => setData(Array.isArray(d) ? d : [])).catch((e) => setErr(String(e)));
  }, [endpoint]);
  return (
    <div className="card">
      <h3>{title}</h3>
      {subtitle && <p className="sub">{subtitle}</p>}
      {err ? <div className="err">{err}</div> : (
        <ResponsiveContainer width="100%" height={280}>
          <BarChart data={data} margin={{ top: 8, right: 16, bottom: 40, left: 8 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#eee" />
            <XAxis dataKey={xKey} angle={-30} textAnchor="end" interval={0} height={60} tick={{ fontSize: 11 }} />
            <YAxis tick={{ fontSize: 11 }} />
            <Tooltip />
            <Bar dataKey={barKey}>
              {data.map((_, i) => <Cell key={i} fill={COLORS[i % COLORS.length]} />)}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      )}
    </div>
  );
}

function Visualizations() {
  return (
    <div>
      <Kpis />
      <div className="grid">
        <Chart title="Sourcing coverage by material group" subtitle="Distinct vendors (mv_sourcing_coverage)"
               endpoint="/api/charts/sourcing_coverage" xKey="material_group" barKey="distinct_vendors" />
        <Chart title="Delivery-risk vendors by risk category" subtitle="mv_supplier_risk"
               endpoint="/api/charts/risk_exposure" xKey="risk_category" barKey="delivery_risk_vendors" />
        <Chart title="Worst on-time delivery by supplier" subtitle="mv_delivery_performance (lower = worse)"
               endpoint="/api/charts/otd" xKey="supplier" barKey="otd_rate" />
        <Chart title="Top net-price spread by material" subtitle="net_price_variance_per_material()"
               endpoint="/api/charts/price_variance" xKey="material" barKey="price_spread" />
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Chat tab — talks to the MAS endpoint via /api/chat
// ---------------------------------------------------------------------------
const SAMPLES = [
  "Which materials have no multi-sourcing strategy?",
  "Which materials are single-sourced and their sole supplier has a delivery-risk flag?",
  "What is the on-time delivery rate per supplier over the last 12 months?",
  "Which suppliers hold a purchasing block but still have active sourcing records?",
];

function Chat() {
  const [msgs, setMsgs] = useState([]);
  const [input, setInput] = useState("");
  const [busy, setBusy] = useState(false);

  const send = async (text) => {
    const q = (text ?? input).trim();
    if (!q || busy) return;
    setMsgs((m) => [...m, { role: "user", text: q }]);
    setInput("");
    setBusy(true);
    try {
      const res = await api("/api/chat", {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ message: q }),
      });
      setMsgs((m) => [...m, { role: "assistant", text: res.answer || "(no answer)", agent: res.agent }]);
    } catch (e) {
      setMsgs((m) => [...m, { role: "assistant", text: "Error: " + e }]);
    } finally { setBusy(false); }
  };

  return (
    <div className="chat">
      <div className="samples">
        {SAMPLES.map((s) => <button key={s} onClick={() => send(s)} disabled={busy}>{s}</button>)}
      </div>
      <div className="messages">
        {msgs.map((m, i) => (
          <div key={i} className={`msg ${m.role}`}>
            {m.agent && <div className="agent">↳ routed to {m.agent}</div>}
            <pre>{m.text}</pre>
          </div>
        ))}
        {busy && <div className="msg assistant"><pre>Thinking… (routing via the Multi-Agent Supervisor)</pre></div>}
      </div>
      <div className="composer">
        <input value={input} onChange={(e) => setInput(e.target.value)}
               onKeyDown={(e) => e.key === "Enter" && send()} placeholder="Ask about sourcing risk or PO analytics…" />
        <button onClick={() => send()} disabled={busy}>Send</button>
      </div>
    </div>
  );
}

export default function App() {
  const [tab, setTab] = useState("viz");
  return (
    <div className="app">
      <header>
        <h1>SAP Purchasing — Sourcing Risk & PO Analytics</h1>
        <nav>
          <button className={tab === "viz" ? "active" : ""} onClick={() => setTab("viz")}>Visualizations</button>
          <button className={tab === "chat" ? "active" : ""} onClick={() => setTab("chat")}>Chat (Agent)</button>
        </nav>
      </header>
      <main>{tab === "viz" ? <Visualizations /> : <Chat />}</main>
      <footer>Powered by Databricks Genie · Agent Bricks Multi-Agent Supervisor · Unity Catalog metric views</footer>
    </div>
  );
}
