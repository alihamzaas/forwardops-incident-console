import { useEffect, useRef, useState } from "react";
import Plotly from "plotly.js-dist-min";

import {
  getGraphShape,
  getHealth,
  loadSampleDataset,
  streamAnalysis,
  uploadDataset,
} from "./api.js";

const DEFAULT_QUERY =
  "Which account and workload has the highest operational risk, and what trend should we investigate first?";

export default function App() {
  const [health, setHealth] = useState(null);
  const [graph, setGraph] = useState(null);
  const [session, setSession] = useState(null);
  const [query, setQuery] = useState(DEFAULT_QUERY);
  const [events, setEvents] = useState([]);
  const [analysis, setAnalysis] = useState(emptyAnalysis());
  const [busy, setBusy] = useState(false);
  const [notice, setNotice] = useState("Load the sample dataset or upload a CSV/Excel file to begin.");

  useEffect(() => {
    void refreshRuntime();
  }, []);

  async function refreshRuntime() {
    try {
      const [healthPayload, graphPayload] = await Promise.all([getHealth(), getGraphShape()]);
      setHealth(healthPayload);
      setGraph(graphPayload);
    } catch (error) {
      setNotice(`Backend is not reachable yet: ${error.message}`);
    }
  }

  async function handleLoadSample() {
    setBusy(true);
    setNotice("Loading bundled workload metrics and materializing the lakehouse dataset...");
    try {
      const payload = await loadSampleDataset();
      setSession(payload);
      setEvents([]);
      setAnalysis(emptyAnalysis());
      setNotice("Sample dataset loaded. The upload flow, profiler, SQLite storage, and PySpark/pandas materialization ran.");
    } catch (error) {
      setNotice(`Sample load failed: ${error.message}`);
    } finally {
      setBusy(false);
    }
  }

  async function handleUpload(event) {
    const file = event.target.files?.[0];
    if (!file) {
      return;
    }
    setBusy(true);
    setNotice(`Uploading ${file.name} and profiling the dataset...`);
    try {
      const payload = await uploadDataset(file);
      setSession(payload);
      setEvents([]);
      setAnalysis(emptyAnalysis());
      setNotice(`${file.name} loaded. Ask a question and run the copilot graph.`);
    } catch (error) {
      setNotice(`Upload failed: ${error.message}`);
    } finally {
      event.target.value = "";
      setBusy(false);
    }
  }

  async function handleAnalyze() {
    if (!session?.session_id || !query.trim()) {
      return;
    }
    setBusy(true);
    setEvents([]);
    setAnalysis(emptyAnalysis());
    setNotice("Streaming LangGraph stages from the FastAPI SSE endpoint...");
    try {
      await streamAnalysis({
        sessionId: session.session_id,
        query,
        onEvent: handleCopilotEvent,
      });
      setNotice("Analysis complete. Review the plan, generated code, sandbox trace, chart, and summary.");
    } catch (error) {
      setNotice(`Analysis failed: ${error.message}`);
    } finally {
      setBusy(false);
    }
  }

  function handleCopilotEvent({ event, payload }) {
    const stage = payload.stage || event;
    const item = {
      id: `${stage}-${Date.now()}-${Math.random().toString(16).slice(2)}`,
      stage,
      payload,
      createdAt: new Date().toLocaleTimeString(),
    };
    setEvents((current) => [...current, item]);
    setAnalysis((current) => reduceAnalysis(current, stage, payload));
  }

  const profile = session?.profile;
  const lakeResult = profile?.lake_result;

  return (
    <div className="app-shell">
      <header className="hero">
        <div>
          <p className="eyebrow">Personal full-stack AI/data project</p>
          <h1>Agentic Data Copilot</h1>
          <p className="hero-text">
            Upload a dataset, ask an analysis question, and watch a LangGraph-style copilot plan the work,
            generate pandas/Plotly code, execute it in a guarded sandbox, and stream the results back to the browser.
          </p>
        </div>
        <RuntimeCard health={health} />
      </header>

      <section className="notice">{notice}</section>

      <main className="layout">
        <section className="panel intake-panel">
          <PanelHeader
            title="Dataset Intake"
            subtitle="FastAPI upload, profiling, SQLite session storage, and PySpark lake materialization."
          />
          <div className="intake-actions">
            <button className="button primary" onClick={handleLoadSample} disabled={busy}>
              Load sample dataset
            </button>
            <label className="button ghost">
              Upload CSV or Excel
              <input type="file" accept=".csv,.xlsx,.xls" onChange={handleUpload} disabled={busy} />
            </label>
          </div>

          <ProfileCard profile={profile} lakeResult={lakeResult} />
        </section>

        <section className="panel graph-panel">
          <PanelHeader
            title="LangGraph Pipeline"
            subtitle="Planner, Analyst, Coder, Executor, and Summarizer stages are explicit and inspectable."
          />
          <GraphView graph={graph} events={events} />
        </section>

        <section className="panel question-panel">
          <PanelHeader
            title="Ask The Copilot"
            subtitle="The backend streams each stage as an SSE response from the analysis endpoint."
          />
          <textarea value={query} onChange={(event) => setQuery(event.target.value)} />
          <button className="button secondary" onClick={handleAnalyze} disabled={busy || !session?.session_id}>
            Run analysis graph
          </button>
        </section>

        <section className="panel plan-panel">
          <PanelHeader title="Plan And Findings" subtitle="Claude is used when ANTHROPIC_API_KEY is configured; otherwise deterministic fallback keeps demos runnable." />
          <ResultBlock label="Planner output" value={analysis.plan} />
          <ResultBlock label="Analyst notes" value={analysis.findings} />
        </section>

        <section className="panel sandbox-panel">
          <PanelHeader
            title="Visible Code Sandbox"
            subtitle="Generated code, policy validation, isolated execution, stdout, and retry metadata."
          />
          <SandboxPanel analysis={analysis} />
        </section>

        <section className="panel chart-panel">
          <PanelHeader title="Generated Chart" subtitle="The sandbox exposes Plotly JSON when generated code assigns a figure to fig." />
          <ChartPanel chartJson={analysis.chartJson} />
        </section>

        <section className="panel summary-panel">
          <PanelHeader title="Readable Summary" subtitle="Summarizer converts raw execution output into a concise explanation." />
          <ResultBlock label="Summary" value={analysis.summary} />
          <EventTimeline events={events} />
        </section>
      </main>
    </div>
  );
}

function RuntimeCard({ health }) {
  const features = health?.features || {};
  return (
    <aside className="runtime-card">
      <span>Runtime</span>
      <strong>{health?.status === "ok" ? "Backend online" : "Checking..."}</strong>
      <div className="feature-grid">
        {Object.entries(features).map(([name, enabled]) => (
          <div className={enabled ? "feature enabled" : "feature"} key={name}>
            {name.replaceAll("_", " ")}
          </div>
        ))}
      </div>
    </aside>
  );
}

function PanelHeader({ title, subtitle }) {
  return (
    <div className="panel-header">
      <h2>{title}</h2>
      <p>{subtitle}</p>
    </div>
  );
}

function ProfileCard({ profile, lakeResult }) {
  if (!profile) {
    return (
      <div className="empty-state">
        No dataset loaded yet. Use the sample for a quick walkthrough or upload your own small CSV.
      </div>
    );
  }
  return (
    <div className="profile-card">
      <div>
        <span>File</span>
        <strong>{profile.filename}</strong>
      </div>
      <div>
        <span>Rows</span>
        <strong>{profile.rows}</strong>
      </div>
      <div>
        <span>Columns</span>
        <strong>{profile.columns}</strong>
      </div>
      <div>
        <span>Lake engine</span>
        <strong>{lakeResult?.engine || "pending"}</strong>
      </div>
      <div className="wide">
        <span>Numeric columns</span>
        <p>{profile.numeric_columns?.join(", ") || "None detected"}</p>
      </div>
      <div className="wide">
        <span>Categorical columns</span>
        <p>{profile.categorical_columns?.join(", ") || "None detected"}</p>
      </div>
    </div>
  );
}

function GraphView({ graph, events }) {
  const completedStages = new Set(events.map((item) => item.stage));
  const nodes = graph?.nodes || ["Planner", "Analyst", "Coder", "Executor", "Summarizer"];
  return (
    <div className="graph-view">
      {nodes.map((node, index) => {
        const stage = node.toLowerCase();
        return (
          <div className={completedStages.has(stage) ? "graph-node complete" : "graph-node"} key={node}>
            <div className="node-index">{index + 1}</div>
            <strong>{node}</strong>
            <p>{graph?.notes?.[node] || "Stage metadata loads from the backend."}</p>
          </div>
        );
      })}
    </div>
  );
}

function ResultBlock({ label, value }) {
  return (
    <div className="result-block">
      <span>{label}</span>
      <pre>{value || "Waiting for this stage..."}</pre>
    </div>
  );
}

function SandboxPanel({ analysis }) {
  const execution = analysis.execution || {};
  const traces = execution.traces || [];
  return (
    <div className="sandbox-grid">
      <div className="result-block code-block">
        <span>Generated code</span>
        <pre>{analysis.code || "Coder stage has not run yet."}</pre>
      </div>
      <div className="trace-list">
        {traces.length === 0 ? (
          <div className="empty-state">Sandbox traces will appear after execution.</div>
        ) : (
          traces.map((trace, index) => (
            <div className="trace-item" key={`${trace.step}-${index}`}>
              <strong>{trace.step}</strong>
              <p>{trace.message}</p>
              {Object.keys(trace.detail || {}).length > 0 ? <code>{JSON.stringify(trace.detail)}</code> : null}
            </div>
          ))
        )}
      </div>
      <ResultBlock label="stdout" value={execution.stdout} />
      {execution.error ? <ResultBlock label="error" value={execution.error} /> : null}
      {execution.retry?.attempted ? (
        <div className="retry-note">Retry repair ran after the first generated code attempt failed.</div>
      ) : null}
    </div>
  );
}

function ChartPanel({ chartJson }) {
  const ref = useRef(null);
  const [error, setError] = useState("");

  useEffect(() => {
    if (!chartJson || !ref.current) {
      return undefined;
    }
    const node = ref.current;
    try {
      const figure = JSON.parse(chartJson);
      Plotly.react(node, figure.data || [], figure.layout || {}, {
        displayModeBar: false,
        responsive: true,
      });
      setError("");
    } catch (chartError) {
      setError(chartError.message);
    }
    return () => Plotly.purge(node);
  }, [chartJson]);

  if (!chartJson) {
    return <div className="empty-state chart-empty">No chart yet. Ask for a trend, distribution, or comparison.</div>;
  }
  return (
    <>
      <div ref={ref} className="plot-shell" />
      {error ? <p className="chart-error">{error}</p> : null}
    </>
  );
}

function EventTimeline({ events }) {
  return (
    <div className="event-timeline">
      <h3>Streamed Events</h3>
      {events.length === 0 ? (
        <div className="empty-state">No SSE events yet.</div>
      ) : (
        events.map((item) => (
          <div className="event-row" key={item.id}>
            <span>{item.createdAt}</span>
            <strong>{item.stage}</strong>
          </div>
        ))
      )}
    </div>
  );
}

function reduceAnalysis(current, stage, payload) {
  const data = payload.data || {};
  if (stage === "planner") {
    return { ...current, plan: data.plan || current.plan };
  }
  if (stage === "analyst") {
    return { ...current, findings: data.findings || current.findings };
  }
  if (stage === "coder") {
    return { ...current, code: data.code || current.code };
  }
  if (stage === "executor") {
    const execution = data.execution || current.execution;
    return {
      ...current,
      code: data.code || current.code,
      execution,
      chartJson: payload.chart_json || execution?.chart_json || current.chartJson,
    };
  }
  if (stage === "summarizer") {
    return { ...current, summary: data.summary || current.summary };
  }
  return current;
}

function emptyAnalysis() {
  return {
    plan: "",
    findings: "",
    code: "",
    execution: null,
    chartJson: null,
    summary: "",
  };
}
