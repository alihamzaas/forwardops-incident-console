const state = {
  dashboard: null,
  selectedIncidentId: null,
  liveSteps: [],
  latestReport: null,
  pendingPipeline: false,
  pendingAgent: false,
  demoMode: false,
};

const elements = {
  metricCards: document.getElementById("metric-cards"),
  customerTable: document.getElementById("customer-table"),
  incidentList: document.getElementById("incident-list"),
  incidentDetail: document.getElementById("incident-detail"),
  agentReport: document.getElementById("agent-report"),
  activityLog: document.getElementById("activity-log"),
  pipelineStatus: document.getElementById("pipeline-status"),
  selectedIncidentLabel: document.getElementById("selected-incident-label"),
  refreshButton: document.getElementById("refresh-dashboard"),
  pipelineButton: document.getElementById("run-pipeline"),
  agentButton: document.getElementById("run-agent"),
};

void init();

async function init() {
  bindEvents();
  await refreshDashboard();
  if (!state.demoMode) {
    connectEventStream();
  }
}

function bindEvents() {
  elements.refreshButton.addEventListener("click", () => refreshDashboard(true));
  elements.pipelineButton.addEventListener("click", runPipeline);
  elements.agentButton.addEventListener("click", launchTriage);
}

async function refreshDashboard(showNotice = false) {
  try {
    const payload = await getJson(urlFor("api/dashboard"));
    state.dashboard = payload;
    state.demoMode = false;
  } catch (error) {
    const payload = await getJson(urlFor("demo/dashboard.json"));
    state.dashboard = payload;
    state.demoMode = true;
  }

  const incidentIds = (state.dashboard.incidents || []).map((incident) => incident.incident_id);
  if (!state.selectedIncidentId || !incidentIds.includes(state.selectedIncidentId)) {
    state.selectedIncidentId = state.dashboard.incidents[0]?.incident_id ?? null;
    state.latestReport = null;
    state.liveSteps = [];
  }

  updateActionState();
  render();

  if (showNotice) {
    setPipelineStatus(state.demoMode
      ? "GitHub Pages demo refreshed from static data."
      : "Dashboard refreshed from the live local control plane.");
  } else if (state.demoMode) {
    setPipelineStatus("Running in GitHub Pages demo mode. Use local mode for live REST + SSE.");
  } else {
    setPipelineStatus("Live control plane connected. REST and SSE are active.");
  }
}

async function runPipeline() {
  state.pendingPipeline = true;
  updateActionState();

  if (state.demoMode) {
    prependActivity("pipeline", "Demo pipeline replay", "Simulating a hosted refresh using the static demo dataset.");
    setPipelineStatus("Replaying the GitHub Pages demo pipeline...");
    renderActivityLog();

    window.setTimeout(() => {
      state.pendingPipeline = false;
      updateActionState();
      prependActivity("pipeline", "Demo pipeline complete", "Static demo data has been reloaded.");
      setPipelineStatus("Demo pipeline replay complete.");
      renderActivityLog();
    }, 900);
    return;
  }

  setPipelineStatus("Rebuilding parquet features and refreshing customer health views...");
  await postJson(urlFor("api/pipeline/run"), {});
}

async function launchTriage() {
  const incident = selectedIncident();
  if (!incident) {
    return;
  }

  state.pendingAgent = true;
  state.liveSteps = [];
  state.latestReport = null;
  updateActionState();

  if (state.demoMode) {
    setPipelineStatus(`Launching hosted demo triage for ${incident.customer_name} / ${incident.workload_name}...`);
    simulateDemoTriage(incident);
    return;
  }

  setPipelineStatus(`Launching triage agent for ${incident.customer_name} / ${incident.workload_name}...`);
  await postJson(urlFor("api/agent/triage"), { incident_id: incident.incident_id });
  renderAgentReport();
}

function connectEventStream() {
  const source = new EventSource(urlFor("api/events"));

  source.addEventListener("pipeline", async (event) => {
    const payload = JSON.parse(event.data);
    prependActivity("pipeline", "Pipeline update", payload.message || "Feature pipeline emitted an update.");
    setPipelineStatus(payload.message || "Pipeline update received.");

    if (payload.status === "completed" || payload.status === "failed") {
      state.pendingPipeline = false;
      updateActionState();
      await refreshDashboard();
    } else {
      renderActivityLog();
    }
  });

  source.addEventListener("agent_status", (event) => {
    const payload = JSON.parse(event.data);
    prependActivity("agent_status", "Agent triage started", payload.message);
    setPipelineStatus(payload.message);
    renderActivityLog();
  });

  source.addEventListener("agent_step", (event) => {
    const payload = JSON.parse(event.data);
    prependActivity("agent_step", payload.step.title, payload.step.detail);

    if (payload.incident_id === state.selectedIncidentId) {
      state.liveSteps = [...state.liveSteps, payload.step];
      renderAgentReport();
    }
    renderActivityLog();
  });

  source.addEventListener("agent_complete", async (event) => {
    const payload = JSON.parse(event.data);
    prependActivity("agent_complete", "Agent triage completed", payload.report.executive_summary);

    state.pendingAgent = false;
    updateActionState();
    if (payload.incident_id === state.selectedIncidentId) {
      state.latestReport = payload.report;
      state.liveSteps = [];
      renderAgentReport();
    }
    setPipelineStatus(`Agent triage finished for ${payload.report.customer_name}.`);
    await refreshDashboard();
  });

  source.addEventListener("agent_error", (event) => {
    const payload = JSON.parse(event.data);
    state.pendingAgent = false;
    updateActionState();
    prependActivity("agent_error", "Agent triage failed", payload.message);
    setPipelineStatus(payload.message);
    renderActivityLog();
  });

  source.onerror = () => {
    if (!state.demoMode) {
      setPipelineStatus("Live connection dropped. Refresh to reconnect or use the hosted demo snapshot.");
    }
  };
}

function simulateDemoTriage(incident) {
  const report = buildDemoReport(incident);
  const steps = report.steps || [];
  prependActivity("agent_status", "Demo triage started", `Generating a hosted demo response for ${incident.customer_name}.`);
  renderActivityLog();

  let index = 0;
  const tick = () => {
    if (index < steps.length) {
      const step = steps[index];
      state.liveSteps = [...state.liveSteps, step];
      prependActivity("agent_step", step.title, step.detail);
      renderAgentReport();
      renderActivityLog();
      index += 1;
      window.setTimeout(tick, 550);
      return;
    }

    state.pendingAgent = false;
    state.latestReport = report;
    state.liveSteps = [];
    updateActionState();
    prependActivity("agent_complete", "Demo triage completed", report.executive_summary);
    setPipelineStatus(`Demo triage finished for ${report.customer_name}.`);
    renderAgentReport();
    renderActivityLog();
  };

  window.setTimeout(tick, 350);
}

function buildDemoReport(incident) {
  const customer = (state.dashboard.customers || []).find((item) => item.customer_id === incident.customer_id) || {
    customer_health_score: 60,
    open_incidents: 2,
  };

  const runbooks = {
    queue_saturation: {
      rootCause: "The workload contended with other production jobs and never acquired enough reserved slots.",
      actions: [
        "Move the workload to a reserved queue during the customer SLA window.",
        "Shift low-priority backfills out of the same time block.",
        "Keep the customer updated because the latency is driven by capacity contention, not bad source data.",
      ],
      deployment: [
        "Reserve capacity in staging for the affected queue.",
        "Replay one representative batch to confirm runtime improvement.",
        "Promote the queue policy before the next customer window.",
      ],
    },
    skewed_shuffle: {
      rootCause: "Uneven partition distribution is forcing large shuffle retries on a narrow slice of keys.",
      actions: [
        "Salt the hot key and raise shuffle partition count for the affected workload.",
        "Replay only the skewed partition window to restore freshness faster.",
        "Capture a skew snapshot for the customer engineering review.",
      ],
      deployment: [
        "Apply the key-salting change in staging.",
        "Validate the replay on the heaviest partition window.",
        "Roll forward once shuffle retries normalize.",
      ],
    },
    executor_oom: {
      rootCause: "Executor memory is undersized for a wide join and the spill curve climbed sharply before failure.",
      actions: [
        "Pin the workload to a higher-memory executor profile.",
        "Increase shuffle partitions before the hot stage.",
        "Replay only the failed slice instead of the full batch.",
      ],
      deployment: [
        "Promote the memory profile in staging.",
        "Validate spill and retry metrics on a targeted replay.",
        "Ship the configuration before the next scheduled run.",
      ],
    },
    metadata_timeout: {
      rootCause: "The workload stalled on metadata or catalog access during the publish path.",
      actions: [
        "Retry against a warm catalog replica.",
        "Move the metadata-heavy publish away from the busiest minute of the window.",
        "Keep the customer posted because recovery can happen without data loss.",
      ],
      deployment: [
        "Validate catalog connectivity in staging.",
        "Shift the publish window and rerun the stage.",
        "Roll out the retry configuration once downstream reads reopen cleanly.",
      ],
    },
    hot_partition: {
      rootCause: "A single hot partition is dominating merge throughput and triggering repeated write retries.",
      actions: [
        "Compact the hotspot partition before the next merge.",
        "Split the merge scope into smaller micro-batches.",
        "Validate downstream reads before widening the backfill.",
      ],
      deployment: [
        "Apply the partition split in staging.",
        "Run a micro-batch backfill on the hotspot.",
        "Promote after merge latency stabilizes.",
      ],
    },
  };

  const template = runbooks[incident.failure_category] || runbooks.queue_saturation;
  const comparableIncidents = (state.dashboard.incidents || [])
    .filter((item) => item.failure_category === incident.failure_category && item.incident_id !== incident.incident_id)
    .slice(0, 3)
    .map((item) => ({
      customer_name: item.customer_name,
      workload_name: item.workload_name,
      severity: item.severity,
      event_ts: item.event_ts,
    }));

  const steps = [
    {
      stage: "planner",
      title: "Framed the customer issue",
      detail: `${incident.customer_name} has a ${incident.severity} severity issue on ${incident.workload_name} with reliability ${incident.reliability_score}.`,
      metrics: {
        queue_depth: incident.queue_depth,
        latency_over_sla_min: incident.latency_over_sla_min,
      },
    },
    {
      stage: "blast-radius",
      title: "Measured customer blast radius",
      detail: `Account health is ${customer.customer_health_score} with ${customer.open_incidents} open high-priority incidents.`,
      metrics: {
        customer_health_score: customer.customer_health_score,
        open_incidents: customer.open_incidents,
      },
    },
    {
      stage: "history",
      title: "Pulled comparable failures",
      detail: comparableIncidents.length
        ? `Matched ${comparableIncidents.length} recent incidents with the same failure mode to guide remediation.`
        : "No directly comparable incidents were present in the hosted snapshot.",
      metrics: {
        matches: comparableIncidents.length,
      },
    },
    {
      stage: "runbook",
      title: "Built the recovery plan",
      detail: template.rootCause,
      metrics: {
        top_action: template.actions[0],
      },
    },
    {
      stage: "customer-update",
      title: "Prepared the stakeholder update",
      detail: `We identified the main issue behind ${incident.workload_name} for ${incident.customer_name}. ${template.rootCause} The top remediation step is to ${template.actions[0].toLowerCase()}.`,
      metrics: {},
    },
  ];

  return {
    run_id: `demo-${incident.incident_id}`,
    incident_id: incident.incident_id,
    customer_id: incident.customer_id,
    customer_name: incident.customer_name,
    workload_name: incident.workload_name,
    severity: incident.severity,
    root_cause: template.rootCause,
    executive_summary: `${incident.customer_name} needs a focused fix on ${incident.workload_name}. The likely driver is ${incident.failure_category.replaceAll("_", " ")}, and the fastest path is to ${template.actions[0].toLowerCase()}.`,
    evidence: [
      `Reliability score is ${incident.reliability_score}.`,
      `Queue depth reached ${incident.queue_depth} and memory utilization reached ${incident.memory_utilization_pct}%.`,
      `Latency ran ${incident.latency_over_sla_min} minutes over SLA.`,
      incident.user_impact,
    ],
    next_actions: template.actions,
    deployment_plan: template.deployment,
    customer_update: `We identified the main issue behind ${incident.workload_name} for ${incident.customer_name}: ${template.rootCause} Current account health is ${customer.customer_health_score}, and the top remediation step is to ${template.actions[0].toLowerCase()}.`,
    comparable_incidents: comparableIncidents,
    steps,
  };
}

function render() {
  renderMetrics();
  renderCustomers();
  renderIncidents();
  renderIncidentDetail();
  renderAgentReport();
  renderActivityLog();
}

function renderMetrics() {
  const overview = state.dashboard?.overview;
  const pipelineState = state.dashboard?.pipeline_state ?? {};
  if (!overview) {
    elements.metricCards.innerHTML = emptyState("No overview data is available yet.");
    return;
  }

  const cards = [
    {
      label: "Customers",
      value: overview.customer_count,
      detail: state.demoMode
        ? "Hosted snapshot covering the same customer accounts as the full local app."
        : "Accounts with active telemetry in the current control plane snapshot.",
    },
    {
      label: "Avg Health",
      value: overview.average_health_score,
      detail: "Aggregated customer health score from recent reliability, latency, and incident load.",
    },
    {
      label: "High Priority",
      value: overview.high_priority_incidents,
      detail: "Open incidents currently tagged as high severity.",
    },
    {
      label: "Jobs / 7d",
      value: overview.jobs_last_7d,
      detail: pipelineState.finished_at
        ? `Last pipeline run finished ${formatDateTime(pipelineState.finished_at)}.`
        : "Feature tables will appear once the first pipeline run completes.",
    },
  ];

  elements.metricCards.innerHTML = cards
    .map(
      (card) => `
        <article class="metric-card">
          <span>${card.label}</span>
          <strong>${card.value}</strong>
          <p>${card.detail}</p>
        </article>
      `
    )
    .join("");
}

function renderCustomers() {
  const customers = state.dashboard?.customers ?? [];
  if (!customers.length) {
    elements.customerTable.innerHTML = emptyState("Customer health data will appear after the feature pipeline runs.");
    return;
  }

  const rows = customers
    .map(
      (customer) => `
        <tr>
          <td>
            <strong>${customer.customer_name}</strong>
            <div class="subtle">${customer.cluster_name}</div>
          </td>
          <td><span class="pill risk-${customer.risk_band}">${customer.risk_band}</span></td>
          <td>${customer.customer_health_score}</td>
          <td>${customer.open_incidents}</td>
          <td>${customer.at_risk_workload}</td>
          <td>${customer.success_rate_pct}%</td>
        </tr>
      `
    )
    .join("");

  elements.customerTable.innerHTML = `
    <table>
      <thead>
        <tr>
          <th>Customer</th>
          <th>Risk</th>
          <th>Health</th>
          <th>Open Incidents</th>
          <th>At-Risk Workload</th>
          <th>Success Rate</th>
        </tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>
  `;
}

function renderIncidents() {
  const incidents = state.dashboard?.incidents ?? [];
  if (!incidents.length) {
    elements.incidentList.innerHTML = emptyState("No incidents are waiting in the queue.");
    elements.agentButton.disabled = true;
    return;
  }

  elements.incidentList.innerHTML = incidents
    .map(
      (incident) => `
        <article class="incident-card ${incident.incident_id === state.selectedIncidentId ? "active" : ""}" data-incident-id="${incident.incident_id}">
          <div class="incident-card-header">
            <div>
              <h3>${incident.customer_name}</h3>
              <p>${incident.workload_name}</p>
            </div>
            <span class="pill ${incident.severity}">${incident.severity}</span>
          </div>
          <p>${incident.summary_line}</p>
          <div class="subtle">${formatDateTime(incident.event_ts)} · ${incident.failure_category.replaceAll("_", " ")}</div>
        </article>
      `
    )
    .join("");

  elements.incidentList.querySelectorAll("[data-incident-id]").forEach((node) => {
    node.addEventListener("click", () => {
      state.selectedIncidentId = node.dataset.incidentId;
      state.latestReport = null;
      state.liveSteps = [];
      updateActionState();
      render();
    });
  });
}

function renderIncidentDetail() {
  const incident = selectedIncident();
  if (!incident) {
    elements.incidentDetail.innerHTML = emptyState("Choose an incident to inspect workload signals and customer impact.");
    elements.selectedIncidentLabel.textContent = "Choose an incident";
    return;
  }

  elements.selectedIncidentLabel.textContent = `${incident.customer_name} · ${incident.workload_name}`;
  elements.incidentDetail.innerHTML = `
    <div class="detail-topline">
      <div>
        <h3>${incident.customer_name}</h3>
        <p class="detail-intro">${incident.customer_summary}</p>
      </div>
      <span class="pill ${incident.severity}">${incident.severity}</span>
    </div>

    <div class="detail-grid">
      ${detailMetric("Reliability", incident.reliability_score, 100)}
      ${detailMetric("Queue Pressure", incident.queue_pressure_score, 100)}
      ${detailMetric("Memory Pressure", incident.memory_pressure_score, 100)}
      ${detailMetric("Latency Over SLA", incident.latency_over_sla_min, Math.max(incident.latency_over_sla_min, 30))}
    </div>

    <div class="detail-card">
      <span>Customer Impact</span>
      <p>${incident.user_impact}</p>
    </div>

    <div class="detail-card">
      <span>Recommended Operator Action</span>
      <p>${incident.recommended_action}</p>
    </div>
  `;
}

function renderAgentReport() {
  const incident = selectedIncident();
  if (!incident) {
    elements.agentReport.innerHTML = emptyState("Select an incident and launch the triage agent to see a streamed response plan.");
    return;
  }

  if (state.liveSteps.length) {
    elements.agentReport.innerHTML = `
      <div class="report-topline">
        <div>
          <h3>Agent is working</h3>
          <p class="report-summary">${state.demoMode ? "Simulating the hosted triage flow using static demo data." : "The console is stepping through customer context, comparable failures, and remediation planning."}</p>
        </div>
        <span class="pill medium">streaming</span>
      </div>
      <div class="step-list">
        ${state.liveSteps.map(renderStep).join("")}
      </div>
    `;
    return;
  }

  if (!state.latestReport) {
    elements.agentReport.innerHTML = emptyState(
      `Launch triage for ${incident.customer_name} to generate an operator plan, deployment checklist, and customer update.`
    );
    return;
  }

  const report = state.latestReport;
  elements.agentReport.innerHTML = `
    <div class="report-topline">
      <div>
        <h3>${report.customer_name} triage plan</h3>
        <p class="report-summary">${report.executive_summary}</p>
      </div>
      <span class="pill ${report.severity}">${report.severity}</span>
    </div>

    <div class="report-grid">
      <div class="report-card">
        <span>Root Cause</span>
        <p>${report.root_cause}</p>
      </div>
      <div class="report-card">
        <span>Customer Update</span>
        <p>${report.customer_update}</p>
      </div>
    </div>

    <div class="report-card">
      <span>Evidence</span>
      <ul class="bullet-list">
        ${report.evidence.map((item) => `<li>${item}</li>`).join("")}
      </ul>
    </div>

    <div class="report-card">
      <span>Next Actions</span>
      <ul class="bullet-list">
        ${report.next_actions.map((item) => `<li>${item}</li>`).join("")}
      </ul>
    </div>

    <div class="report-card">
      <span>Deployment Plan</span>
      <ul class="bullet-list">
        ${report.deployment_plan.map((item) => `<li>${item}</li>`).join("")}
      </ul>
    </div>

    <div class="report-card">
      <span>Comparable Incidents</span>
      ${
        report.comparable_incidents.length
          ? `<ul class="bullet-list">${report.comparable_incidents
              .map(
                (item) =>
                  `<li>${item.customer_name} / ${item.workload_name} at ${formatDateTime(item.event_ts)} (${item.severity})</li>`
              )
              .join("")}</ul>`
          : "<p>No directly comparable incidents were present in the recent queue.</p>"
      }
    </div>
  `;
}

function renderActivityLog() {
  const activity = state.dashboard?.recent_activity ?? [];
  if (!activity.length) {
    elements.activityLog.innerHTML = emptyState("Platform activity will stream here when the pipeline or triage agent is active.");
    return;
  }

  elements.activityLog.innerHTML = activity
    .map(
      (item) => `
        <article class="activity-item">
          <strong>${item.title}</strong>
          <p>${item.detail}</p>
          <time>${formatDateTime(item.timestamp)}</time>
        </article>
      `
    )
    .join("");
}

function updateActionState() {
  const hasIncident = Boolean(selectedIncident());
  elements.agentButton.disabled = !hasIncident || state.pendingAgent;
  elements.pipelineButton.disabled = state.pendingPipeline;
}

function selectedIncident() {
  return (state.dashboard?.incidents ?? []).find((incident) => incident.incident_id === state.selectedIncidentId) || null;
}

function prependActivity(kind, title, detail) {
  if (!state.dashboard) {
    return;
  }

  state.dashboard.recent_activity = [
    {
      kind,
      title,
      detail,
      timestamp: new Date().toISOString(),
    },
    ...(state.dashboard.recent_activity ?? []),
  ].slice(0, 12);
}

function setPipelineStatus(message) {
  elements.pipelineStatus.textContent = message;
}

function detailMetric(label, value, scaleMax) {
  const safeValue = Number.isFinite(value) ? value : 0;
  const width = Math.min(100, Math.max(6, (safeValue / scaleMax) * 100));
  return `
    <div class="detail-card">
      <span>${label}</span>
      <strong>${roundValue(safeValue)}</strong>
      <div class="metric-bar">
        <div class="metric-bar-fill" style="width: ${width}%"></div>
      </div>
    </div>
  `;
}

function renderStep(step) {
  const metrics = Object.entries(step.metrics || {})
    .map(([key, value]) => `<span>${key}: ${value}</span>`)
    .join("");

  return `
    <article class="step-item">
      <h4>${step.title}</h4>
      <p>${step.detail}</p>
      ${metrics ? `<div class="inline-metrics">${metrics}</div>` : ""}
    </article>
  `;
}

function emptyState(text) {
  return `<div class="empty-state">${text}</div>`;
}

function formatDateTime(value) {
  if (!value) {
    return "Not available";
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }
  return parsed.toLocaleString([], {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
}

function roundValue(value) {
  return Math.round(value * 10) / 10;
}

function urlFor(path) {
  return new URL(path, window.location.href).toString();
}

async function getJson(url) {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Request failed: ${response.status}`);
  }
  return response.json();
}

async function postJson(url, payload) {
  const response = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!response.ok) {
    throw new Error(`Request failed: ${response.status}`);
  }
  return response.json();
}
