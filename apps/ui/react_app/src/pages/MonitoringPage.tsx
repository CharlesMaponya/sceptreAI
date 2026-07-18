import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  Activity, BarChart3, Download, FileCheck2, Filter, Gauge, RefreshCw, Scale,
  Settings2, Waves,
} from "lucide-react";
import { lazy, Suspense, useMemo, useState } from "react";
import { api, getSession, json } from "../api";
import {
  Badge, Button, Card, EmptyState, ErrorState, Loading, Metric, Modal, Notice,
  PageHeader,
} from "../components/ui";
import { formatDate, titleCase } from "../lib";

const PlotlyChart = lazy(() => import("../components/PlotlyChart"));

type HealthStatus = "healthy" | "warning" | "critical" | "unknown";

interface MetricPoint {
  id: string; name: string; kind: string; value: number | null; recorded_at: string;
  sample_count: number | null; higher_is_better: boolean | null; status: string;
  metadata: Record<string, unknown>;
}
interface MetricSeries {
  name: string; kind: string; higher_is_better: boolean | null; points: MetricPoint[];
}
interface MonitoringConfig {
  enabled: boolean; schedule: string; resource_class: string; metrics: string[];
  thresholds: Record<string, { warning: number; critical: number; direction: "above" | "below" }>;
  retraining_enabled: boolean; approval_required: boolean; revision: number;
  updated_at: string | null; updated_by_id: string | null;
}
interface TimelineEvent {
  kind: string; label: string; status: string; occurred_at: string;
  details: Record<string, unknown>;
}
interface MonitoredDeployment {
  project_id: string; project_name: string; deployment_run_id: string;
  model_version_id: string | null; registry_entry_id: string | null;
  model_name: string; model_version: number | null; environment: string;
  task_type: string; deployment_status: string; health_status: HealthStatus;
  deployed_at: string; last_observation_at: string | null; monitoring: MonitoringConfig;
  baseline_metric_name: string | null; baseline_metric_value: number | null;
  metric_series: MetricSeries[]; drift_history: MetricPoint[]; retraining_events: number;
  open_alerts: number; governance_reports: number; timeline: TimelineEvent[];
}
interface MonitoringDashboard {
  scope: string; generated_at: string; deployment_count: number; healthy_count: number;
  attention_count: number; unmonitored_count: number; open_alert_count: number;
  deployments: MonitoredDeployment[];
}
interface GovernanceSummary {
  id: string; project_id: string; deployment_run_id: string; model_version_id: string | null;
  version: number; generated_at: string; evidence_cutoff_at: string;
  generated_by_id: string | null; content_hash: string; json_download_url: string;
  html_download_url: string | null;
}
interface GovernanceReport extends GovernanceSummary { report: Record<string, unknown> }

export function MonitoringPage() {
  const client = useQueryClient();
  const [projectFilter, setProjectFilter] = useState("all");
  const [healthFilter, setHealthFilter] = useState("all");
  const [selected, setSelected] = useState<MonitoredDeployment | null>(null);
  const [panel, setPanel] = useState<"config" | "governance" | null>(null);
  const dashboard = useQuery({
    queryKey: ["monitoring-dashboard"],
    queryFn: () => api<MonitoringDashboard>("/monitoring/dashboard"),
    refetchInterval: 30_000,
  });
  const projects = useMemo(() => Array.from(new Map(
    (dashboard.data?.deployments || []).map((item) => [item.project_id, item.project_name]),
  )), [dashboard.data]);
  const deployments = useMemo(() => (dashboard.data?.deployments || []).filter((item) =>
    (projectFilter === "all" || item.project_id === projectFilter)
    && (healthFilter === "all" || item.health_status === healthFilter)),
  [dashboard.data, healthFilter, projectFilter]);
  const refresh = () => client.invalidateQueries({ queryKey: ["monitoring-dashboard"] });

  if (dashboard.isLoading) return <Loading label="Loading model monitoring…" />;
  if (dashboard.error) return <ErrorState error={dashboard.error} retry={() => dashboard.refetch()} />;
  return <>
    <PageHeader eyebrow="Model portfolio" title="Model metrics"
      description="Performance, drift, retraining, and governance evidence across every deployment you can access."
      action={<Button variant="secondary" onClick={refresh}><RefreshCw size={15} />Refresh evidence</Button>} />
    <section className="monitoring-kpis" aria-label="Monitoring summary">
      <Metric label="Deployments" value={dashboard.data?.deployment_count || 0} hint="across accessible projects" />
      <Metric label="Healthy" value={dashboard.data?.healthy_count || 0} hint="within configured thresholds" />
      <Metric label="Needs attention" value={dashboard.data?.attention_count || 0} hint="warning or critical" />
      <Metric label="Open signals" value={dashboard.data?.open_alert_count || 0} hint="performance or drift" />
      <Metric label="Not monitored" value={dashboard.data?.unmonitored_count || 0} hint="configuration disabled" />
    </section>
    <Card className="monitoring-toolbar">
      <div><Filter size={16} /><span>Focus the portfolio</span></div>
      <label>Project<select value={projectFilter} onChange={(event) => setProjectFilter(event.target.value)}>
        <option value="all">All accessible projects</option>
        {projects.map(([id, name]) => <option value={id} key={id}>{name}</option>)}
      </select></label>
      <label>Health<select value={healthFilter} onChange={(event) => setHealthFilter(event.target.value)}>
        <option value="all">Every state</option><option value="healthy">Healthy</option>
        <option value="warning">Warning</option><option value="critical">Critical</option>
        <option value="unknown">Awaiting evidence</option>
      </select></label>
      <small>Updated {formatDate(dashboard.data?.generated_at)}</small>
    </Card>
    {deployments.length ? <div className="monitoring-list">{deployments.map((deployment) =>
      <DeploymentMonitor key={deployment.deployment_run_id} deployment={deployment}
        configure={() => { setSelected(deployment); setPanel("config"); }}
        govern={() => { setSelected(deployment); setPanel("governance"); }} />)}</div>
      : <Card><EmptyState icon={<Waves />} title="No deployments match this view"
        description="Change the filters, or deploy a registered model before collecting production evidence." /></Card>}
    {selected && panel === "config" && <MonitoringConfigModal deployment={selected}
      close={() => setPanel(null)} saved={() => { setPanel(null); refresh(); }} />}
    {selected && panel === "governance" && <GovernanceModal deployment={selected}
      close={() => setPanel(null)} generated={refresh} />}
  </>;
}

function DeploymentMonitor({ deployment, configure, govern }: {
  deployment: MonitoredDeployment; configure: () => void; govern: () => void;
}) {
  const options = [
    ...deployment.metric_series.map((series) => ({ key: series.name, label: titleCase(series.name), series })),
    ...(deployment.drift_history.length ? [{
      key: "drift_share", label: "Drift share", series: {
        name: "drift_share", kind: "drift", higher_is_better: false,
        points: deployment.drift_history,
      },
    }] : []),
  ];
  const [metricName, setMetricName] = useState(options[0]?.key || "");
  const selected = options.find((item) => item.key === metricName) || options[0];
  const latest = selected?.series.points.at(-1);
  const statusCopy = deployment.health_status === "unknown"
    ? "Awaiting production evidence"
    : deployment.health_status === "healthy" ? "Within configured thresholds"
      : deployment.health_status === "warning" ? "Review recent degradation" : "Action required";
  return <article className={`monitoring-deployment monitoring-deployment--${deployment.health_status}`}>
    <header className="monitoring-deployment__header">
      <div className="monitoring-model-mark"><Gauge /></div>
      <div><span>{deployment.project_name} · {titleCase(deployment.environment)}</span>
        <h2>{deployment.model_name}{deployment.model_version ? ` v${deployment.model_version}` : ""}</h2>
        <p>{titleCase(deployment.task_type)} · deployed {formatDate(deployment.deployed_at)}</p></div>
      <div className="monitoring-health"><Badge status={deployment.health_status} />
        <small>{statusCopy}</small></div>
    </header>
    <div className="monitoring-deployment__body">
      <section className="monitoring-chart-panel">
        <div className="monitoring-chart-panel__head"><div><span>Evidence over time</span>
          <strong>{latest?.value == null ? "No observations" : formatMetric(latest.name, latest.value)}</strong></div>
          {options.length > 0 && <select aria-label={`Metric for ${deployment.model_name}`}
            value={selected?.key || ""} onChange={(event) => setMetricName(event.target.value)}>
            {options.map((option) => <option value={option.key} key={option.key}>{option.label}</option>)}
          </select>}</div>
        {selected?.series.points.length ? <Suspense fallback={<Loading label="Rendering metric history…" />}>
          <PlotlyChart className="monitoring-chart" data={[{
            type: "scatter", mode: "lines+markers", x: selected.series.points.map((point) => point.recorded_at),
            y: selected.series.points.map((point) => point.value), line: { color: "#2854c5", width: 3, shape: "spline" },
            marker: { color: "#fff", line: { color: "#2854c5", width: 2 }, size: 7 },
            hovertemplate: "%{x}<br>%{y:.4f}<extra></extra>",
          }]} layout={{ autosize: true, height: 250, margin: { l: 48, r: 18, t: 18, b: 40 },
            paper_bgcolor: "rgba(0,0,0,0)", plot_bgcolor: "rgba(244,247,252,.7)", showlegend: false,
            xaxis: { showgrid: false }, yaxis: { gridcolor: "#e3e8f1", zeroline: false },
            font: { family: "Manrope Variable, system-ui, sans-serif", color: "#657089", size: 10 },
          }} config={{ displayModeBar: false, responsive: true }} useResizeHandler style={{ width: "100%" }} />
        </Suspense> : <div className="monitoring-no-series"><BarChart3 /><b>No production series yet</b>
          <span>Configure monitoring, then send deployment metrics through the authenticated metrics endpoint.</span></div>}
      </section>
      <aside className="monitoring-evidence-panel">
        <div className="monitoring-evidence-grid">
          <div><span>Training baseline</span><strong>{deployment.baseline_metric_value == null ? "Not recorded"
            : `${titleCase(deployment.baseline_metric_name || "metric")} ${deployment.baseline_metric_value.toFixed(4)}`}</strong></div>
          <div><span>Drift checks</span><strong>{deployment.drift_history.length}</strong></div>
          <div><span>Retraining events</span><strong>{deployment.retraining_events}</strong></div>
          <div><span>Governance reports</span><strong>{deployment.governance_reports}</strong></div>
        </div>
        <div className="monitoring-mode"><span><Activity size={14} />Monitoring mode</span>
          <strong>{deployment.monitoring.enabled ? titleCase(deployment.monitoring.schedule) : "Disabled"}</strong>
          <small>{titleCase(deployment.monitoring.resource_class)} Job class · revision {deployment.monitoring.revision}</small></div>
        <div className="monitoring-actions"><Button variant="secondary" onClick={configure}>
          <Settings2 size={15} />Configure</Button><Button onClick={govern}>
          <FileCheck2 size={15} />Governance & audit</Button></div>
      </aside>
    </div>
    <footer className="monitoring-timeline"><span>Recent lifecycle</span>
      {deployment.timeline.length ? deployment.timeline.slice(0, 4).map((event, index) =>
        <div key={`${event.kind}-${event.occurred_at}-${index}`}><i className={`timeline-dot timeline-dot--${event.status}`} />
          <span><b>{event.label}</b><small>{titleCase(event.kind)} · {formatDate(event.occurred_at)}</small></span>
          <Badge status={event.status} /></div>)
        : <small>No deployment events have been recorded.</small>}
    </footer>
  </article>;
}

function MonitoringConfigModal({ deployment, close, saved }: {
  deployment: MonitoredDeployment; close: () => void; saved: () => void;
}) {
  const [enabled, setEnabled] = useState(deployment.monitoring.enabled);
  const [schedule, setSchedule] = useState(deployment.monitoring.schedule);
  const [resourceClass, setResourceClass] = useState(deployment.monitoring.resource_class);
  const [retraining, setRetraining] = useState(deployment.monitoring.retraining_enabled);
  const save = useMutation({
    mutationFn: () => api(`/projects/${deployment.project_id}/operations/deployments/${deployment.deployment_run_id}/monitoring/config`,
      json("PUT", { ...deployment.monitoring, enabled, schedule, resource_class: resourceClass,
        retraining_enabled: retraining, revision: undefined, updated_at: undefined, updated_by_id: undefined })),
    onSuccess: saved,
  });
  return <Modal title="Configure deployment monitoring"
    description={`Control cadence and compute for ${deployment.model_name}. Threshold changes are revisioned.`}
    onClose={close}><div className="stack monitoring-config-form">
      <label className="check-row"><span><b>Monitoring enabled</b><small>Collect and evaluate deployment-linked evidence.</small></span>
        <input type="checkbox" checked={enabled} onChange={(event) => setEnabled(event.target.checked)} /></label>
      <div className="form-grid"><label>Evaluation cadence<select value={schedule} onChange={(event) => setSchedule(event.target.value)}>
        <option value="manual">Manual</option><option value="hourly">Hourly</option>
        <option value="daily">Daily</option><option value="weekly">Weekly</option>
      </select></label><label>Monitoring Job size<select value={resourceClass} onChange={(event) => setResourceClass(event.target.value)}>
        <option value="small">Small</option><option value="standard">Standard</option>
        <option value="large">Large</option><option value="xlarge">Extra large</option>
      </select></label></div>
      <Notice><Scale size={16} />Job size controls bounded drift/report workloads. API and monitoring-worker HPA remain operator-managed cluster capabilities.</Notice>
      <label className="check-row"><span><b>Allow retraining proposals</b>
        <small>Threshold breaches may propose retraining; promotion still requires approval.</small></span>
        <input type="checkbox" checked={retraining} onChange={(event) => setRetraining(event.target.checked)} /></label>
      {save.error && <Notice tone="danger">{save.error.message}</Notice>}
      <div className="modal__actions"><Button variant="ghost" onClick={close}>Cancel</Button>
        <Button loading={save.isPending} onClick={() => save.mutate()}>Save monitoring policy</Button></div>
    </div></Modal>;
}

function GovernanceModal({ deployment, close, generated }: {
  deployment: MonitoredDeployment; close: () => void; generated: () => void;
}) {
  const [current, setCurrent] = useState<GovernanceReport | null>(null);
  const path = `/projects/${deployment.project_id}/operations/deployments/${deployment.deployment_run_id}/governance/reports`;
  const reports = useQuery({ queryKey: ["governance-reports", deployment.deployment_run_id],
    queryFn: () => api<GovernanceSummary[]>(path) });
  const generate = useMutation({ mutationFn: () => api<GovernanceReport>(path, json("POST")),
    onSuccess: (report) => { setCurrent(report); reports.refetch(); generated(); } });
  const open = useMutation({ mutationFn: (id: string) => api<GovernanceReport>(`${path}/${id}`), onSuccess: setCurrent });
  return <Modal title="Governance & audit"
    description={`Versioned evidence for ${deployment.model_name} and deployment ${deployment.deployment_run_id.slice(0, 8)}.`}
    onClose={close}><div className="governance-modal">
      <div className="governance-rail"><div><span>Report history</span>
        <Button loading={generate.isPending} onClick={() => generate.mutate()}><FileCheck2 size={15} />Generate snapshot</Button></div>
        {generate.error && <Notice tone="danger">{generate.error.message}</Notice>}
        {reports.isLoading ? <Loading label="Loading reports…" /> : reports.data?.length
          ? reports.data.map((report) => <button key={report.id} onClick={() => open.mutate(report.id)}
            className={current?.id === report.id ? "active" : ""}><FileCheck2 /><span><b>Version {report.version}</b>
              <small>Evidence to {formatDate(report.evidence_cutoff_at)}</small></span></button>)
          : <div className="governance-empty"><Scale /><b>No report snapshots</b><span>Generate the first immutable evidence package.</span></div>}
      </div>
      <div className="governance-document">
        {open.isPending ? <Loading label="Opening governance evidence…" /> : current ? <>
          <header><div><span>Governance report · v{current.version}</span><h3>{deployment.model_name}</h3>
            <small>SHA-256 {current.content_hash}</small></div><div>
              <Button variant="secondary" onClick={() => downloadGovernance(current.json_download_url, `governance-v${current.version}.json`)}><Download size={14} />JSON</Button>
              {current.html_download_url && <Button variant="secondary" onClick={() => downloadGovernance(current.html_download_url!, `governance-v${current.version}.html`)}><Download size={14} />HTML</Button>}
            </div></header>
          <div className="governance-sections">{Object.entries(current.report).filter(([name]) => name !== "schema_version").map(([name, value]) =>
            <details key={name} open={name === "model_development"}><summary>{titleCase(name)}</summary>
              <GovernanceValue value={value} /></details>)}</div>
        </> : <div className="governance-placeholder"><FileCheck2 /><h3>Select or generate a report</h3>
          <p>The snapshot assembles model development, preprocessing, training, tuning, explainability, drift, monitoring, and leakage evidence.</p></div>}
      </div>
      <div className="modal__actions governance-close"><Button variant="ghost" onClick={close}>Close</Button></div>
    </div></Modal>;
}

function GovernanceValue({ value }: { value: unknown }) {
  if (Array.isArray(value)) return value.length ? <ol>{value.map((item, index) =>
    <li key={index}><GovernanceValue value={item} /></li>)}</ol> : <span className="muted">No evidence recorded</span>;
  if (value && typeof value === "object") return <dl>{Object.entries(value as Record<string, unknown>).map(([key, item]) =>
    <div key={key}><dt>{titleCase(key)}</dt><dd><GovernanceValue value={item} /></dd></div>)}</dl>;
  if (value == null) return <span className="muted">Not recorded</span>;
  return <span>{String(value)}</span>;
}

async function downloadGovernance(path: string, filename: string) {
  const token = getSession()?.tokens.access_token;
  const response = await fetch(path, { headers: token ? { Authorization: `Bearer ${token}` } : {} });
  if (!response.ok) throw new Error(`Report download failed (${response.status}).`);
  const url = URL.createObjectURL(await response.blob());
  const anchor = document.createElement("a"); anchor.href = url; anchor.download = filename; anchor.click();
  URL.revokeObjectURL(url);
}

function formatMetric(name: string, value: number) {
  if (name.includes("rate") || name.includes("share") || name.includes("accuracy") || name.includes("f1")) {
    return `${(value * 100).toFixed(1)}%`;
  }
  if (name.includes("latency") || name.endsWith("_ms")) return `${value.toFixed(0)} ms`;
  return value.toLocaleString(undefined, { maximumFractionDigits: 4 });
}
