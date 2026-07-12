import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  Activity, BarChart3, BrainCircuit, ChevronDown, ChevronRight, CircleStop, Cpu,
  FileCheck2, FileSpreadsheet, FileText, Gauge, MemoryStick, Play, Plus, RefreshCw,
  Rocket, TerminalSquare, Trophy, Upload,
} from "lucide-react";
import { lazy, Suspense, useEffect, useMemo, useState } from "react";
import type { ReactNode } from "react";
import { Link, useParams } from "react-router-dom";
import { api, json, uploadFormData } from "../api";
import {
  Badge, Button, Card, EmptyState, ErrorState, Loading, Metric, Modal, Notice, PageHeader,
} from "../components/ui";
import { formatBytes, formatDate, titleCase } from "../lib";
import type {
  Dataset, DatasetVersion, Estimator, Leaderboard, ModelRun, TaskType, TrainingResourceUsage,
} from "../types";

type Analysis = ModelRun & { run_name: string | null };
interface AnalysisResult {
  run_id: string; status: string; model_name: string; metrics: Record<string, number>;
  diagnostics: Record<string, unknown>;
  feature_importance: Array<{ feature: string; mean_absolute_shap?: number; contribution_percent?: number }>;
  artifacts: Array<{ id: string; name: string; kind: string; byte_size: number | null }>;
}
interface Logs { run_id: string; status: string; lines: string[] }
interface VersionOption { id: string; label: string; columns: string[] }
interface DatasetUploadResult { dataset: Dataset; version: DatasetVersion }
const PlotlyChart = lazy(() => import("../components/PlotlyChart"));

export function RunsPage() {
  const { projectId = "" } = useParams();
  const client = useQueryClient();
  const [selectedId, setSelectedId] = useState("");
  const runs = useQuery({
    queryKey: ["runs", projectId],
    queryFn: () => api<ModelRun[]>(`/projects/${projectId}/training/runs`),
    refetchInterval: (query) =>
      query.state.data?.some((run) => ["queued", "precheck_running", "running"].includes(run.status))
        ? 3000 : false,
  });
  useEffect(() => {
    if (runs.data?.length && !runs.data.some((run) => run.id === selectedId)) {
      setSelectedId(runs.data[0].id);
    }
  }, [runs.data, selectedId]);
  const selected = runs.data?.find((run) => run.id === selectedId);
  if (runs.isLoading) return <Loading />;
  if (runs.error) return <ErrorState error={runs.error} retry={() => runs.refetch()} />;
  return <>
    <PageHeader eyebrow="Evidence workspace" title="Results & validation"
      description="Compare candidates, inspect diagnostics, and challenge a model before promotion." />
    {!runs.data?.length
      ? <Card><EmptyState icon={<BarChart3 />} title="No experiment results yet"
          description="Launch a training run to compare model candidates and build an evidence trail." /></Card>
      : <div className="runs-layout">
          <Card className="run-list">
            <div className="run-list__head"><b>Training runs</b><span>{runs.data.length}</span></div>
            {runs.data.map((run) => <button key={run.id}
              className={selectedId === run.id ? "active" : ""} onClick={() => setSelectedId(run.id)}>
              <span><b>{run.run_name || run.id.slice(0, 8)}</b>
                <small>{titleCase(run.task_type)} · {formatDate(run.created_at)}</small></span>
              <Badge status={run.status} /><ChevronRight size={16} />
            </button>)}
          </Card>
          {selected && <RunDetail projectId={projectId} run={selected}
            invalidate={() => client.invalidateQueries({ queryKey: ["runs", projectId] })} />}
        </div>}
  </>;
}

function RunDetail({ projectId, run, invalidate }: {
  projectId: string; run: ModelRun; invalidate: () => void;
}) {
  const [tab, setTab] = useState<"leaderboard" | "analysis" | "logs">("leaderboard");
  const [showAdd, setShowAdd] = useState(false);
  const leaderboard = useQuery({
    queryKey: ["leaderboard", projectId, run.id],
    queryFn: () => api<Leaderboard>(`/projects/${projectId}/training/runs/${run.id}/leaderboard`),
    refetchInterval: ["queued", "precheck_running", "running"].includes(run.status) ? 3000 : false,
  });
  const resources = useQuery({
    queryKey: ["run-resources", projectId, run.id],
    queryFn: () => api<TrainingResourceUsage>(`/projects/${projectId}/training/runs/${run.id}/resources`),
    refetchInterval: ["queued", "precheck_running", "running"].includes(run.status) ? 2000 : false,
  });
  const logs = useQuery({
    queryKey: ["logs", run.id],
    queryFn: () => api<Logs>(`/projects/${projectId}/training/runs/${run.id}/logs`),
    refetchInterval: ["queued", "precheck_running", "running"].includes(run.status) ? 2000 : false,
  });
  const cancel = useMutation({
    mutationFn: () => api(`/projects/${projectId}/training/runs/${run.id}/cancel`, json("POST")),
    onSuccess: invalidate,
  });
  const restart = useMutation({
    mutationFn: () => api(`/projects/${projectId}/training/runs/${run.id}/restart`, json("POST")),
    onSuccess: invalidate,
  });
  const winner = leaderboard.data?.entries.find((entry) => entry.model === leaderboard.data?.winner);
  return <div className="run-detail">
    <Card className="run-summary">
      <div className="section-heading"><div><span className="eyebrow">Training run</span>
        <h2>{run.run_name || run.id.slice(0, 8)}</h2>
        <p>{titleCase(run.task_type)} · Created {formatDate(run.created_at)}</p></div>
        <Badge status={run.status} />
      </div>
      {run.plain_english_failure && <Notice tone="danger">{run.plain_english_failure}</Notice>}
      {run.failure_message && <details className="run-failure"><summary>Technical failure details</summary><pre>{run.failure_message}</pre></details>}
      <div className="metrics-grid metrics-grid--compact">
        <Metric label="Candidates" value={leaderboard.data?.entries.length || "—"} />
        <Metric label="Winner" value={leaderboard.data?.winner || "Pending"} />
        <Metric label="Primary metric" value={titleCase(leaderboard.data?.primary_metric || "Pending")} />
        <Metric label="Finished" value={formatDate(run.finished_at)} />
      </div>
      <div className="button-row run-actions">
        {run.status === "succeeded" && <Button variant="secondary" onClick={() => setShowAdd(true)}>
          <Plus size={16} />Add models</Button>}
        {["queued", "precheck_running", "running"].includes(run.status) &&
          <Button variant="danger" loading={cancel.isPending} onClick={() => cancel.mutate()}>
            <CircleStop size={16} />Cancel run</Button>}
        {["failed", "cancelled", "preempted"].includes(run.status) &&
          <Button variant="secondary" loading={restart.isPending} onClick={() => restart.mutate()}>
            <RefreshCw size={16} />Restart run</Button>}
      </div>
    </Card>
    <div className="tabs" role="tablist" aria-label="Run detail">
      <button role="tab" aria-selected={tab === "leaderboard"} className={tab === "leaderboard" ? "active" : ""}
        onClick={() => setTab("leaderboard")}>Leaderboard</button>
      <button role="tab" aria-selected={tab === "analysis"} className={tab === "analysis" ? "active" : ""}
        onClick={() => setTab("analysis")}>Validate & explain</button>
      <button role="tab" aria-selected={tab === "logs"} className={tab === "logs" ? "active" : ""}
        onClick={() => setTab("logs")}>Logs</button>
    </div>
    {tab === "leaderboard" && <LeaderboardPanel projectId={projectId} leaderboard={leaderboard} winner={winner}
      task={run.task_type} resources={resources} logs={logs} />}
    {tab === "analysis" && <AnalysisPanel projectId={projectId} run={run}
      successfulModels={leaderboard.data?.entries.filter((entry) => entry.status === "succeeded").map((entry) => entry.model) || []} />}
    {tab === "logs" && <LogsPanel logs={logs} />}
    {showAdd && <AddModelsModal projectId={projectId} run={run}
      completed={new Set(leaderboard.data?.entries.map((entry) => entry.model))}
      close={() => setShowAdd(false)} done={() => { setShowAdd(false); invalidate(); }} />}
  </div>;
}

function LeaderboardPanel({ projectId, leaderboard, winner, task, resources, logs }: {
  projectId: string;
  leaderboard: ReturnType<typeof useQuery<Leaderboard>>;
  winner: Leaderboard["entries"][number] | undefined;
  task: TaskType;
  resources: ReturnType<typeof useQuery<TrainingResourceUsage>>;
  logs: ReturnType<typeof useQuery<Logs>>;
}) {
  const [expanded, setExpanded] = useState<string | null>(null);
  return <Card className="section-card">
    <LiveTrainingSummary resources={resources} />
    {leaderboard.isLoading ? <Loading label="Loading model evidence…" />
      : leaderboard.error ? <ErrorState error={leaderboard.error} retry={() => leaderboard.refetch()} />
      : leaderboard.data?.entries.length ? <>
        <div className="winner-banner"><Trophy /><div><span>Top candidate</span>
          <b>{leaderboard.data.winner || "Ranking in progress"}</b>
          <small>{winner?.primary_score != null
            ? `${titleCase(leaderboard.data.primary_metric || "score")}: ${winner.primary_score.toFixed(4)}`
            : "Results are still being collected"}</small></div>
          {leaderboard.data.winner && <Link className="button button--primary"
            to={`/projects/${projectId}/operations?trainingRunId=${leaderboard.data.run_id}&model=${encodeURIComponent(leaderboard.data.winner)}`}>
            <Rocket size={15} />Deploy model</Link>}</div>
        <div className="leaderboard-accordion" role="table" aria-label="Model leaderboard">
          <div className="leaderboard-accordion__head" role="row"><span>Rank</span><span>Model</span><span>Status</span>
            <span>{titleCase(leaderboard.data.primary_metric || "Score")}</span><span>Duration</span><span /></div>
          {leaderboard.data.entries.map((entry) => {
            const open = expanded === entry.model;
            return <article className={`leaderboard-model${open ? " leaderboard-model--open" : ""}`} key={entry.model}>
              <button type="button" className="leaderboard-model__trigger" aria-expanded={open}
                onClick={() => setExpanded(open ? null : entry.model)}>
                <span className="rank">{entry.rank || "—"}</span><span><b>{entry.model}</b><small>{titleCase(entry.cost_tier)} cost</small></span>
                <Badge status={entry.status} /><span className="score">{entry.primary_score?.toFixed(4) || "—"}</span>
                <span>{entry.duration_seconds ? `${entry.duration_seconds.toFixed(1)}s` : "—"}</span><ChevronDown size={17} />
              </button>
              {entry.error && <small className="cell-error leaderboard-model__error">{entry.error}</small>}
              {open && <ModelEvidence entry={entry} task={task}
                metricDirections={leaderboard.data.metric_directions} resources={resources} logs={logs} />}
            </article>;
          })}
        </div>
      </> : <EmptyState title="Results are on their way"
        description="Candidates appear progressively as training completes." />}
  </Card>;
}

type LeaderboardEntry = Leaderboard["entries"][number];
type Curve = { label: string; points: Array<Record<string, number | null>> };
type PredictionSample = { order: number; actual: number; predicted: number; residual: number };

function ModelEvidence({ entry, task, metricDirections, resources, logs }: {
  entry: LeaderboardEntry;
  task: TaskType;
  metricDirections: Record<string, string>;
  resources: ReturnType<typeof useQuery<TrainingResourceUsage>>;
  logs: ReturnType<typeof useQuery<Logs>>;
}) {
  const [tab, setTab] = useState<"metrics" | "diagnostics" | "resources" | "parameters" | "logs">("metrics");
  return <div className="leaderboard-model__body">
    <div className="model-tabs" role="tablist" aria-label={`${entry.model} evidence`}>
      {(["metrics", "diagnostics", "resources", "parameters", "logs"] as const).map((name) =>
        <button key={name} role="tab" aria-selected={tab === name} className={tab === name ? "active" : ""}
          onClick={() => setTab(name)}>{titleCase(name)}</button>)}
    </div>
    {tab === "metrics" && <section><h3>Metrics</h3><div className="metric-pills">
      {Object.entries(entry.metrics).map(([name, value]) => <span key={name}>
        <small>{titleCase(name)} · {metricDirections[name] || "review"}</small><b>{value.toFixed(4)}</b></span>)}</div></section>}
    {tab === "diagnostics" && <ModelDiagnosticCharts diagnostics={entry.diagnostics} task={task} />}
    {tab === "resources" && <ResourcePanel resources={resources} />}
    {tab === "parameters" && <pre className="model-parameters-json">{JSON.stringify(entry.best_params, null, 2)}</pre>}
    {tab === "logs" && (logs.isLoading ? <Loading /> : logs.error
      ? <ErrorState error={logs.error} retry={() => logs.refetch()} />
      : <pre className="log-viewer" aria-label="Training logs">{logs.data?.lines.join("\n") || "No log output yet."}</pre>)}
  </div>;
}

function LiveTrainingSummary({ resources }: {
  resources: ReturnType<typeof useQuery<TrainingResourceUsage>>;
}) {
  if (resources.isLoading) return <Loading label="Loading training progress…" />;
  if (resources.error) return <Notice tone="danger">Live training details are temporarily unavailable.</Notice>;
  const value = resources.data;
  if (!value) return null;
  const total = value.total_candidates || 0;
  const completed = value.completed_candidates || 0;
  const progress = Number.isFinite(value.progress) ? value.progress : 0;
  return <div className="live-training-summary">
    <div><Activity size={18} /><span><small>Active model</small><b>{value.current_candidate || "Waiting for candidate"}</b></span></div>
    <div><Gauge size={18} /><span><small>Current phase</small><b>{titleCase(value.current_phase || value.pod_phase || value.status || "waiting")}</b></span></div>
    <div className="live-training-summary__progress"><span>
      <b>Candidate {Math.min(completed + (value.current_candidate ? 1 : 0), total)} of {total}</b>
      <small>{Math.round(progress * 100)}% complete</small></span>
      <progress max={1} value={progress} /></div>
  </div>;
}

function ResourcePanel({ resources }: { resources: ReturnType<typeof useQuery<TrainingResourceUsage>> }) {
  if (resources.isLoading) return <Loading label="Loading resource telemetry…" />;
  if (resources.error) return <ErrorState error={resources.error} retry={() => resources.refetch()} />;
  const value = resources.data;
  if (!value) return null;
  const cpuPercent = value.cpu_limit_cores ? (value.cpu_usage_cores || 0) / value.cpu_limit_cores * 100 : 0;
  const memoryPercent = value.memory_limit_mb ? (value.memory_usage_mb || 0) / value.memory_limit_mb * 100 : 0;
  return <div className="resource-panel">
    <div className="resource-cards">
      <ResourceMeter icon={<Cpu />} label="CPU" current={value.cpu_usage_cores == null ? "Unavailable" : `${value.cpu_usage_cores.toFixed(2)} cores`}
        detail={`Peak ${formatNumber(value.peak_cpu_usage_cores, " cores")} · limit ${formatNumber(value.cpu_limit_cores, " cores")}`} percent={cpuPercent} />
      <ResourceMeter icon={<MemoryStick />} label="Memory" current={value.memory_usage_mb == null ? "Unavailable" : `${value.memory_usage_mb.toLocaleString()} MB`}
        detail={`Peak ${formatNumber(value.peak_memory_usage_mb, " MB")} · limit ${formatNumber(value.memory_limit_mb, " MB")}`} percent={memoryPercent} />
      <ResourceMeter icon={<Gauge />} label="Accelerator"
        current={value.gpu_count ? `${titleCase(value.gpu_vendor || "GPU")} × ${value.gpu_count}` : "CPU"}
        detail={value.gpu_resource || "No GPU allocated"} percent={value.gpu_utilization_percent || 0} hideProgress={!value.gpu_telemetry_available} />
    </div>
    <div className="resource-meta"><span>Pod <b>{value.pod_name || "Expired"}</b></span>
      <span>Node <b>{value.node_name || "—"}</b></span><span>Restarts <b>{value.restart_count}</b></span>
      <span>Elapsed <b>{formatDuration(value.elapsed_seconds)}</b></span>
      <span>Remaining <b>{value.estimated_remaining_seconds == null ? "Estimating…" : formatDuration(value.estimated_remaining_seconds)}</b></span></div>
    {!value.telemetry_available && <Notice>Live CPU and memory require Kubernetes Metrics Server. Peak and last-known values remain visible after the pod expires.</Notice>}
    {value.gpu_count > 0 && !value.gpu_telemetry_available && <Notice>GPU allocation is confirmed, but utilization and VRAM require NVIDIA DCGM or Intel GPU telemetry.</Notice>}
  </div>;
}

function ResourceMeter({ icon, label, current, detail, percent, hideProgress = false }: {
  icon: ReactNode; label: string; current: string; detail: string; percent: number; hideProgress?: boolean;
}) {
  return <section className="resource-meter"><div>{icon}<span><small>{label}</small><b>{current}</b></span></div>
    {!hideProgress && <progress max={100} value={Math.min(100, Math.max(0, percent))} />}
    <small>{detail}</small></section>;
}

function formatNumber(value: number | null, suffix: string) { return value == null ? "—" : `${value.toLocaleString()}${suffix}`; }
function formatDuration(seconds: number) {
  const minutes = Math.floor(seconds / 60); const remainder = Math.round(seconds % 60);
  return minutes ? `${minutes}m ${remainder}s` : `${remainder}s`;
}

function ModelDiagnosticCharts({ diagnostics, task }: {
  diagnostics: Record<string, unknown>;
  task: TaskType;
}) {
  const learning = diagnostics.learning_curve as { scoring?: string; points?: Array<Record<string, number>> } | undefined;
  const crossValidation = diagnostics.cross_validation as Record<string, unknown> | undefined;
  return <div className="model-evidence-grid">
    {task === "classification" && <ClassificationCharts diagnostics={diagnostics} />}
    {(task === "regression" || task === "time_series") && <RegressionCharts diagnostics={diagnostics} timeSeries={task === "time_series"} />}
    {task === "clustering" && <ClusteringCharts diagnostics={diagnostics} />}
    {learning?.points?.length ? <EvidenceChart title={`Learning curve · ${titleCase(learning.scoring || "score")}`}
      data={[
        { type: "scatter", mode: "lines+markers", name: "Training", x: learning.points.map((point) => point.training_rows), y: learning.points.map((point) => point.training_mean), line: { color: "#3159e8" } },
        { type: "scatter", mode: "lines+markers", name: "Validation", x: learning.points.map((point) => point.training_rows), y: learning.points.map((point) => point.validation_mean), line: { color: "#e08835" } },
      ]} xTitle="Training rows" yTitle={titleCase(learning.scoring || "score")} /> : null}
    {crossValidation && typeof crossValidation.mean === "number" ? <EvidenceChart title="Cross-validation stability"
      data={[{ type: "bar", x: ["Mean", "Standard deviation"], y: [crossValidation.mean, Number(crossValidation.standard_deviation || 0)], marker: { color: ["#3159e8", "#9aa7d8"] } }]} /> : null}
  </div>;
}

function ClassificationCharts({ diagnostics }: { diagnostics: Record<string, unknown> }) {
  const labels = Array.isArray(diagnostics.labels) ? diagnostics.labels.map(String) : [];
  const matrix = Array.isArray(diagnostics.confusion_matrix) ? diagnostics.confusion_matrix as number[][] : [];
  const rocCurves = Array.isArray(diagnostics.roc_curves) ? diagnostics.roc_curves as Curve[] : [];
  const precisionRecall = Array.isArray(diagnostics.precision_recall_curves) ? diagnostics.precision_recall_curves as Curve[] : [];
  const report = diagnostics.classification_report as Record<string, Record<string, number>> | undefined;
  const reportLabels = report ? Object.keys(report).filter((label) => typeof report[label] === "object" && "precision" in report[label]) : [];
  return <>
    {matrix.length ? <EvidenceChart title="Confusion matrix" data={[{ type: "heatmap", z: matrix, x: labels, y: labels, colorscale: "Blues", text: matrix.map((row) => row.map(String)), texttemplate: "%{text}", hovertemplate: "Actual %{y}<br>Predicted %{x}<br>Count %{z}<extra></extra>" }]} xTitle="Predicted" yTitle="Actual" /> : null}
    {rocCurves.length ? <EvidenceChart title="ROC curve" data={[
      ...rocCurves.map((curve) => ({ type: "scatter", mode: "lines", name: curve.label, x: curve.points.map((point) => point.false_positive_rate), y: curve.points.map((point) => point.true_positive_rate) })),
      { type: "scatter", mode: "lines", name: "Random", x: [0, 1], y: [0, 1], line: { dash: "dash", color: "#9aa1b2" } },
    ]} xTitle="False positive rate" yTitle="True positive rate" /> : null}
    {precisionRecall.length ? <EvidenceChart title="Precision–recall curve" data={precisionRecall.map((curve) => ({ type: "scatter", mode: "lines", name: curve.label, x: curve.points.map((point) => point.recall), y: curve.points.map((point) => point.precision) }))} xTitle="Recall" yTitle="Precision" /> : null}
    {reportLabels.length ? <EvidenceChart title="Per-class quality" data={["precision", "recall", "f1-score"].map((metric) => ({ type: "bar", name: titleCase(metric), x: reportLabels, y: reportLabels.map((label) => Number(report?.[label]?.[metric] || 0)) }))} yTitle="Score" /> : null}
  </>;
}

function RegressionCharts({ diagnostics, timeSeries }: { diagnostics: Record<string, unknown>; timeSeries: boolean }) {
  const samples = Array.isArray(diagnostics.prediction_samples) ? diagnostics.prediction_samples as PredictionSample[] : [];
  if (!samples.length) return null;
  const minimum = Math.min(...samples.flatMap((item) => [item.actual, item.predicted]));
  const maximum = Math.max(...samples.flatMap((item) => [item.actual, item.predicted]));
  return <>
    <EvidenceChart title="Actual vs predicted" data={[
      { type: "scatter", mode: "markers", name: "Predictions", x: samples.map((item) => item.actual), y: samples.map((item) => item.predicted), marker: { color: "#3159e8", opacity: .65 } },
      { type: "scatter", mode: "lines", name: "Ideal", x: [minimum, maximum], y: [minimum, maximum], line: { dash: "dash", color: "#e08835" } },
    ]} xTitle="Actual" yTitle="Predicted" />
    <EvidenceChart title="Residual distribution" data={[{ type: "histogram", x: samples.map((item) => item.residual), marker: { color: "#3159e8" } }]} xTitle="Residual" yTitle="Rows" />
    {timeSeries ? <EvidenceChart title="Chronological holdout" data={[
      { type: "scatter", mode: "lines", name: "Actual", x: samples.map((item) => item.order), y: samples.map((item) => item.actual) },
      { type: "scatter", mode: "lines", name: "Predicted", x: samples.map((item) => item.order), y: samples.map((item) => item.predicted) },
    ]} xTitle="Holdout order" yTitle="Target" /> : null}
  </>;
}

function ClusteringCharts({ diagnostics }: { diagnostics: Record<string, unknown> }) {
  const sizes = diagnostics.cluster_sizes as Record<string, number> | undefined;
  const crossValidation = diagnostics.cross_validation as { fold_metrics?: Array<Record<string, number>> } | undefined;
  const foldMetrics = crossValidation?.fold_metrics || [];
  const metricNames = [...new Set(foldMetrics.flatMap((fold) => Object.keys(fold)))];
  return <>
    {sizes && Object.keys(sizes).length ? <EvidenceChart title="Cluster sizes" data={[{ type: "bar", x: Object.keys(sizes), y: Object.values(sizes), marker: { color: "#3159e8" } }]} xTitle="Cluster" yTitle="Rows" /> : null}
    {foldMetrics.length ? <EvidenceChart title="Cross-validation by fold" data={metricNames.map((metric) => ({ type: "scatter", mode: "lines+markers", name: titleCase(metric), x: foldMetrics.map((_, index) => index + 1), y: foldMetrics.map((fold) => fold[metric]) }))} xTitle="Fold" yTitle="Metric" /> : null}
  </>;
}

function EvidenceChart({ title, data, xTitle, yTitle }: {
  title: string;
  data: Array<Record<string, unknown>>;
  xTitle?: string;
  yTitle?: string;
}) {
  return <section className="model-evidence-chart"><h3>{title}</h3><Suspense fallback={<Loading label="Loading visualization…" />}>
    <PlotlyChart data={data} layout={{ autosize: true, height: 310, margin: { l: 55, r: 15, t: 15, b: 55 }, paper_bgcolor: "rgba(0,0,0,0)", plot_bgcolor: "#f8f9fc", barmode: "group", xaxis: { title: { text: xTitle }, automargin: true }, yaxis: { title: { text: yTitle }, automargin: true }, legend: { orientation: "h", y: 1.12 }, font: { family: "Inter, system-ui, sans-serif", size: 10, color: "#4e5870" } }} config={{ displayModeBar: false, responsive: true }} useResizeHandler style={{ width: "100%" }} />
  </Suspense></section>;
}

function AnalysisPanel({ projectId, run, successfulModels }: {
  projectId: string; run: ModelRun; successfulModels: string[];
}) {
  const client = useQueryClient();
  const [model, setModel] = useState("");
  const [evaluationColumn, setEvaluationColumn] = useState("");
  const [validationFile, setValidationFile] = useState<File | null>(null);
  const [validationUploadProgress, setValidationUploadProgress] = useState(0);
  const [uploadedValidation, setUploadedValidation] = useState<DatasetUploadResult | null>(null);
  const [maxRows, setMaxRows] = useState(200);
  const [selectedAnalysis, setSelectedAnalysis] = useState("");
  const [analysisTab, setAnalysisTab] = useState<"validation" | "explainability">("validation");
  const analyses = useQuery({
    queryKey: ["analyses", run.id], enabled: successfulModels.length > 0,
    queryFn: () => api<Analysis[]>(`/projects/${projectId}/training/runs/${run.id}/analyses`),
    refetchInterval: (query) => query.state.data?.some((item) =>
      ["queued", "precheck_running", "running"].includes(item.status)) ? 5000 : false,
  });
  const versions = useQuery({
    queryKey: ["all-versions", projectId],
    queryFn: async () => {
      const datasets = await api<Dataset[]>(`/projects/${projectId}/datasets`);
      const groups = await Promise.all(datasets.map(async (dataset) => ({
        dataset,
        versions: await api<DatasetVersion[]>(`/projects/${projectId}/datasets/${dataset.id}/versions`),
      })));
      return groups.flatMap(({ dataset, versions: items }): VersionOption[] => items.map((version) => ({
        id: version.id, label: `${dataset.name} · v${version.version_number}`,
        columns: (version.schema_json || version.dataset_schema)?.columns?.map((column) => column.name) || [],
      })));
    },
  });
  useEffect(() => {
    if (successfulModels.length && !successfulModels.includes(model)) setModel(successfulModels[0]);
  }, [successfulModels, model]);
  const trainingColumns = versions.data?.find((item) => item.id === run.dataset_version_id)?.columns || [];
  const uploadedValidationColumns = (uploadedValidation?.version.schema_json
    || uploadedValidation?.version.dataset_schema)?.columns?.map((column) => column.name) || [];
  const missingValidationColumns = trainingColumns.filter(
    (column) => !uploadedValidationColumns.includes(column),
  );
  const validationSchemaReady = trainingColumns.length > 0;
  const visibleAnalyses = useMemo(() => (analyses.data || []).filter((item) =>
    item.run_kind === analysisTab && item.params.model_name === model),
  [analyses.data, analysisTab, model]);
  const completedExplanation = useMemo(() => (analyses.data || []).find((item) =>
    item.run_kind === "explainability" && item.status === "succeeded"
    && item.params.model_name === model), [analyses.data, model]);
  useEffect(() => {
    const preferred = analysisTab === "explainability" && completedExplanation
      ? completedExplanation : visibleAnalyses[0];
    if (completedExplanation && analysisTab === "explainability"
      && selectedAnalysis !== completedExplanation.id) {
      setSelectedAnalysis(completedExplanation.id);
    } else if (preferred && !visibleAnalyses.some((item) => item.id === selectedAnalysis)) {
      setSelectedAnalysis(preferred.id);
    } else if (!preferred && selectedAnalysis) {
      setSelectedAnalysis("");
    }
  }, [analysisTab, completedExplanation, selectedAnalysis, visibleAnalyses]);
  const explain = useMutation({
    mutationFn: () => api<{ run: Analysis }>(`/projects/${projectId}/training/runs/${run.id}/explanations`,
      json("POST", { model_name: model, max_rows: maxRows, expected_minutes: 10 })),
    onSuccess: ({ run: launched }) => {
      setSelectedAnalysis(launched.id);
      client.removeQueries({ queryKey: ["analysis-result", run.id, launched.id] });
      analyses.refetch();
    },
  });
  const uploadValidation = useMutation({
    mutationFn: async (file: File) => {
      const body = new FormData();
      body.set("dataset_name", `External validation · ${file.name}`.slice(0, 220));
      body.set("description", `Uploaded to validate ${run.run_name || run.id}`);
      body.set("tags", JSON.stringify({ purpose: "external_validation", source_run_id: run.id }));
      body.set("file", file, file.name);
      return uploadFormData<DatasetUploadResult>(
        `/projects/${projectId}/datasets/upload`, body, setValidationUploadProgress,
      );
    },
    onSuccess: (uploaded) => {
      setUploadedValidation(uploaded);
      setEvaluationColumn("");
      client.invalidateQueries({ queryKey: ["datasets", projectId] });
    },
  });
  const validate = useMutation({
    mutationFn: () => api<{ run: Analysis }>(`/projects/${projectId}/training/runs/${run.id}/validations`,
      json("POST", {
        model_name: model, dataset_version_id: uploadedValidation!.version.id,
        evaluation_column: run.task_type === "clustering" ? evaluationColumn || null : null,
        expected_minutes: 5,
      })),
    onSuccess: ({ run: launched }) => {
      setSelectedAnalysis(launched.id);
      client.removeQueries({ queryKey: ["analysis-result", run.id, launched.id] });
      analyses.refetch();
    },
  });
  const result = useQuery({
    queryKey: ["analysis-result", run.id, selectedAnalysis], enabled: Boolean(selectedAnalysis),
    queryFn: () => api<AnalysisResult>(
      `/projects/${projectId}/training/runs/${run.id}/analyses/${selectedAnalysis}`),
    refetchInterval: (query) => ["queued", "precheck_running", "running"].includes(
      query.state.data?.status || "",
    ) ? 2000 : false,
  });
  const selectedRun = visibleAnalyses.find((item) => item.id === selectedAnalysis);
  const explanationSucceeded = analysisTab === "explainability"
    && selectedRun?.run_kind === "explainability"
    && result.data?.status === "succeeded";
  return <Card className="analysis-card">
    <div><BrainCircuit /><span><h2>Challenge a candidate</h2>
      <p>Test on an external dataset or calculate feature contributions before promotion.</p></span></div>
    {!successfulModels.length ? <Notice>Successful model candidates are required before analysis.</Notice> : <>
      <label>Candidate model<select value={model} onChange={(event) => setModel(event.target.value)}>
        {successfulModels.map((name) => <option key={name}>{name}</option>)}</select></label>
      <div className="analysis-mode-tabs" role="tablist" aria-label="Model analysis type">
        <button role="tab" aria-selected={analysisTab === "validation"}
          className={analysisTab === "validation" ? "active" : ""}
          onClick={() => setAnalysisTab("validation")}>External validation</button>
        <button role="tab" aria-selected={analysisTab === "explainability"}
          className={analysisTab === "explainability" ? "active" : ""}
          onClick={() => setAnalysisTab("explainability")}>SHAP explainability</button>
      </div>
      {analysisTab === "validation" && <div className="analysis-actions analysis-actions--single">
        <section><FileCheck2 /><h3>Validate on external data</h3>
          <p>Upload an external dataset. Its columns are checked against the training data before validation can start.</p>
          <label className="dropzone analysis-upload"><input type="file"
            accept=".csv,.parquet,.xlsx,.xls,.json,.jsonl" onChange={(event) => {
              setValidationFile(event.target.files?.[0] || null);
              setUploadedValidation(null);
              setValidationUploadProgress(0);
              uploadValidation.reset();
            }} />
            <FileSpreadsheet /><b>{validationFile?.name || "Choose an external dataset"}</b>
            <span>CSV, Parquet, Excel, JSON, or JSONL</span></label>
          {validationFile && !uploadedValidation && <Button variant="secondary"
            loading={uploadValidation.isPending} onClick={() => uploadValidation.mutate(validationFile)}>
            <Upload size={15} />Upload and inspect</Button>}
          {uploadValidation.isPending && <div className="progress-panel"><div><b>Uploading external dataset</b>
            <span>{validationUploadProgress}%</span></div><progress value={validationUploadProgress} max={100} /></div>}
          {uploadValidation.error && <Notice tone="danger">{uploadValidation.error.message}</Notice>}
          {uploadedValidation && !validationSchemaReady && <Notice>
            Loading the training schema before validation can start.
          </Notice>}
          {uploadedValidation && validationSchemaReady && missingValidationColumns.length > 0 && <Notice tone="danger">
            This file cannot be validated. Missing training columns: {missingValidationColumns.join(", ")}.
          </Notice>}
          {uploadedValidation && validationSchemaReady && missingValidationColumns.length === 0 && <Notice tone="success">
            Schema matched: all {trainingColumns.length} training columns are present.
          </Notice>}
          {run.task_type === "clustering" && <label>Reference label <span className="optional">Optional</span>
            <select value={evaluationColumn} onChange={(event) => setEvaluationColumn(event.target.value)}>
              <option value="">No reference label</option>
              {uploadedValidationColumns.map((column) => <option key={column}>{column}</option>)}
            </select></label>}
          <Button disabled={!uploadedValidation || !validationSchemaReady || missingValidationColumns.length > 0}
            loading={validate.isPending} onClick={() => validate.mutate()}>
            <Play size={15} />Run validation</Button>
          {validate.error && <Notice tone="danger">{validate.error.message}</Notice>}
        </section>
      </div>}
      {analysisTab === "explainability" && !explanationSucceeded &&
        <div className="analysis-actions analysis-actions--single"><section><BrainCircuit /><h3>Explain with SHAP</h3>
          <p>Quantify which features contributed most to this model's decisions.</p>
          <label>Sample rows<input type="number" min={20} max={1000} step={20}
            value={maxRows} onChange={(event) => setMaxRows(Number(event.target.value))} /></label>
          <Button loading={explain.isPending} onClick={() => explain.mutate()}>
            <Play size={15} />Calculate SHAP</Button>
          {explain.error && <Notice tone="danger">{explain.error.message}</Notice>}
        </section></div>}
    </>}
    {!explanationSucceeded && <h3>{analysisTab === "validation" ? "Validation history" : "Explainability history"}</h3>}
    {visibleAnalyses.length ? <>
      {!explanationSucceeded && <div className="analysis-history">{visibleAnalyses.map((item) =>
        <button className={selectedAnalysis === item.id ? "active" : ""} key={item.id}
          onClick={() => setSelectedAnalysis(item.id)}>
          <span><b>{item.run_name || item.id.slice(0, 8)}</b><small>{titleCase(item.run_kind)}</small></span>
          <Badge status={item.status} />
        </button>)}</div>}
      {selectedRun && (result.isLoading ? <Loading label="Loading analysis result…" />
        : result.data && <AnalysisResultPanel result={result.data} />)}
    </> : <p className="muted">No {analysisTab === "validation" ? "validation" : "explainability"} jobs yet.</p>}
  </Card>;
}

function AnalysisResultPanel({ result }: { result: AnalysisResult }) {
  const importance = [...result.feature_importance].map((item) => ({
    ...item, weight: Math.abs(Number(item.mean_absolute_shap ?? item.contribution_percent ?? 0)),
  })).sort((a, b) => b.weight - a.weight);
  const total = importance.reduce((sum, item) => sum + item.weight, 0);
  return <div className="analysis-result">
    <div className="section-heading"><div><h3>{result.model_name}</h3>
      <p>Persisted analysis evidence</p></div><Badge status={result.status} /></div>
    {["queued", "precheck_running", "running"].includes(result.status) &&
      <Notice>Explainability is still running. Feature contributions will appear here automatically.</Notice>}
    {Object.keys(result.metrics).length > 0 && <div className="metric-pills">
      {Object.entries(result.metrics).map(([name, value]) =>
        <span key={name}><small>{titleCase(name)}</small><b>{value.toFixed(4)}</b></span>)}</div>}
    {importance.length > 0 && <div className="importance"><h3>Feature contribution</h3>
      {importance.slice(0, 20).map((item) => {
        const percent = total ? item.weight / total * 100 : 0;
        return <div key={item.feature}><span>{item.feature}</span><i><b style={{ width: `${percent}%` }} /></i>
          <strong>{percent.toFixed(1)}%</strong></div>;
      })}</div>}
    {result.artifacts.length > 0 && <div className="artifact-list">{result.artifacts.map((artifact) =>
      <div key={artifact.id}><FileText size={15} /><span><b>{artifact.name}</b>
        <small>{titleCase(artifact.kind)} · {formatBytes(artifact.byte_size)}</small></span></div>)}</div>}
  </div>;
}

function LogsPanel({ logs }: { logs: ReturnType<typeof useQuery<Logs>> }) {
  return <Card className="logs-card"><div className="section-heading"><div><h2>Training logs</h2>
    <p>Polling safely while the run is active.</p></div><TerminalSquare /></div>
    {logs.isLoading ? <Loading /> : logs.error ? <ErrorState error={logs.error} retry={() => logs.refetch()} />
      : <pre className="log-viewer" aria-label="Training logs">{logs.data?.lines.join("\n") || "No log output yet."}</pre>}
  </Card>;
}

function AddModelsModal({ projectId, run, completed, close, done }: {
  projectId: string; run: ModelRun; completed: Set<string>; close: () => void; done: () => void;
}) {
  const [selected, setSelected] = useState<string[]>([]);
  const [iterations, setIterations] = useState(Number(run.params.optimization_iterations || 5));
  const [folds, setFolds] = useState(Number(run.params.cv_folds || 3));
  const [minutes, setMinutes] = useState(Number(run.params.expected_minutes || 10));
  const estimators = useQuery({
    queryKey: ["estimators", projectId, run.task_type],
    queryFn: () => api<Estimator[]>(
      `/projects/${projectId}/training/estimators?task_type=${run.task_type as TaskType}`),
  });
  const available = estimators.data?.filter((item) => !completed.has(item.name)) || [];
  const add = useMutation({
    mutationFn: () => api(`/projects/${projectId}/training/runs/${run.id}/models`, json("POST", {
      candidate_models: selected, optimization_iterations: iterations,
      cv_folds: folds, expected_minutes: minutes, prefer_gpu: false,
    })),
    onSuccess: done,
  });
  return <Modal title="Train additional models"
    description="Completed candidates are preserved; only the selected additions will run." onClose={close}>
    {estimators.isLoading ? <Loading /> : available.length ? <div className="stack">
      <div className="selection-actions"><Button variant="secondary" type="button"
        onClick={() => setSelected(available.slice(0, 20).map((model) => model.name))}>Select all models</Button>
        {selected.length > 0 && <Button variant="ghost" type="button" onClick={() => setSelected([])}>Clear selection</Button>}</div>
      <div className="model-grid">{available.map((item) => <label
        className={selected.includes(item.name) ? "model-option active" : "model-option"} key={item.name}>
        <input type="checkbox" checked={selected.includes(item.name)}
          onChange={() => setSelected((current) => current.includes(item.name)
            ? current.filter((name) => name !== item.name) : [...current, item.name])} />
        <span><b>{item.name}</b><small>{titleCase(item.cost_tier)} cost</small></span><i>✓</i>
      </label>)}</div>
      <div className="form-grid form-grid--3">
        <label>Search iterations<input type="number" min={1} max={25} value={iterations}
          onChange={(event) => setIterations(Number(event.target.value))} /></label>
        <label>CV folds<select value={folds} onChange={(event) => setFolds(Number(event.target.value))}>
          {[2, 3, 4, 5].map((value) => <option key={value}>{value}</option>)}</select></label>
        <label>Minutes<input type="number" min={1} max={120} value={minutes}
          onChange={(event) => setMinutes(Number(event.target.value))} /></label>
      </div>
      {add.error && <Notice tone="danger">{add.error.message}</Notice>}
      <div className="modal__actions"><Button variant="ghost" onClick={close}>Cancel</Button>
        <Button disabled={!selected.length} loading={add.isPending} onClick={() => add.mutate()}>
          Train selected models</Button></div>
    </div> : <EmptyState title="Every compatible model is already included"
      description="This run's leaderboard already contains the full available catalog." />}
  </Modal>;
}
