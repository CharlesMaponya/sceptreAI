import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  BarChart3, BrainCircuit, ChevronRight, CircleStop, FileCheck2, FileText,
  Play, Plus, RefreshCw, TerminalSquare, Trophy,
} from "lucide-react";
import { useEffect, useState } from "react";
import { useParams } from "react-router-dom";
import { api, json } from "../api";
import {
  Badge, Button, Card, EmptyState, ErrorState, Loading, Metric, Modal, Notice, PageHeader,
} from "../components/ui";
import { formatBytes, formatDate, titleCase } from "../lib";
import type {
  Dataset, DatasetVersion, Estimator, Leaderboard, ModelRun, TaskType,
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
    {tab === "leaderboard" && <LeaderboardPanel leaderboard={leaderboard} winner={winner} />}
    {tab === "analysis" && <AnalysisPanel projectId={projectId} run={run}
      successfulModels={leaderboard.data?.entries.filter((entry) => entry.status === "succeeded").map((entry) => entry.model) || []} />}
    {tab === "logs" && <LogsPanel projectId={projectId} run={run} />}
    {showAdd && <AddModelsModal projectId={projectId} run={run}
      completed={new Set(leaderboard.data?.entries.map((entry) => entry.model))}
      close={() => setShowAdd(false)} done={() => { setShowAdd(false); invalidate(); }} />}
  </div>;
}

function LeaderboardPanel({ leaderboard, winner }: {
  leaderboard: ReturnType<typeof useQuery<Leaderboard>>;
  winner: Leaderboard["entries"][number] | undefined;
}) {
  return <Card className="section-card">
    {leaderboard.isLoading ? <Loading label="Loading model evidence…" />
      : leaderboard.error ? <ErrorState error={leaderboard.error} retry={() => leaderboard.refetch()} />
      : leaderboard.data?.entries.length ? <>
        <div className="winner-banner"><Trophy /><div><span>Top candidate</span>
          <b>{leaderboard.data.winner || "Ranking in progress"}</b>
          <small>{winner?.primary_score != null
            ? `${titleCase(leaderboard.data.primary_metric || "score")}: ${winner.primary_score.toFixed(4)}`
            : "Results are still being collected"}</small></div></div>
        <div className="table-wrap"><table><thead><tr><th>Rank</th><th>Model</th><th>Status</th>
          <th>{titleCase(leaderboard.data.primary_metric || "Score")}</th><th>Duration</th></tr></thead>
          <tbody>{leaderboard.data.entries.map((entry) => <tr key={entry.model}>
            <td className="rank">{entry.rank || "—"}</td>
            <td><b>{entry.model}</b><small className="cell-sub">{titleCase(entry.cost_tier)} cost</small>
              {entry.error && <small className="cell-error">{entry.error}</small>}</td>
            <td><Badge status={entry.status} /></td>
            <td className="score">{entry.primary_score?.toFixed(4) || "—"}</td>
            <td>{entry.duration_seconds ? `${entry.duration_seconds.toFixed(1)}s` : "—"}</td>
          </tr>)}</tbody></table></div>
        <details className="diagnostics"><summary>Candidate metrics and parameters</summary>
          {leaderboard.data.entries.filter((entry) => entry.status === "succeeded").map((entry) =>
            <div key={entry.model}><h3>{entry.model}</h3>
              <div className="metric-pills">{Object.entries(entry.metrics).map(([name, value]) =>
                <span key={name}><small>{titleCase(name)}</small><b>{value.toFixed(4)}</b></span>)}</div>
              <pre>{JSON.stringify(entry.best_params, null, 2)}</pre></div>)}
        </details>
      </> : <EmptyState title="Results are on their way"
        description="Candidates appear progressively as training completes." />}
  </Card>;
}

function AnalysisPanel({ projectId, run, successfulModels }: {
  projectId: string; run: ModelRun; successfulModels: string[];
}) {
  const [model, setModel] = useState("");
  const [versionId, setVersionId] = useState("");
  const [evaluationColumn, setEvaluationColumn] = useState("");
  const [maxRows, setMaxRows] = useState(200);
  const [selectedAnalysis, setSelectedAnalysis] = useState("");
  const analyses = useQuery({
    queryKey: ["analyses", run.id], enabled: run.status === "succeeded",
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
  useEffect(() => {
    if (versions.data?.length && !versions.data.some((item) => item.id === versionId)) setVersionId(versions.data[0].id);
  }, [versions.data, versionId]);
  useEffect(() => {
    if (analyses.data?.length && !analyses.data.some((item) => item.id === selectedAnalysis)) {
      setSelectedAnalysis(analyses.data[0].id);
    }
  }, [analyses.data, selectedAnalysis]);
  const explain = useMutation({
    mutationFn: () => api(`/projects/${projectId}/training/runs/${run.id}/explanations`,
      json("POST", { model_name: model, max_rows: maxRows, expected_minutes: 10 })),
    onSuccess: () => analyses.refetch(),
  });
  const validate = useMutation({
    mutationFn: () => api(`/projects/${projectId}/training/runs/${run.id}/validations`,
      json("POST", {
        model_name: model, dataset_version_id: versionId,
        evaluation_column: run.task_type === "clustering" ? evaluationColumn || null : null,
        expected_minutes: 5,
      })),
    onSuccess: () => analyses.refetch(),
  });
  const result = useQuery({
    queryKey: ["analysis-result", run.id, selectedAnalysis], enabled: Boolean(selectedAnalysis),
    queryFn: () => api<AnalysisResult>(
      `/projects/${projectId}/training/runs/${run.id}/analyses/${selectedAnalysis}`),
  });
  const currentVersion = versions.data?.find((item) => item.id === versionId);
  return <Card className="analysis-card">
    <div><BrainCircuit /><span><h2>Challenge a candidate</h2>
      <p>Test on an external dataset or calculate feature contributions before promotion.</p></span></div>
    {!successfulModels.length ? <Notice>Successful model candidates are required before analysis.</Notice> : <>
      <label>Candidate model<select value={model} onChange={(event) => setModel(event.target.value)}>
        {successfulModels.map((name) => <option key={name}>{name}</option>)}</select></label>
      <div className="analysis-actions">
        <section><FileCheck2 /><h3>External validation</h3>
          <p>Measure this candidate against a separate immutable dataset version.</p>
          <label>Dataset version<select value={versionId} onChange={(event) => setVersionId(event.target.value)}>
            {versions.data?.map((item) => <option value={item.id} key={item.id}>{item.label}</option>)}</select></label>
          {run.task_type === "clustering" && <label>Reference label <span className="optional">Optional</span>
            <select value={evaluationColumn} onChange={(event) => setEvaluationColumn(event.target.value)}>
              <option value="">No reference label</option>
              {currentVersion?.columns.map((column) => <option key={column}>{column}</option>)}
            </select></label>}
          <Button disabled={!versionId} loading={validate.isPending} onClick={() => validate.mutate()}>
            <Play size={15} />Run validation</Button>
          {validate.error && <Notice tone="danger">{validate.error.message}</Notice>}
        </section>
        <section><BrainCircuit /><h3>SHAP explainability</h3>
          <p>Quantify which features contributed most to this model's decisions.</p>
          <label>Sample rows<input type="number" min={20} max={1000} step={20}
            value={maxRows} onChange={(event) => setMaxRows(Number(event.target.value))} /></label>
          <Button loading={explain.isPending} onClick={() => explain.mutate()}>
            <Play size={15} />Calculate SHAP</Button>
          {explain.error && <Notice tone="danger">{explain.error.message}</Notice>}
        </section>
      </div>
    </>}
    <h3>Analysis history</h3>
    {analyses.data?.length ? <>
      <div className="analysis-history">{analyses.data.map((item) =>
        <button className={selectedAnalysis === item.id ? "active" : ""} key={item.id}
          onClick={() => setSelectedAnalysis(item.id)}>
          <span><b>{item.run_name || item.id.slice(0, 8)}</b><small>{titleCase(item.run_kind)}</small></span>
          <Badge status={item.status} />
        </button>)}</div>
      {result.isLoading ? <Loading label="Loading analysis result…" />
        : result.data && <AnalysisResultPanel result={result.data} />}
    </> : <p className="muted">No validation or explainability jobs yet.</p>}
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

function LogsPanel({ projectId, run }: { projectId: string; run: ModelRun }) {
  const logs = useQuery({
    queryKey: ["logs", run.id],
    queryFn: () => api<Logs>(`/projects/${projectId}/training/runs/${run.id}/logs`),
    refetchInterval: ["queued", "precheck_running", "running"].includes(run.status) ? 2000 : false,
  });
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
