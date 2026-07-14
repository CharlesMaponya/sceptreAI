import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  Activity, AlertTriangle, Box, Braces, CloudCog, Cpu, Database, ExternalLink, Gauge,
  FileSpreadsheet, RefreshCw, Rocket, ShieldCheck, Square, Trash2, Upload,
} from "lucide-react";
import { useEffect, useMemo, useState } from "react";
import { useParams, useSearchParams } from "react-router-dom";
import { api, json, uploadFormData } from "../api";
import {
  Badge, Button, Card, EmptyState, ErrorState, Loading, Metric, Modal, Notice, PageHeader,
} from "../components/ui";
import { formatBytes, formatDate, titleCase } from "../lib";
import type { Dataset, DatasetVersion, Leaderboard, ModelRun, PlatformHealth } from "../types";

interface Registry {
  id: string; model_run_id: string; stage: string; model_name: string; version: number;
  champion_metric_name: string | null; champion_metric_value: number | null;
  is_fallback: boolean; created_at: string;
  training_dataset_version_id: string;
  training_feature_columns: string[];
}
interface DeployStatus {
  run: ModelRun; runtime_state: string; endpoint: string | null; docs_url?: string | null;
  openapi_url?: string | null; status: string;
  service_name?: string | null; namespace?: string | null;
  internal_endpoint?: string | null; internal_docs_url?: string | null;
  internal_openapi_url?: string | null;
  platform_endpoint?: string | null; platform_online_endpoint?: string | null;
  platform_offline_endpoint?: string | null; platform_metadata_url?: string | null;
  platform_docs_url?: string | null; platform_openapi_url?: string | null;
  platform_live_url?: string | null; platform_ready_url?: string | null;
}
interface DriftRun extends ModelRun {
  tags: { diagnostics?: { drift_share_percent?: number; drifted_feature_count?: number } };
}
interface CleanupResult {
  dry_run: boolean; artifact_count: number; artifact_bytes: number; artifact_ids: string[];
  deleted_object_uris: string[]; deleted_kubernetes_jobs: string[]; errors: string[];
}
interface DatasetUploadResult { dataset: Dataset; version: DatasetVersion }

export function OperationsPage() {
  const { projectId = "" } = useParams();
  const [searchParams, setSearchParams] = useSearchParams();
  const client = useQueryClient();
  const requestedRunId = searchParams.get("trainingRunId") || "";
  const requestedModel = searchParams.get("model") || "";
  const [registerOpen, setRegisterOpen] = useState(Boolean(requestedRunId || requestedModel));
  const closeRegister = () => {
    setRegisterOpen(false);
    if (requestedRunId || requestedModel) setSearchParams({}, { replace: true });
  };
  const health = useQuery({
    queryKey: ["health", projectId],
    queryFn: () => api<PlatformHealth>(`/projects/${projectId}/operations/health`),
    refetchInterval: 15_000,
  });
  const registry = useQuery({
    queryKey: ["registry", projectId],
    queryFn: () => api<Registry[]>(`/projects/${projectId}/operations/registry`),
  });
  const deployments = useQuery({
    queryKey: ["deployments", projectId],
    queryFn: () => api<DeployStatus[]>(`/projects/${projectId}/operations/deployments`),
    refetchInterval: (query) => query.state.data?.some((item) =>
      ["queued", "precheck_running", "running", "succeeded"].includes(item.status)) ? 15_000 : false,
  });
  const driftRuns = useQuery({
    queryKey: ["drift-runs", projectId],
    queryFn: () => api<DriftRun[]>(`/projects/${projectId}/operations/drift-runs`),
    refetchInterval: (query) => query.state.data?.some((item) =>
      ["queued", "precheck_running", "running"].includes(item.status)) ? 5000 : false,
  });
  const refreshOperations = () => {
    client.invalidateQueries({ queryKey: ["registry", projectId] });
    client.invalidateQueries({ queryKey: ["deployments", projectId] });
    client.invalidateQueries({ queryKey: ["drift-runs", projectId] });
    client.invalidateQueries({ queryKey: ["health", projectId] });
  };
  if (health.isLoading || registry.isLoading) return <Loading />;
  if (health.error) return <ErrorState error={health.error} retry={() => health.refetch()} />;
  return <>
    <PageHeader eyebrow="Model operations" title="Deploy & monitor"
      description="Promote approved models, check drift, track runtime health, and keep a safe fallback."
      action={<Button onClick={() => setRegisterOpen(true)}><Box size={16} />Register model</Button>} />
    <div className="metrics-grid">
      <Metric label="Platform" value={health.data?.capacity.connected ? "Connected" : "Degraded"}
        hint={`${health.data?.capacity.ready_nodes || 0} ready nodes`} />
      <Metric label="Available CPU" value={health.data?.capacity.available_cpu_cores.toFixed(1) || "—"}
        hint="cluster cores" />
      <Metric label="Available memory"
        value={`${((health.data?.capacity.available_memory_mb || 0) / 1024).toFixed(1)} GiB`} />
      <Metric label="Active deployments" value={health.data?.active_deployments || 0} />
    </div>
    {health.data?.capacity.warnings.length
      ? <Notice>{health.data.capacity.warnings.join(" ")}</Notice> : null}
    <Card className="section-card">
      <div className="section-heading"><div><h2>Model registry</h2>
        <p>The governed source of deployable model versions.</p></div><ShieldCheck className="section-icon" /></div>
      {registry.data?.length ? <div className="registry-grid">{registry.data.map((entry) =>
        <RegistryCard key={entry.id} projectId={projectId} entry={entry}
          refresh={refreshOperations} />)}</div>
        : <EmptyState icon={<Box />} title="No registered models"
          description="Register a successful training candidate before promoting or deploying it."
          action={<Button onClick={() => setRegisterOpen(true)}>Register model</Button>} />}
    </Card>
    <DriftPanel runs={driftRuns.data || []} />
    <Card className="section-card">
      <div className="section-heading"><div><h2>Deployments</h2>
        <p>Live and historical inference services.</p></div><CloudCog className="section-icon" /></div>
      {deployments.isLoading ? <Loading label="Loading deployments…" />
        : deployments.error ? <ErrorState error={deployments.error} retry={() => deployments.refetch()} />
          : deployments.data?.length ? <div className="table-wrap"><table><thead><tr>
        <th>Deployment</th><th>Runtime</th><th>Status</th><th>Endpoint</th><th /></tr></thead>
        <tbody>{deployments.data.map((deployment) =>
          <DeploymentRow key={deployment.run.id} projectId={projectId}
            deployment={deployment} refresh={() => deployments.refetch()} />)}</tbody></table></div>
        : <div className="inline-empty"><Rocket /><span><b>No model deployments</b>
          <small>Promote a registry entry and deploy it when the evidence is ready.</small></span></div>}
    </Card>
    <CleanupPanel projectId={projectId} />
    {registerOpen && <RegisterModal projectId={projectId} initialRunId={requestedRunId}
      initialModel={requestedModel} close={closeRegister}
      done={() => { closeRegister(); registry.refetch(); }} />}
  </>;
}

function RegistryCard({ projectId, entry, refresh }: {
  projectId: string; entry: Registry; refresh: () => void;
}) {
  const [stage, setStage] = useState(entry.stage);
  const [driftOpen, setDriftOpen] = useState(false);
  const [deployOpen, setDeployOpen] = useState(false);
  useEffect(() => setStage(entry.stage), [entry.stage]);
  const action = useMutation({
    mutationFn: ({ path, body }: { path: string; body?: object }) =>
      api(`/projects/${projectId}/operations/registry/${entry.id}/${path}`, json("POST", body)),
    onSuccess: refresh,
  });
  return <div className="registry-card">
    <div className="registry-card__head"><div className="model-icon"><Gauge /></div>
      <div><b>{entry.model_name}</b><small>Version {entry.version}</small></div>
      <Badge status={entry.stage} /></div>
    <dl><div><dt>Champion metric</dt><dd>{entry.champion_metric_name
      ? `${titleCase(entry.champion_metric_name)} ${entry.champion_metric_value?.toFixed(4) || ""}`
      : "Not recorded"}</dd></div><div><dt>Registered</dt><dd>{formatDate(entry.created_at)}</dd></div></dl>
    {entry.is_fallback && <span className="fallback"><ShieldCheck size={14} /> Safe fallback</span>}
    {action.error && <Notice tone="danger">{action.error.message}</Notice>}
    <div className="registry-card__actions">
      <select aria-label={`Stage for ${entry.model_name}`} value={stage} onChange={(event) => setStage(event.target.value)}>
        <option value="candidate">Candidate</option><option value="staging">Staging</option>
        <option value="production">Production</option><option value="archived">Archived</option>
        <option value="rejected">Rejected</option>
      </select>
      <Button variant="secondary" disabled={stage === entry.stage || action.isPending}
        onClick={() => action.mutate({ path: "stage", body: { stage } })}>Update stage</Button>
      <Button variant="secondary" disabled={entry.is_fallback || entry.stage !== "staging"}
        onClick={() => action.mutate({ path: "fallback" })}>Set fallback</Button>
      <Button variant="secondary" onClick={() => setDriftOpen(true)}>
        <RefreshCw size={15} />Drift</Button>
      <Button disabled={!["staging", "production"].includes(entry.stage)}
        onClick={() => setDeployOpen(true)}><Rocket size={15} />Deploy</Button>
    </div>
    {driftOpen && <DriftModal projectId={projectId} entry={entry}
      close={() => setDriftOpen(false)} done={() => { setDriftOpen(false); refresh(); }} />}
    {deployOpen && <ConfirmModal title={`Deploy ${entry.model_name} v${entry.version}?`}
      description="This creates an inference workload in Kubernetes using one replica, 500m CPU, and 1 GiB memory."
      confirmLabel="Deploy model" close={() => setDeployOpen(false)}
      action={() => action.mutateAsync({
        path: "deployments", body: { replicas: 1, cpu_request: "500m", memory_request: "1Gi" },
      }).then(() => setDeployOpen(false))} />}
  </div>;
}

function DriftModal({ projectId, entry, close, done }: {
  projectId: string; entry: Registry; close: () => void; done: () => void;
}) {
  const [file, setFile] = useState<File | null>(null);
  const [uploadProgress, setUploadProgress] = useState(0);
  const [uploaded, setUploaded] = useState<DatasetUploadResult | null>(null);
  const [maxRows, setMaxRows] = useState(10_000);
  const uploadedColumns = (uploaded?.version.schema_json || uploaded?.version.dataset_schema)
    ?.columns?.map((column) => column.name) || [];
  const trainingFeatureColumns = entry.training_feature_columns || [];
  const missingColumns = trainingFeatureColumns.filter(
    (column) => !uploadedColumns.includes(column),
  );
  const upload = useMutation({
    mutationFn: async (selected: File) => {
      const body = new FormData();
      body.set("dataset_name", `Drift comparison · ${selected.name}`.slice(0, 220));
      body.set("description", `External drift data for ${entry.model_name}`);
      body.set("tags", JSON.stringify({ purpose: "drift", registry_entry_id: entry.id }));
      body.set("file", selected, selected.name);
      return uploadFormData<DatasetUploadResult>(
        `/projects/${projectId}/datasets/upload`, body, setUploadProgress,
      );
    },
    onSuccess: setUploaded,
  });
  const launch = useMutation({
    mutationFn: () => api(`/projects/${projectId}/operations/registry/${entry.id}/drift`,
      json("POST", { dataset_version_id: uploaded!.version.id, max_rows: maxRows, expected_minutes: 10 })),
    onSuccess: done,
  });
  return <Modal title="Run a drift check"
    description={`Compare current data with the training baseline for ${entry.model_name}.`} onClose={close}>
    <div className="stack"><Notice>Upload external data. Drift starts only after its feature columns match the registered model's training schema.</Notice>
      <label className="dropzone analysis-upload"><input type="file"
        accept=".csv,.parquet,.xlsx,.xls,.json,.jsonl" onChange={(event) => {
          setFile(event.target.files?.[0] || null); setUploaded(null); setUploadProgress(0); upload.reset();
        }} />
        <FileSpreadsheet /><b>{file?.name || "Choose external drift data"}</b>
        <span>CSV, Parquet, Excel, JSON, or JSONL</span></label>
      {file && !uploaded && <Button variant="secondary" loading={upload.isPending}
        onClick={() => upload.mutate(file)}><Upload size={15} />Upload and inspect</Button>}
      {upload.isPending && <div className="progress-panel"><div><b>Uploading drift dataset</b>
        <span>{uploadProgress}%</span></div><progress value={uploadProgress} max={100} /></div>}
      {upload.error && <Notice tone="danger">{upload.error.message}</Notice>}
      {uploaded && missingColumns.length > 0 && <Notice tone="danger">
        Drift cannot start. Missing training features: {missingColumns.join(", ")}.</Notice>}
      {uploaded && missingColumns.length === 0 && <Notice tone="success">
        Schema matched: all {trainingFeatureColumns.length} training features are present.</Notice>}
      <label>Maximum rows<input type="number" min={100} max={100000} step={100}
        value={maxRows} onChange={(event) => setMaxRows(Number(event.target.value))} /></label>
      {launch.error && <Notice tone="danger">{launch.error.message}</Notice>}
      <div className="modal__actions"><Button variant="ghost" onClick={close}>Cancel</Button>
        <Button disabled={!uploaded || missingColumns.length > 0}
          loading={launch.isPending} onClick={() => launch.mutate()}>
          Run drift check</Button></div>
    </div>
  </Modal>;
}

function DriftPanel({ runs }: { runs: DriftRun[] }) {
  const latest = runs.find((run) => run.status === "succeeded" && run.tags?.diagnostics);
  const diagnostics = latest?.tags.diagnostics;
  return <Card className="section-card">
    <div className="section-heading"><div><h2>Drift checks</h2>
      <p>Compare production-like data with registered training baselines.</p></div><Database className="section-icon" /></div>
    {runs.length ? <>
      {diagnostics && <div className="drift-summary"><Metric label="Latest drift share"
        value={`${(diagnostics.drift_share_percent || 0).toFixed(1)}%`} />
        <Metric label="Drifted features" value={diagnostics.drifted_feature_count || 0} />
        <progress value={diagnostics.drift_share_percent || 0} max={100} /></div>}
      <div className="table-wrap"><table><thead><tr><th>Run</th><th>Status</th>
        <th>Drift share</th><th>Drifted features</th><th>Created</th></tr></thead>
        <tbody>{runs.map((run) => <tr key={run.id}><td><b>{run.run_name || run.id.slice(0, 8)}</b></td>
          <td><Badge status={run.status} /></td>
          <td>{run.tags?.diagnostics?.drift_share_percent == null ? "—"
            : `${run.tags.diagnostics.drift_share_percent.toFixed(1)}%`}</td>
          <td>{run.tags?.diagnostics?.drifted_feature_count ?? "—"}</td>
          <td>{formatDate(run.created_at)}</td></tr>)}</tbody></table></div>
    </> : <div className="inline-empty"><Activity /><span><b>No drift checks yet</b>
      <small>Choose Drift on a registered model to compare a current dataset.</small></span></div>}
  </Card>;
}

function DeploymentRow({ projectId, deployment, refresh }: {
  projectId: string; deployment: DeployStatus; refresh: () => void;
}) {
  const [confirmStop, setConfirmStop] = useState(false);
  const [apiAccessOpen, setApiAccessOpen] = useState(false);
  const stop = useMutation({
    mutationFn: () => api(`/projects/${projectId}/operations/deployments/${deployment.run.id}/stop`, json("POST")),
    onSuccess: () => { setConfirmStop(false); refresh(); },
  });
  return <tr><td><b>{deployment.run.run_name || deployment.run.id.slice(0, 8)}</b>
    <small className="cell-sub">{formatDate(deployment.run.created_at)}</small></td>
    <td>{titleCase(deployment.runtime_state)}</td><td><Badge status={deployment.status} /></td>
    <td><DeploymentAccess deployment={deployment} openApiAccess={() => setApiAccessOpen(true)} /></td>
    <td>{!["cancelled", "failed"].includes(deployment.status) &&
      <Button variant="ghost" onClick={() => setConfirmStop(true)}><Square size={14} />Stop</Button>}
      {confirmStop && <ConfirmModal danger title="Stop this deployment?"
        description="The prediction endpoint will become unavailable. The registered model and evidence remain intact."
        confirmLabel="Stop deployment" close={() => setConfirmStop(false)}
        action={() => stop.mutateAsync()} />}
      {apiAccessOpen && <ApiAccessModal deployment={deployment}
        close={() => setApiAccessOpen(false)} />}</td></tr>;
}

function DeploymentAccess({ deployment, openApiAccess }: {
  deployment: DeployStatus; openApiAccess: () => void;
}) {
  const succeeded = deployment.status === "succeeded";
  const ready = succeeded && deployment.runtime_state === "ready";
  if (ready && (deployment.platform_endpoint || deployment.endpoint)) {
    return <Button variant="secondary" onClick={openApiAccess}>
      <Braces size={14} />API access</Button>;
  }
  if (["failed", "cancelled", "preempted"].includes(deployment.status)
    || ["missing", "unavailable"].includes(deployment.runtime_state)) {
    return <span className="endpoint-state endpoint-state--unavailable">Unavailable</span>;
  }
  if (succeeded) {
    return <span className="endpoint-state">API access unavailable</span>;
  }
  return <span className="endpoint-state">Provisioning</span>;
}

function ApiAccessModal({ deployment, close }: {
  deployment: DeployStatus; close: () => void;
}) {
  const platformEndpoints = [
    ["POST", "Batch prediction", deployment.platform_endpoint],
    ["POST", "Online prediction", deployment.platform_online_endpoint],
    ["POST", "Offline file prediction", deployment.platform_offline_endpoint],
    ["GET", "Model metadata", deployment.platform_metadata_url],
    ["GET", "Swagger documentation", deployment.platform_docs_url],
    ["GET", "OpenAPI schema", deployment.platform_openapi_url],
    ["GET", "Liveness", deployment.platform_live_url],
    ["GET", "Readiness", deployment.platform_ready_url],
  ] as const;
  const clusterDetails = [
    ["Cluster endpoint", deployment.internal_endpoint],
    ["Cluster docs", deployment.internal_docs_url],
    ["Cluster OpenAPI", deployment.internal_openapi_url],
  ] as const;
  const hasClusterDetails = clusterDetails.some(([, value]) => Boolean(value));
  return <Modal title="Model API access"
    description="Use Sceptre's authenticated gateway from the same address as this workspace."
    onClose={close}>
    <div className="stack endpoint-access">
      <Notice><strong>Bearer authentication required.</strong> Send your Sceptre access token in the <code>Authorization: Bearer &lt;token&gt;</code> header.</Notice>
      <section className="endpoint-access__section">
        <h3>Application gateway</h3>
        <p>These project-scoped URLs work through the API already serving this UI.</p>
        <div className="gateway-endpoints">{platformEndpoints.map(([method, label, path]) =>
          path && <PlatformEndpoint key={label} method={method} label={label} path={path} />)}</div>
      </section>
      {deployment.endpoint && <section className="endpoint-access__section">
        <h3>Direct external service</h3>
        <p>These links are available because external model exposure is configured for this cluster.</p>
        <div className="endpoint-links">
          <a href={deployment.endpoint} target="_blank" rel="noreferrer">
            Endpoint <ExternalLink size={13} /></a>
          {deployment.docs_url && <a href={deployment.docs_url} target="_blank" rel="noreferrer">
            Docs <ExternalLink size={13} /></a>}
          {deployment.openapi_url && <a href={deployment.openapi_url} target="_blank" rel="noreferrer">
            OpenAPI <ExternalLink size={13} /></a>}
        </div>
      </section>}
      {hasClusterDetails && <section className="endpoint-access__section endpoint-access__section--secondary">
        <h3>Kubernetes internal</h3>
        <p>These service addresses are reachable by workloads inside the cluster.</p>
        <div className="endpoint-access__urls">
          {clusterDetails.map(([label, value]) => value
            && <EndpointValue key={label} label={label} value={value} />)}
        </div>
        {deployment.service_name && deployment.namespace && <small>
          Service {deployment.service_name} in namespace {deployment.namespace}
        </small>}
      </section>}
      <div className="modal__actions"><Button variant="ghost" onClick={close}>Close</Button></div>
    </div>
  </Modal>;
}

function PlatformEndpoint({ method, label, path }: {
  method: string; label: string; path: string;
}) {
  const url = new URL(path, window.location.origin).toString();
  return <div><span className={`gateway-method gateway-method--${method.toLowerCase()}`}>{method}</span>
    <span><b>{label}</b><code>{url}</code></span></div>;
}

function EndpointValue({ label, value }: { label: string; value?: string | null }) {
  return <div><span>{label}</span><code>{value || "Not reported"}</code></div>;
}

function CleanupPanel({ projectId }: { projectId: string }) {
  const [days, setDays] = useState(30);
  const [confirm, setConfirm] = useState(false);
  const cleanup = useMutation({
    mutationFn: (dryRun: boolean) => api<CleanupResult>(`/projects/${projectId}/operations/cleanup`,
      json("POST", { older_than_days: days, dry_run: dryRun, cleanup_finished_jobs: true })),
  });
  return <Card className="section-card cleanup-card">
    <div className="section-heading"><div><h2>Resource cleanup</h2>
      <p>Preview protected artifact cleanup before deleting anything.</p></div><Trash2 className="section-icon" /></div>
    <div className="cleanup-controls"><label>Artifacts older than
      <input type="number" min={1} max={3650} value={days}
        onChange={(event) => setDays(Number(event.target.value))} /><small>days</small></label>
      <Button variant="secondary" loading={cleanup.isPending}
        onClick={() => cleanup.mutate(true)}>Preview cleanup</Button>
      <Button variant="danger" disabled={!cleanup.data || !cleanup.data.dry_run || cleanup.data.artifact_count === 0}
        onClick={() => setConfirm(true)}><Trash2 size={15} />Delete eligible resources</Button></div>
    {cleanup.error && <Notice tone="danger">{cleanup.error.message}</Notice>}
    {cleanup.data && <div className="cleanup-result">
      <Metric label={cleanup.data.dry_run ? "Eligible artifacts" : "Deleted artifacts"}
        value={cleanup.data.artifact_count} />
      <Metric label="Artifact size" value={formatBytes(cleanup.data.artifact_bytes)} />
      <Metric label="Kubernetes jobs removed" value={cleanup.data.deleted_kubernetes_jobs.length} />
      {cleanup.data.errors.length > 0 && <Notice tone="danger">{cleanup.data.errors.join(" ")}</Notice>}
    </div>}
    {confirm && <ConfirmModal danger title={`Delete ${cleanup.data?.artifact_count || 0} eligible artifacts?`}
      description="This cannot be undone. Protected artifacts attached to active registry entries are excluded by the API."
      confirmLabel="Delete resources" close={() => setConfirm(false)}
      action={() => cleanup.mutateAsync(false).then(() => setConfirm(false))} />}
  </Card>;
}

function ConfirmModal({ title, description, confirmLabel, action, close, danger = false }: {
  title: string; description: string; confirmLabel: string; action: () => Promise<unknown>;
  close: () => void; danger?: boolean;
}) {
  const [pending, setPending] = useState(false);
  const [error, setError] = useState("");
  async function confirm() {
    setPending(true); setError("");
    try { await action(); } catch (cause) {
      setError(cause instanceof Error ? cause.message : "The action failed."); setPending(false);
    }
  }
  return <Modal title={title} description={description} onClose={close}>
    <div className="stack">{danger && <Notice tone="danger"><AlertTriangle size={16} />
      Review the impact before continuing.</Notice>}{error && <Notice tone="danger">{error}</Notice>}
      <div className="modal__actions"><Button variant="ghost" disabled={pending} onClick={close}>Cancel</Button>
        <Button variant={danger ? "danger" : "primary"} loading={pending} onClick={confirm}>{confirmLabel}</Button></div>
    </div>
  </Modal>;
}

function RegisterModal({ projectId, initialRunId, initialModel, close, done }: {
  projectId: string; initialRunId: string; initialModel: string;
  close: () => void; done: () => void;
}) {
  const [runId, setRunId] = useState(initialRunId);
  const [model, setModel] = useState(initialModel);
  const runs = useQuery({
    queryKey: ["runs", projectId],
    queryFn: () => api<ModelRun[]>(`/projects/${projectId}/training/runs`),
  });
  const successful = runs.data?.filter((run) => run.status === "succeeded") || [];
  const board = useQuery({
    queryKey: ["leaderboard", projectId, runId], enabled: Boolean(runId),
    queryFn: () => api<Leaderboard>(`/projects/${projectId}/training/runs/${runId}/leaderboard`),
  });
  const models = useMemo(
    () => board.data?.entries.filter((entry) => entry.status === "succeeded")
      .map((entry) => entry.model) || [],
    [board.data],
  );
  useEffect(() => {
    if (models.length && !models.includes(model)) setModel(models[0]);
  }, [models, model]);
  const register = useMutation({
    mutationFn: () => api(`/projects/${projectId}/operations/registry`,
      json("POST", { training_run_id: runId, model_name: model })),
    onSuccess: done,
  });
  return <Modal title="Register a trained model"
    description="Attach a successful candidate to the governed model registry." onClose={close}>
    {runs.isLoading ? <Loading /> : successful.length ? <div className="stack">
      <label>Training run<select value={runId} onChange={(event) => { setRunId(event.target.value); setModel(""); }}>
        <option value="">Select a run</option>{successful.map((run) =>
          <option value={run.id} key={run.id}>{run.run_name || run.id.slice(0, 8)}</option>)}</select></label>
      <label>Successful candidate<select value={model} disabled={!models.length}
        onChange={(event) => setModel(event.target.value)}>
        <option value="">{runId ? "Select a model" : "Choose a run first"}</option>
        {models.map((name) => <option key={name}>{name}</option>)}</select></label>
      <Notice><Cpu size={16} />Only successful leaderboard candidates can be registered.</Notice>
      {register.error && <Notice tone="danger">{register.error.message}</Notice>}
      <div className="modal__actions"><Button variant="ghost" onClick={close}>Cancel</Button>
        <Button disabled={!runId || !model} loading={register.isPending}
          onClick={() => register.mutate()}>Register model</Button></div>
    </div> : <EmptyState icon={<Activity />} title="No successful runs"
      description="Complete a training run before registering a model." />}
  </Modal>;
}
