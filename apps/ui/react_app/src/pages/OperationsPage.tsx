import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Activity, Box, CloudCog, Cpu, ExternalLink, Gauge, Rocket, ShieldCheck, Square } from "lucide-react";
import { useState } from "react";
import { useParams } from "react-router-dom";
import { api, json } from "../api";
import { Badge, Button, Card, EmptyState, ErrorState, Loading, Metric, Modal, Notice, PageHeader } from "../components/ui";
import { formatDate, titleCase } from "../lib";
import type { ModelRun, PlatformHealth } from "../types";

interface Registry {
  id: string; model_run_id: string; stage: string; model_name: string; version: number;
  champion_metric_name: string | null; champion_metric_value: number | null; is_fallback: boolean; created_at: string;
}
interface DeployStatus { run: ModelRun; runtime_state: string; endpoint: string | null; status: string }

export function OperationsPage() {
  const { projectId = "" } = useParams();
  const client = useQueryClient();
  const [registerOpen, setRegisterOpen] = useState(false);
  const health = useQuery({ queryKey: ["health", projectId], queryFn: () => api<PlatformHealth>(`/projects/${projectId}/operations/health`), refetchInterval: 15_000 });
  const registry = useQuery({ queryKey: ["registry", projectId], queryFn: () => api<Registry[]>(`/projects/${projectId}/operations/registry`) });
  const deployments = useQuery({ queryKey: ["deployments", projectId], queryFn: () => api<DeployStatus[]>(`/projects/${projectId}/operations/deployments`), refetchInterval: 8000 });
  if (health.isLoading || registry.isLoading) return <Loading />;
  if (health.error) return <ErrorState error={health.error} retry={() => health.refetch()} />;
  return <>
    <PageHeader eyebrow="Model operations" title="Deploy & monitor" description="Promote approved models, track runtime health, and keep a safe fallback."
      action={<Button onClick={() => setRegisterOpen(true)}><Box size={16} />Register model</Button>} />
    <div className="metrics-grid"><Metric label="Platform" value={health.data?.capacity.connected ? "Connected" : "Degraded"} hint={`${health.data?.capacity.ready_nodes || 0} ready nodes`} /><Metric label="Available CPU" value={health.data?.capacity.available_cpu_cores.toFixed(1) || "—"} hint="cluster cores" /><Metric label="Available memory" value={`${((health.data?.capacity.available_memory_mb || 0) / 1024).toFixed(1)} GiB`} /><Metric label="Active deployments" value={health.data?.active_deployments || 0} /></div>
    {health.data?.capacity.warnings.length ? <Notice>{health.data.capacity.warnings.join(" ")}</Notice> : null}
    <Card className="section-card"><div className="section-heading"><div><h2>Model registry</h2><p>The governed source of deployable model versions.</p></div><ShieldCheck className="section-icon" /></div>
      {registry.data?.length ? <div className="registry-grid">{registry.data.map((entry) => <RegistryCard key={entry.id} projectId={projectId} entry={entry} refresh={() => { client.invalidateQueries({ queryKey: ["registry", projectId] }); client.invalidateQueries({ queryKey: ["deployments", projectId] }); }} />)}</div>
        : <EmptyState icon={<Box />} title="No registered models" description="Register a successful training candidate before promoting or deploying it." action={<Button onClick={() => setRegisterOpen(true)}>Register model</Button>} />}</Card>
    <Card className="section-card"><div className="section-heading"><div><h2>Deployments</h2><p>Live and historical inference services.</p></div><CloudCog className="section-icon" /></div>
      {deployments.data?.length ? <div className="table-wrap"><table><thead><tr><th>Deployment</th><th>Runtime</th><th>Status</th><th>Endpoint</th><th /></tr></thead><tbody>{deployments.data.map((deployment) =>
        <DeploymentRow key={deployment.run.id} projectId={projectId} deployment={deployment} refresh={() => deployments.refetch()} />)}</tbody></table></div>
        : <div className="inline-empty"><Rocket /><span><b>No model deployments</b><small>Promote a registry entry and deploy it when the evidence is ready.</small></span></div>}</Card>
    {registerOpen && <RegisterModal projectId={projectId} close={() => setRegisterOpen(false)} done={() => { setRegisterOpen(false); registry.refetch(); }} />}
  </>;
}

function RegistryCard({ projectId, entry, refresh }: { projectId: string; entry: Registry; refresh: () => void }) {
  const action = useMutation({ mutationFn: ({ path, body }: { path: string; body?: object }) => api(`/projects/${projectId}/operations/registry/${entry.id}/${path}`, json("POST", body)), onSuccess: refresh });
  return <div className="registry-card"><div className="registry-card__head"><div className="model-icon"><Gauge /></div><div><b>{entry.model_name}</b><small>Version {entry.version}</small></div><Badge status={entry.stage} /></div>
    <dl><div><dt>Champion metric</dt><dd>{entry.champion_metric_name ? `${titleCase(entry.champion_metric_name)} ${entry.champion_metric_value?.toFixed(4) || ""}` : "Not recorded"}</dd></div><div><dt>Registered</dt><dd>{formatDate(entry.created_at)}</dd></div></dl>
    {entry.is_fallback && <span className="fallback"><ShieldCheck size={14} /> Safe fallback</span>}
    {action.error && <Notice tone="danger">{action.error.message}</Notice>}
    <div className="registry-card__actions"><select value={entry.stage} onChange={(e) => action.mutate({ path: "stage", body: { stage: e.target.value } })}><option value="candidate">Candidate</option><option value="staging">Staging</option><option value="production">Production</option><option value="archived">Archived</option><option value="rejected">Rejected</option></select>
      <Button variant="secondary" disabled={entry.is_fallback} onClick={() => action.mutate({ path: "fallback" })}>Set fallback</Button>
      <Button disabled={entry.stage !== "production"} onClick={() => action.mutate({ path: "deployments", body: { replicas: 1, cpu_request: "500m", memory_request: "1Gi" } })}><Rocket size={15} />Deploy</Button></div></div>;
}

function DeploymentRow({ projectId, deployment, refresh }: { projectId: string; deployment: DeployStatus; refresh: () => void }) {
  const stop = useMutation({ mutationFn: () => api(`/projects/${projectId}/operations/deployments/${deployment.run.id}/stop`, json("POST")), onSuccess: refresh });
  return <tr><td><b>{deployment.run.run_name || deployment.run.id.slice(0, 8)}</b><small className="cell-sub">{formatDate(deployment.run.created_at)}</small></td><td>{titleCase(deployment.runtime_state)}</td><td><Badge status={deployment.status} /></td><td>{deployment.endpoint ? <a href={deployment.endpoint} target="_blank" rel="noreferrer">Open endpoint <ExternalLink size={13} /></a> : "Pending"}</td><td>{["queued", "running", "succeeded"].includes(deployment.status) && <Button variant="ghost" loading={stop.isPending} onClick={() => stop.mutate()}><Square size={14} />Stop</Button>}</td></tr>;
}

function RegisterModal({ projectId, close, done }: { projectId: string; close: () => void; done: () => void }) {
  const [runId, setRunId] = useState(""); const [model, setModel] = useState("");
  const runs = useQuery({ queryKey: ["runs", projectId], queryFn: () => api<ModelRun[]>(`/projects/${projectId}/training/runs`) });
  const successful = runs.data?.filter((run) => run.status === "succeeded") || [];
  const register = useMutation({ mutationFn: () => api(`/projects/${projectId}/operations/registry`, json("POST", { training_run_id: runId, model_name: model })), onSuccess: done });
  return <Modal title="Register a trained model" description="Attach a successful candidate to the governed model registry." onClose={close}>
    {runs.isLoading ? <Loading /> : successful.length ? <div className="stack"><label>Training run<select value={runId} onChange={(e) => setRunId(e.target.value)}><option value="">Select a run</option>{successful.map((run) => <option value={run.id} key={run.id}>{run.run_name || run.id.slice(0, 8)}</option>)}</select></label>
      <label>Model name<input value={model} onChange={(e) => setModel(e.target.value)} placeholder="Exact leaderboard model name" /></label>
      <Notice><Cpu size={16} /> Use the exact candidate name shown on the run leaderboard.</Notice>{register.error && <Notice tone="danger">{register.error.message}</Notice>}
      <div className="modal__actions"><Button variant="ghost" onClick={close}>Cancel</Button><Button disabled={!runId || !model} loading={register.isPending} onClick={() => register.mutate()}>Register model</Button></div></div>
      : <EmptyState icon={<Activity />} title="No successful runs" description="Complete a training run before registering a model." />}</Modal>;
}
