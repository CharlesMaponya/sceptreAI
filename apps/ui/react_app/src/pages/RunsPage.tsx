import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { BarChart3, BrainCircuit, ChevronRight, CircleStop, Play, RefreshCw, Trophy } from "lucide-react";
import { useEffect, useMemo, useState } from "react";
import { useParams } from "react-router-dom";
import { api, json } from "../api";
import { Badge, Button, Card, EmptyState, ErrorState, Loading, Metric, Notice, PageHeader } from "../components/ui";
import { formatDate, titleCase } from "../lib";
import type { Leaderboard, ModelRun } from "../types";

type Analysis = ModelRun & { run_name: string | null };

export function RunsPage() {
  const { projectId = "" } = useParams();
  const client = useQueryClient();
  const [selectedId, setSelectedId] = useState("");
  const runs = useQuery({
    queryKey: ["runs", projectId], queryFn: () => api<ModelRun[]>(`/projects/${projectId}/training/runs`),
    refetchInterval: (query) => query.state.data?.some((run) => ["queued", "precheck_running", "running"].includes(run.status)) ? 3000 : false,
  });
  useEffect(() => { if (runs.data?.length && !runs.data.some((run) => run.id === selectedId)) setSelectedId(runs.data[0].id); }, [runs.data, selectedId]);
  const selected = runs.data?.find((run) => run.id === selectedId);
  if (runs.isLoading) return <Loading />;
  if (runs.error) return <ErrorState error={runs.error} retry={() => runs.refetch()} />;
  return <>
    <PageHeader eyebrow="Evidence workspace" title="Results & validation" description="Compare candidates, inspect diagnostics, and challenge a model before promotion." />
    {!runs.data?.length ? <Card><EmptyState icon={<BarChart3 />} title="No experiment results yet" description="Launch a training run to compare model candidates and build an evidence trail." /></Card> :
      <div className="runs-layout"><Card className="run-list"><div className="run-list__head"><b>Training runs</b><span>{runs.data.length}</span></div>{runs.data.map((run) =>
        <button key={run.id} className={selectedId === run.id ? "active" : ""} onClick={() => setSelectedId(run.id)}><span><b>{run.run_name || run.id.slice(0, 8)}</b><small>{titleCase(run.task_type)} · {formatDate(run.created_at)}</small></span><Badge status={run.status} /><ChevronRight size={16} /></button>)}</Card>
        {selected && <RunDetail projectId={projectId} run={selected} invalidate={() => client.invalidateQueries({ queryKey: ["runs", projectId] })} />}</div>}
  </>;
}

function RunDetail({ projectId, run, invalidate }: { projectId: string; run: ModelRun; invalidate: () => void }) {
  const [tab, setTab] = useState<"leaderboard" | "analysis">("leaderboard");
  const [model, setModel] = useState("");
  const leaderboard = useQuery({
    queryKey: ["leaderboard", projectId, run.id], queryFn: () => api<Leaderboard>(`/projects/${projectId}/training/runs/${run.id}/leaderboard`),
    refetchInterval: ["queued", "precheck_running", "running"].includes(run.status) ? 3000 : false,
  });
  const analyses = useQuery({ queryKey: ["analyses", run.id], enabled: run.status === "succeeded", queryFn: () => api<Analysis[]>(`/projects/${projectId}/training/runs/${run.id}/analyses`), refetchInterval: 5000 });
  const successful = useMemo(() => leaderboard.data?.entries.filter((entry) => entry.status === "succeeded") || [], [leaderboard.data]);
  useEffect(() => { if (successful.length && !successful.some((entry) => entry.model === model)) setModel(successful[0].model); }, [successful, model]);
  const cancel = useMutation({ mutationFn: () => api(`/projects/${projectId}/training/runs/${run.id}/cancel`, json("POST")), onSuccess: invalidate });
  const restart = useMutation({ mutationFn: () => api(`/projects/${projectId}/training/runs/${run.id}/restart`, json("POST")), onSuccess: invalidate });
  const explain = useMutation({
    mutationFn: () => api(`/projects/${projectId}/training/runs/${run.id}/explanations`, json("POST", { model_name: model, max_rows: 200, expected_minutes: 10 })),
    onSuccess: () => analyses.refetch(),
  });
  const winner = leaderboard.data?.entries.find((entry) => entry.model === leaderboard.data?.winner);
  return <div className="run-detail">
    <Card className="run-summary"><div className="section-heading"><div><span className="eyebrow">Training run</span><h2>{run.run_name || run.id.slice(0, 8)}</h2><p>{titleCase(run.task_type)} · Created {formatDate(run.created_at)}</p></div><Badge status={run.status} /></div>
      {run.plain_english_failure && <Notice tone="danger">{run.plain_english_failure}</Notice>}
      <div className="metrics-grid metrics-grid--compact"><Metric label="Candidates" value={leaderboard.data?.entries.length || "—"} /><Metric label="Winner" value={leaderboard.data?.winner || "Pending"} /><Metric label="Primary metric" value={titleCase(leaderboard.data?.primary_metric || "Pending")} /><Metric label="Finished" value={formatDate(run.finished_at)} /></div>
      {["queued", "precheck_running", "running"].includes(run.status) && <Button variant="danger" loading={cancel.isPending} onClick={() => cancel.mutate()}><CircleStop size={16} />Cancel run</Button>}
      {["failed", "cancelled", "preempted"].includes(run.status) && <Button variant="secondary" loading={restart.isPending} onClick={() => restart.mutate()}><RefreshCw size={16} />Restart run</Button>}
    </Card>
    <div className="tabs" role="tablist"><button className={tab === "leaderboard" ? "active" : ""} onClick={() => setTab("leaderboard")}>Leaderboard</button><button className={tab === "analysis" ? "active" : ""} onClick={() => setTab("analysis")}>Validate & explain</button></div>
    {tab === "leaderboard" ? <Card className="section-card">
      {leaderboard.isLoading ? <Loading label="Loading model evidence…" /> : leaderboard.error ? <ErrorState error={leaderboard.error} retry={() => leaderboard.refetch()} /> :
        leaderboard.data?.entries.length ? <><div className="winner-banner"><Trophy /><div><span>Top candidate</span><b>{leaderboard.data.winner || "Ranking in progress"}</b><small>{winner?.primary_score != null ? `${titleCase(leaderboard.data.primary_metric || "score")}: ${winner.primary_score.toFixed(4)}` : "Results are still being collected"}</small></div></div>
          <div className="table-wrap"><table><thead><tr><th>Rank</th><th>Model</th><th>Status</th><th>{titleCase(leaderboard.data.primary_metric || "Score")}</th><th>Duration</th></tr></thead><tbody>{leaderboard.data.entries.map((entry) =>
            <tr key={entry.model}><td className="rank">{entry.rank || "—"}</td><td><b>{entry.model}</b><small className="cell-sub">{titleCase(entry.cost_tier)} cost</small></td><td><Badge status={entry.status} /></td><td className="score">{entry.primary_score?.toFixed(4) || "—"}</td><td>{entry.duration_seconds ? `${entry.duration_seconds.toFixed(1)}s` : "—"}</td></tr>)}</tbody></table></div></>
          : <EmptyState title="Results are on their way" description="Candidates appear progressively as training completes." />}</Card>
      : <Card className="analysis-card"><div><BrainCircuit /><span><h2>Challenge a candidate</h2><p>Generate SHAP feature contributions for a successful model. Results are persisted with this run.</p></span></div>
        {successful.length ? <><label>Candidate model<select value={model} onChange={(e) => setModel(e.target.value)}>{successful.map((entry) => <option key={entry.model}>{entry.model}</option>)}</select></label>
          {explain.error && <Notice tone="danger">{explain.error.message}</Notice>}<Button loading={explain.isPending} onClick={() => explain.mutate()}><Play size={16} />Calculate SHAP explanation</Button></> : <Notice>Successful model candidates are required before analysis.</Notice>}
        <h3>Analysis history</h3>{analyses.data?.length ? <div className="table-wrap"><table><thead><tr><th>Analysis</th><th>Type</th><th>Status</th><th>Created</th></tr></thead><tbody>{analyses.data.map((item) =>
          <tr key={item.id}><td><b>{item.run_name || item.id.slice(0, 8)}</b></td><td>{titleCase(item.run_kind)}</td><td><Badge status={item.status} /></td><td>{formatDate(item.created_at)}</td></tr>)}</tbody></table></div> : <p className="muted">No validation or explainability jobs yet.</p>}
      </Card>}
  </div>;
}
