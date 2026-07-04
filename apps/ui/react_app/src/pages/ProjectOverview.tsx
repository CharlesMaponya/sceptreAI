import { useQuery } from "@tanstack/react-query";
import { ArrowRight, BarChart3, Rocket, Sparkles, Upload } from "lucide-react";
import { Link, useParams } from "react-router-dom";
import { api } from "../api";
import { Badge, Card, ErrorState, Loading, Metric, PageHeader } from "../components/ui";
import { formatDate, titleCase } from "../lib";
import type { Dataset, ModelRun, Project } from "../types";

export function ProjectOverview() {
  const { projectId = "" } = useParams();
  const project = useQuery({ queryKey: ["project", projectId], queryFn: () => api<Project>(`/projects/${projectId}`) });
  const datasets = useQuery({ queryKey: ["datasets", projectId], queryFn: () => api<Dataset[]>(`/projects/${projectId}/datasets`) });
  const runs = useQuery({ queryKey: ["runs", projectId], queryFn: () => api<ModelRun[]>(`/projects/${projectId}/training/runs`), refetchInterval: 10_000 });
  if (project.isLoading || datasets.isLoading || runs.isLoading) return <Loading />;
  if (project.error) return <ErrorState error={project.error} retry={() => project.refetch()} />;
  const recent = runs.data?.slice(0, 4) || [];
  const active = runs.data?.filter((run) => ["queued", "precheck_running", "running"].includes(run.status)).length || 0;
  const succeeded = runs.data?.filter((run) => run.status === "succeeded").length || 0;
  const next = !datasets.data?.length ? { to: "data", icon: Upload, title: "Bring in your first dataset", text: "Upload CSV, Parquet, Excel, JSON, or JSONL. Profiling starts automatically.", action: "Upload data" }
    : !runs.data?.length ? { to: "training", icon: Sparkles, title: "Your data is ready to train", text: "Choose a target and candidate models, then review compute requirements before launch.", action: "Configure training" }
      : { to: "runs", icon: BarChart3, title: "Review model evidence", text: "Compare ranked candidates, diagnostics, validation results, and explanations.", action: "Open results" };
  const NextIcon = next.icon;

  return <>
    <PageHeader eyebrow="Project overview" title={project.data?.name || "Project"} description={project.data?.description || "Your governed model workspace."} />
    <div className="metrics-grid"><Metric label="Datasets" value={datasets.data?.length || 0} hint="immutable sources" />
      <Metric label="Training runs" value={runs.data?.length || 0} hint={`${active} currently active`} />
      <Metric label="Successful runs" value={succeeded} hint="ready to review" />
      <Metric label="Last activity" value={formatDate(runs.data?.[0]?.created_at || project.data?.updated_at)} /></div>
    <div className="overview-grid">
      <Card className="next-card"><div className="next-card__icon"><NextIcon /></div><div><span className="eyebrow">Recommended next step</span><h2>{next.title}</h2><p>{next.text}</p>
        <Link className="button button--primary" to={next.to}>{next.action}<ArrowRight size={16} /></Link></div></Card>
      <Card className="journey"><h2>Model journey</h2><p className="muted">A clear path from raw data to an operating model.</p>
        {[
          ["1", "Data", "Upload and profile", datasets.data?.length ? "complete" : "current"],
          ["2", "Train", "Compare candidates", succeeded ? "complete" : datasets.data?.length ? "current" : "upcoming"],
          ["3", "Validate", "Challenge and explain", succeeded ? "current" : "upcoming"],
          ["4", "Operate", "Promote and monitor", "upcoming"],
        ].map(([number, name, text, state]) => <div className={`journey__step journey__step--${state}`} key={name}><i>{state === "complete" ? "✓" : number}</i><div><b>{name}</b><small>{text}</small></div></div>)}</Card>
    </div>
    <Card className="section-card"><div className="section-heading"><div><h2>Recent training</h2><p>Latest activity across this project.</p></div><Link to="runs">View all <ArrowRight size={15} /></Link></div>
      {recent.length ? <div className="table-wrap"><table><thead><tr><th>Run</th><th>Task</th><th>Status</th><th>Created</th></tr></thead><tbody>{recent.map((run) =>
        <tr key={run.id}><td><b>{run.run_name || run.id.slice(0, 8)}</b></td><td>{titleCase(run.task_type)}</td><td><Badge status={run.status} /></td><td>{formatDate(run.created_at)}</td></tr>)}</tbody></table></div>
        : <div className="inline-empty"><Rocket /><span><b>No training runs yet</b><small>Your completed experiments will appear here.</small></span></div>}</Card>
  </>;
}
