import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { ArrowRight, Rocket, Sparkles, Upload } from "lucide-react";
import { lazy, Suspense, useEffect, useMemo, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { api, json } from "../api";
import { Badge, Button, Card, ErrorState, Loading, Metric, Notice, PageHeader } from "../components/ui";
import { formatDate, titleCase } from "../lib";
import type { Dataset, DatasetColumnPreview, DatasetVersion, ModelRun, ProfileJob, Project, TaskType } from "../types";

const NO_TARGET = "No target";
const TERMINAL_PROFILE_STATUSES = ["succeeded", "failed", "cancelled"];
const PlotlyChart = lazy(() => import("../components/PlotlyChart"));

type TargetColumnProfile = {
  name: string;
  semantic_type: string;
  statistics: Record<string, string | number | null>;
  distribution: Array<{ label: string; count: number }>;
  preview_values?: Array<string | number>;
  preview_distribution?: Array<{ label: string; count: number }>;
};
type CompletedProfile = ProfileJob & {
  feature_profiles_json: Record<string, TargetColumnProfile>;
};

export function ProjectOverview() {
  const { projectId = "" } = useParams();
  const client = useQueryClient();
  const [datasetId, setDatasetId] = useState("");
  const [target, setTarget] = useState(NO_TARGET);
  const project = useQuery({ queryKey: ["project", projectId], queryFn: () => api<Project>(`/projects/${projectId}`) });
  const datasets = useQuery({ queryKey: ["datasets", projectId], queryFn: () => api<Dataset[]>(`/projects/${projectId}/datasets`) });
  const runs = useQuery({ queryKey: ["runs", projectId], queryFn: () => api<ModelRun[]>(`/projects/${projectId}/training/runs`), refetchInterval: 10_000 });
  useEffect(() => {
    if (datasets.data?.length && !datasets.data.some((dataset) => dataset.id === datasetId)) {
      setDatasetId(datasets.data[0].id);
    }
  }, [datasets.data, datasetId]);
  const versions = useQuery({
    queryKey: ["versions", projectId, datasetId],
    enabled: Boolean(datasetId),
    queryFn: () => api<DatasetVersion[]>(`/projects/${projectId}/datasets/${datasetId}/versions`),
  });
  const version = versions.data?.[0];
  const columns = useMemo(
    () => (version?.schema_json || version?.dataset_schema)?.columns?.map((column) => column.name) || [],
    [version],
  );
  const profilePath = version
    ? `/projects/${projectId}/datasets/${datasetId}/versions/${version.id}`
    : "";
  const profile = useQuery({
    queryKey: ["profile", version?.id],
    enabled: Boolean(version),
    queryFn: () => api<ProfileJob | null>(`${profilePath}/profile-jobs/latest`),
    refetchInterval: (query) => query.state.data
      && !TERMINAL_PROFILE_STATUSES.includes(query.state.data.status) ? 2_000 : false,
  });
  useEffect(() => {
    setTarget(profile.data?.target_column || NO_TARGET);
  }, [profile.data?.id, profile.data?.target_column, version?.id]);
  const normalizedTarget = target === NO_TARGET ? null : target;
  const targetChanged = Boolean(profile.data)
    && normalizedTarget !== (profile.data?.target_column || null);
  const profileActive = Boolean(profile.data)
    && !TERMINAL_PROFILE_STATUSES.includes(profile.data!.status);
  const startProfile = useMutation({
    mutationFn: () => api<ProfileJob>(`${profilePath}/profile-jobs`, json("POST", {
      target_column: normalizedTarget,
      force: false,
    })),
    onSuccess: (job) => {
      client.setQueryData(["profile", version?.id], job);
      client.invalidateQueries({ queryKey: ["profile", version?.id] });
    },
  });
  const inferredTask = profile.data?.overview_json?.task_inference;
  const profileSucceeded = profile.data?.status === "succeeded";
  const targetProfileResult = useQuery({
    queryKey: ["profile-result", profile.data?.id],
    enabled: Boolean(
      profileSucceeded
      && profile.data?.target_column
      && inferredTask?.task_type !== "clustering",
    ),
    queryFn: () => api<CompletedProfile>(`${profilePath}/profile-jobs/${profile.data!.id}/result`),
  });

  if (project.isLoading || datasets.isLoading || runs.isLoading) return <Loading />;
  const error = project.error || datasets.error || runs.error;
  if (error) return <ErrorState error={error} retry={() => {
    project.refetch(); datasets.refetch(); runs.refetch();
  }} />;

  const recent = runs.data?.slice(0, 4) || [];
  const active = runs.data?.filter((run) => ["queued", "precheck_running", "running"].includes(run.status)).length || 0;
  const succeeded = runs.data?.filter((run) => run.status === "succeeded").length || 0;
  const selectedDataset = datasets.data?.find((dataset) => dataset.id === datasetId);
  const selectedColumn = columns.length
    ? (version?.schema_json || version?.dataset_schema)?.columns?.find(
      (column) => column.name === normalizedTarget,
    )
    : undefined;
  const targetProfile = profile.data?.target_column
    ? targetProfileResult.data?.feature_profiles_json?.[profile.data.target_column]
    : undefined;
  const profileMatchesSelection = profileSucceeded && !targetChanged;
  const provisionalTask = inferPreviewTask(normalizedTarget, selectedColumn);
  const displayedTask = profileMatchesSelection && inferredTask
    ? inferredTask.task_type
    : provisionalTask;
  const previewProfile = selectedColumn ? previewColumnProfile(selectedColumn) : undefined;
  const displayedTargetProfile = profileMatchesSelection && targetProfile
    ? targetProfile
    : previewProfile;
  const canStartProfile = Boolean(version)
    && !profileActive
    && (!profile.data || targetChanged || ["failed", "cancelled"].includes(profile.data.status));
  const actionLabel = targetChanged ? "Reprofile with target" : "Start profile";
  const guidanceTitle = !datasets.data?.length
    ? "Bring in your first dataset"
    : profileMatchesSelection && inferredTask
      ? `${titleCase(inferredTask.task_type)} task identified`
      : profileActive
        ? "Profiling your dataset"
        : normalizedTarget
          ? `${titleCase(provisionalTask)} task preview`
          : "Choose what you want to predict";
  const GuidanceIcon = !datasets.data?.length ? Upload : Sparkles;

  return <>
    <PageHeader eyebrow="Project overview" title={project.data?.name || "Project"} description={project.data?.description || "Your governed model workspace."} />
    <div className="metrics-grid"><Metric label="Datasets" value={datasets.data?.length || 0} hint="immutable sources" />
      <Metric label="Training runs" value={runs.data?.length || 0} hint={`${active} currently active`} />
      <Metric label="Successful runs" value={succeeded} hint="ready to review" />
      <Metric label="Last activity" value={formatDate(runs.data?.[0]?.created_at || project.data?.updated_at)} /></div>
    <div className="overview-grid">
      <Card className="next-card"><div className="next-card__icon"><GuidanceIcon /></div><div className="overview-guidance"><span className="eyebrow">Project guidance</span><h2>{guidanceTitle}</h2>
        {!datasets.data?.length ? <><p>Upload CSV, Parquet, Excel, JSON, or JSONL. You will choose a target before profiling begins.</p>
          <Link className="button button--primary" to="data">Upload data<ArrowRight size={16} /></Link></> : <>
          <p>Select the latest dataset you want to work with and choose its target column. Profiling starts only when you confirm.</p>
          <div className="overview-guidance__controls">
            <label>Dataset<select value={datasetId} onChange={(event) => setDatasetId(event.target.value)}>
              {datasets.data?.map((dataset) => <option value={dataset.id} key={dataset.id}>{dataset.name}</option>)}
            </select></label>
            <label>Target column<select value={target} onChange={(event) => setTarget(event.target.value)} disabled={!version || profileActive}>
              <option value={NO_TARGET}>{NO_TARGET}</option>
              {columns.map((column) => <option value={column} key={column}>{column}</option>)}
            </select></label>
            {canStartProfile && <Button loading={startProfile.isPending} onClick={() => startProfile.mutate()}>{actionLabel}</Button>}
          </div>
          {versions.isLoading || profile.isLoading ? <Loading label="Checking the latest dataset profile…" /> : null}
          {!version && !versions.isLoading && <Notice tone="danger">The selected dataset has no available version.</Notice>}
          {startProfile.error && <Notice tone="danger">{startProfile.error.message}</Notice>}
          {version && <div className="profile-summary"><div><span>{profileMatchesSelection ? "Inferred task" : "Provisional task"}</span><strong>{titleCase(displayedTask)}</strong></div>
            <div><span>Target</span><strong>{normalizedTarget || "No target"}</strong></div><p>{profileMatchesSelection && inferredTask
              ? inferredTask.rationale
              : previewTaskRationale(displayedTask, normalizedTarget)}</p></div>}
          {displayedTask !== "clustering" && normalizedTarget && displayedTargetProfile
            && <TargetVisualization task={displayedTask} profile={displayedTargetProfile} preview={!profileMatchesSelection || !targetProfile} />}
          {profileActive && <div className="progress-panel" role="status"><div><b>{titleCase(profile.data?.current_stage || "Preparing")}</b><span>{Math.round((profile.data?.progress || 0) * 100)}%</span></div>
            <progress value={profile.data?.progress || 0} max={1} /><p>Profiling is running in the background. You can continue using the project.</p></div>}
          {profileMatchesSelection && inferredTask && <>
            <p className="muted">Profile confidence: {Math.round(inferredTask.confidence * 100)}%.</p>
            <p><b>Suggested next step:</b> Review the training configuration when you are ready. Nothing will launch until you explicitly confirm it.</p>
            <div className="button-row"><Link className="button button--primary" to="training">Configure training<ArrowRight size={16} /></Link>
              <Link className="button button--secondary" to="data">Review profile</Link></div></>}
          {profile.data?.status === "failed" && <Notice tone="danger">{profile.data.failure_message || "Profiling failed. Review the target and try again."}</Notice>}
          {!profile.data && !profile.isLoading && <p className="muted">No profile has started yet. Selecting a target does not trigger any work by itself.</p>}
          {selectedDataset && version && <small className="muted">Using {selectedDataset.name}, version {version.version_number}.</small>}
        </>}
      </div></Card>
      <Card className="journey"><h2>Model journey</h2><p className="muted">A clear path from raw data to an operating model.</p>
        {[
          ["1", "Data", "Upload, target, and profile", profileSucceeded ? "complete" : "current"],
          ["2", "Train", "Compare candidates", succeeded ? "complete" : profileSucceeded ? "current" : "upcoming"],
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

function TargetVisualization({ task, profile, preview }: {
  task: "classification" | "regression" | "time_series";
  profile: TargetColumnProfile;
  preview: boolean;
}) {
  const distribution = profile.distribution.length
    ? profile.distribution
    : profile.preview_distribution || [];
  const previewValues = profile.preview_values || [];
  const title = task === "classification" ? "Class balance"
    : task === "regression" ? "Regression target distribution"
      : "Time-series target distribution";
  const description = task === "classification"
    ? "Class counts reveal imbalance before model training."
    : task === "regression"
      ? "The histogram shows the range, concentration, and skew of the continuous target."
      : "The histogram shows how the temporal target is distributed across its observed range.";

  const plotData = task !== "classification" && previewValues.length
    ? [{ type: "histogram" as const, x: previewValues, marker: { color: "#3159e8" }, hovertemplate: "%{x}<br>Count: %{y}<extra></extra>" }]
    : [{ type: "bar" as const, x: distribution.map((bucket) => bucket.label), y: distribution.map((bucket) => bucket.count), marker: { color: "#3159e8" }, hovertemplate: "%{x}<br>Count: %{y}<extra></extra>" }];

  return <section className="target-visualization" aria-labelledby="target-visualization-title">
    <div><span className="eyebrow">{preview ? "Instant target preview" : "Profiled target"}</span><h3 id="target-visualization-title">{title}</h3><p>{description}</p></div>
    {!previewValues.length && !distribution.length ? <Notice>A target distribution is not available.</Notice>
      : <Suspense fallback={<Loading label="Loading Plotly visualization…" />}><PlotlyChart className="plotly-target-chart" data={plotData} layout={{
        autosize: true,
        height: 300,
        margin: { l: 50, r: 15, t: 15, b: 70 },
        paper_bgcolor: "rgba(0,0,0,0)",
        plot_bgcolor: "#f8f9fc",
        bargap: task === "classification" ? .25 : .05,
        xaxis: { title: { text: task === "classification" ? profile.name : "Target range" }, automargin: true },
        yaxis: { title: { text: "Count" }, rangemode: "tozero", automargin: true },
        font: { family: "Inter, system-ui, sans-serif", color: "#4e5870", size: 11 },
        showlegend: false,
      }} config={{ displayModeBar: false, responsive: true }} useResizeHandler style={{ width: "100%" }} /></Suspense>}
    {task === "regression" && <div className="target-statistics">
      {[["Minimum", "min"], ["Median", "median"], ["Mean", "mean"], ["Maximum", "max"]].map(([label, key]) =>
        <div key={key}><span>{label}</span><strong>{formatTargetStatistic(profile.statistics[key])}</strong></div>)}
    </div>}
    {task === "time_series" && <div className="target-statistics">
      <div><span>Earliest</span><strong>{formatTargetStatistic(profile.statistics.min)}</strong></div>
      <div><span>Latest</span><strong>{formatTargetStatistic(profile.statistics.max)}</strong></div>
    </div>}
  </section>;
}

function inferPreviewTask(target: string | null, column?: DatasetColumnPreview): TaskType {
  if (!target) return "clustering";
  if (column?.semantic_type === "temporal") return "time_series";
  if (column?.semantic_type === "numerical_continuous") return "regression";
  return "classification";
}

function previewTaskRationale(task: TaskType, target: string | null) {
  if (!target) return "No target is selected, so the provisional task is unsupervised clustering.";
  if (task === "regression") return "The selected target is continuous numeric, indicating regression.";
  if (task === "time_series") return "The selected target is temporal, indicating time-series analysis.";
  return "The selected target is categorical, text-like, or low-cardinality numeric, indicating classification.";
}

function previewColumnProfile(column: DatasetColumnPreview): TargetColumnProfile {
  const fallbackValues = column.sample_values || [];
  const previewValues = column.preview_values?.length
    ? column.preview_values
    : column.semantic_type === "numerical_continuous"
      ? fallbackValues.map(Number).filter(Number.isFinite)
      : column.semantic_type === "temporal" ? fallbackValues : [];
  const fallbackCounts = fallbackValues.reduce<Record<string, number>>((counts, value) => {
    counts[value] = (counts[value] || 0) + 1;
    return counts;
  }, {});
  return {
    name: column.name,
    semantic_type: column.semantic_type || "unknown",
    statistics: column.statistics || {},
    distribution: [],
    preview_values: previewValues,
    preview_distribution: column.preview_distribution?.length
      ? column.preview_distribution
      : Object.entries(fallbackCounts).map(([label, count]) => ({ label, count })),
  };
}

function formatTargetStatistic(value: string | number | null | undefined) {
  if (value == null) return "—";
  return typeof value === "number" ? value.toLocaleString(undefined, { maximumFractionDigits: 3 }) : value;
}
