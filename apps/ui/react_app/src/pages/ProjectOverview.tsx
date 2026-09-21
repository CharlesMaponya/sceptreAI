import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { ArrowRight, Boxes, CheckCircle2, Clock3, Database, Rocket, Sparkles, Upload } from "lucide-react";
import { lazy, memo, Suspense, useEffect, useMemo, useState } from "react";
import { Link, useParams } from "react-router";
import { api, json } from "../api";
import { Badge, Button, Card, ErrorState, Loading, Metric, Notice, PageHeader } from "../components/ui";
import { formatDate, titleCase } from "../lib";
import type { Dataset, DatasetVersion, ModelRun, ProfileJob, Project } from "../types";
import {
  formatTargetStatistic, inferPreviewTask, previewColumnProfile, previewTaskRationale,
} from "./presentation";
import type { TargetColumnProfile } from "./presentation";
import { ProjectJourney } from "./ProjectJourney";

const NO_TARGET = "No target";
const TERMINAL_PROFILE_STATUSES = ["succeeded", "failed", "cancelled"];
const MAX_CONTROL_PLANE_PROFILE_BYTES = 100 * 1024 * 1024;
const PlotlyChart = lazy(() => import("../components/PlotlyChart"));

type CompletedProfile = ProfileJob & {
  feature_profiles_json: Record<string, TargetColumnProfile>;
};

export function ProjectOverview() {
  const { projectId = "" } = useParams();
  const client = useQueryClient();
  const [datasetId, setDatasetId] = useState("");
  const [target, setTarget] = useState(NO_TARGET);
  const [timeColumn, setTimeColumn] = useState("");
  const project = useQuery({ queryKey: ["project", projectId], queryFn: () => api<Project>(`/projects/${projectId}`), refetchInterval: 5000 });
  const datasets = useQuery({ queryKey: ["datasets", projectId], queryFn: () => api<Dataset[]>(`/projects/${projectId}/datasets`) });
  const runs = useQuery({ queryKey: ["runs", projectId], queryFn: () => api<ModelRun[]>(`/projects/${projectId}/training/runs`), refetchInterval: 10_000 });
  useEffect(() => {
    if (datasets.data?.length && !datasets.data.some((dataset) => dataset.id === datasetId)) {
      const trainingSource = datasets.data.find((dataset) =>
        !["external_validation", "drift", "offline_scoring"].includes(String(dataset.tags?.purpose)));
      setDatasetId((trainingSource || datasets.data[0]).id);
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
    setTimeColumn(profile.data?.overview_json?.time_column || "");
  }, [profile.data?.id, profile.data?.target_column, profile.data?.overview_json?.time_column, version?.id]);
  const normalizedTarget = target === NO_TARGET ? null : target;
  const selectedColumn = columns.length
    ? (version?.schema_json || version?.dataset_schema)?.columns?.find(
      (column) => column.name === normalizedTarget,
    )
    : undefined;
  const hasStoredPreview = Boolean(
    selectedColumn?.preview_values?.length
    || selectedColumn?.preview_distribution?.length
    || selectedColumn?.sample_values?.length,
  );
  const sampledTargetProfile = useQuery({
    queryKey: ["target-preview", version?.id, normalizedTarget],
    enabled: Boolean(version && normalizedTarget && !hasStoredPreview
      && (profile.data?.status !== "succeeded" || normalizedTarget !== profile.data.target_column)),
    queryFn: () => api<TargetColumnProfile>(
      `${profilePath}/target-preview?column=${encodeURIComponent(normalizedTarget!)}`,
    ),
  });
  const targetChanged = Boolean(profile.data)
    && (normalizedTarget !== (profile.data?.target_column || null)
      || timeColumn !== (profile.data?.overview_json?.time_column || ""));
  const profileActive = Boolean(profile.data)
    && !TERMINAL_PROFILE_STATUSES.includes(profile.data!.status);
  const startProfile = useMutation({
    mutationFn: () => api<ProfileJob>(`${profilePath}/profile-jobs`, json("POST", {
      target_column: normalizedTarget,
      time_column: timeColumn || null,
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
    queryKey: ["completed-profile-result", profile.data?.id],
    enabled: Boolean(
      profileSucceeded
      && profile.data?.id
      && profile.data?.target_column
      && inferredTask?.task_type !== "clustering",
    ),
    queryFn: () => api<CompletedProfile>(`${profilePath}/profile-jobs/${profile.data!.id}/result`),
    staleTime: Infinity,
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
  const targetProfile = profile.data?.target_column
    ? targetProfileResult.data?.feature_profiles_json?.[profile.data.target_column]
    : undefined;
  const profileMatchesSelection = profileSucceeded && !targetChanged;
  const requiresDistributedProfile = (version?.byte_size || 0) > MAX_CONTROL_PLANE_PROFILE_BYTES;
  const provisionalTask = inferPreviewTask(
    normalizedTarget,
    sampledTargetProfile.data || selectedColumn,
  );
  const displayedTask = profileMatchesSelection && inferredTask
    ? inferredTask.task_type
    : provisionalTask;
  const previewProfile = sampledTargetProfile.data
    || (selectedColumn ? previewColumnProfile(selectedColumn) : undefined);
  const displayedTargetProfile = profileMatchesSelection ? targetProfile : previewProfile;
  const canStartProfile = Boolean(version)
    && !versions.isLoading && !profile.isLoading
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
    <section className="overview-hero" aria-label="Project summary">
    <img className="overview-hero__globe" src="/data-globe.svg" alt="" aria-hidden />
    <PageHeader eyebrow="Project overview" title={project.data?.name || "Project"} description={project.data?.description || "Your governed model workspace."} />
    <div className="metrics-grid"><Metric label="Datasets" value={datasets.data?.length || 0} hint="immutable sources" icon={<Database size={21} />} />
      <Metric label="Training runs" value={runs.data?.length || 0} hint={`${active} currently active`} icon={<Boxes size={21} />} />
      <Metric label="Successful runs" value={succeeded} hint="ready to review" icon={<CheckCircle2 size={21} />} />
      <Metric label="Last activity" value={formatDate(runs.data?.[0]?.created_at || project.data?.updated_at)} icon={<Clock3 size={21} />} /></div>
    </section>
    <div className="overview-grid">
      <Card className="next-card"><div className="next-card__icon"><GuidanceIcon /></div><div className="overview-guidance"><span className="eyebrow">Project guidance</span><h2>{guidanceTitle}</h2>
        {!datasets.data?.length ? <><p>Upload CSV, Parquet, Excel, JSON, or JSONL. You will choose a target before profiling begins.</p>
          <Link className="button button--primary" to="data">Upload data<ArrowRight size={16} /></Link></> : <>
          <p>Select the latest dataset you want to work with and choose its target column. Profiling starts only when you confirm.</p>
          <div className="overview-guidance__controls">
            <label>Dataset<select value={datasetId} onChange={(event) => setDatasetId(event.target.value)}>
              {datasets.data?.map((dataset) => <option value={dataset.id} key={dataset.id}>{dataset.name}</option>)}
            </select></label>
            <label>Target column<select value={target} onChange={(event) => setTarget(event.target.value)} disabled={!version || profileActive || profile.isLoading}>
              <option value={NO_TARGET}>{NO_TARGET}</option>
              {columns.map((column) => <option value={column} key={column}>{column}</option>)}
            </select></label>
            {canStartProfile && <Button loading={startProfile.isPending} onClick={() => startProfile.mutate()}>{actionLabel}</Button>}
          </div>
          <details><summary>Time-series preparation</summary>
            <label>Time column<select value={timeColumn} onChange={(event) => setTimeColumn(event.target.value)} disabled={!version || profileActive}>
              <option value="">No chronological split</option>
              {columns.filter((column) => column !== normalizedTarget).map((column) => <option value={column} key={column}>{column}</option>)}
            </select><small>Select the observation time when predicting future values. Validation and final testing use later observations.</small></label>
          </details>
          {versions.isLoading || profile.isLoading ? <Loading label="Checking the latest dataset profile…" /> : null}
          {!version && !versions.isLoading && <Notice tone="danger">The selected dataset has no available version.</Notice>}
          {startProfile.error && <Notice tone="danger">{startProfile.error.message}</Notice>}
          {sampledTargetProfile.isLoading && <Loading label="Sampling the selected target…" />}
          {sampledTargetProfile.error && <Notice tone="danger">{sampledTargetProfile.error.message}</Notice>}
          {requiresDistributedProfile && <Notice>
            Full profiling for this dataset runs as isolated splitter and preparation jobs on KubeRay. The instant target preview remains available while those jobs run.
          </Notice>}
          {version && <div className="profile-summary"><div><span>{profileMatchesSelection ? "Inferred task" : "Provisional task"}</span><strong>{titleCase(displayedTask)}</strong></div>
            <div><span>Target</span><strong>{normalizedTarget || "No target"}</strong></div><p>{profileMatchesSelection && inferredTask
              ? inferredTask.rationale
              : previewTaskRationale(displayedTask, normalizedTarget)}</p></div>}
          {profileMatchesSelection && targetProfileResult.isLoading && <Loading label="Loading the completed target profile…" />}
          {profileMatchesSelection && targetProfileResult.error && <ErrorState error={targetProfileResult.error} retry={() => targetProfileResult.refetch()} />}
          {displayedTask !== "clustering" && normalizedTarget && displayedTargetProfile
            && <TargetVisualization task={displayedTask} profile={displayedTargetProfile} preview={!profileMatchesSelection}
              scope={profile.data?.overview_json?.execution_mode === "kuberay" ? "Training split" : "Dataset profile"}
              rowCount={targetProfileResult.data?.row_count ?? profile.data?.row_count} versionNumber={version?.version_number} />}
          {profileMatchesSelection && !targetProfileResult.isLoading && !targetProfileResult.error && !targetProfile && <Notice>The completed profile has no target distribution. Reprofile this dataset to calculate it.</Notice>}
          {profileActive && <div className="progress-panel" role="status"><div><b>{titleCase(profile.data?.current_stage || "Preparing")}</b><span>{Math.round((profile.data?.progress || 0) * 100)}%</span></div>
            <progress value={profile.data?.progress || 0} max={1} /><p>{profile.data?.overview_json?.execution_mode === "kuberay"
              ? `KubeRay generation ${profile.data.overview_json.workflow_generation || 1} is running. You can safely leave this page.`
              : "Profiling is running in the background. You can continue using the project."}</p></div>}
          {profileMatchesSelection && inferredTask && <>
            <p className="muted">Profile confidence: {Math.round(inferredTask.confidence * 100)}%.</p>
            {targetProfileResult.data?.overview_json?.leakage_analysis?.excluded_columns.length ? <Notice tone="danger">Training will exclude target-leakage features: {targetProfileResult.data.overview_json.leakage_analysis.excluded_columns.join(", ")}.</Notice> : <Notice tone="success">No high-confidence target leakage was detected.</Notice>}
            <p><b>Suggested next step:</b> Review the training configuration when you are ready. Nothing will launch until you explicitly confirm it.</p>
            <div className="button-row"><Link className="button button--primary" to="training">Configure training<ArrowRight size={16} /></Link>
              <Link className="button button--secondary" to="data">Review profile</Link></div></>}
          {profile.data?.status === "failed"
            ? <Notice tone="danger">{profile.data.failure_message || "Profiling failed after its retry budget was exhausted. The verified upload is still available."}</Notice>
            : null}
          {!profile.data && !profile.isLoading && <p className="muted">No profile has started yet. Selecting a target does not trigger any work by itself.</p>}
          {selectedDataset && version && <small className="muted">Using {selectedDataset.name}, version {version.version_number}.</small>}
        </>}
      </div></Card>
      <ProjectJourney projectId={projectId} />
    </div>
    <Card className="section-card"><div className="section-heading"><div><h2>Recent training</h2><p>Latest activity across this project.</p></div><Link to="runs">View all <ArrowRight size={15} /></Link></div>
      {recent.length ? <div className="table-wrap"><table><thead><tr><th>Run</th><th>Task</th><th>Status</th><th>Created</th></tr></thead><tbody>{recent.map((run) =>
        <tr key={run.id}><td><b>{run.run_name || run.id.slice(0, 8)}</b></td><td>{titleCase(run.task_type)}</td><td><Badge status={run.status} /></td><td>{formatDate(run.created_at)}</td></tr>)}</tbody></table></div>
        : <div className="inline-empty"><Rocket /><span><b>No training runs yet</b><small>Your completed experiments will appear here.</small></span></div>}</Card>
  </>;
}

const TargetVisualization = memo(function TargetVisualization({ task, profile, preview, scope, rowCount, versionNumber }: {
  task: "classification" | "regression" | "time_series";
  profile: TargetColumnProfile;
  preview: boolean;
  scope: string;
  rowCount?: number;
  versionNumber?: number;
}) {
  const distribution = profile.distribution.length
    ? profile.distribution
    : profile.preview_distribution || [];
  const previewValues = profile.preview_values || [];
  const title = task === "classification" ? "Class balance"
    : task === "regression" ? "Regression target distribution"
      : "Time-series target distribution";
  const description = task === "classification"
    ? preview
      ? "This upload sample is a preliminary view, not the completed training distribution."
      : "Counts show non-missing target values in this completed profile. Starting training does not change this chart."
    : task === "regression"
      ? "The histogram shows the range, concentration, and skew of the continuous target."
      : "The histogram shows how the temporal target is distributed across its observed range.";

  const plotData = task !== "classification" && previewValues.length
    ? [{ type: "histogram" as const, x: previewValues, marker: { color: "#cb0c9f" }, hovertemplate: "%{x}<br>Count: %{y}<extra></extra>" }]
    : [{ type: "bar" as const, x: distribution.map((bucket) => bucket.label), y: distribution.map((bucket) => bucket.count), marker: { color: "#cb0c9f" }, hovertemplate: "%{x}<br>Count: %{y}<extra></extra>" }];

  return <section className="target-visualization" aria-labelledby="target-visualization-title">
    <div><span className="eyebrow">{preview ? "Instant target preview" : `${scope} · completed profile`}</span><h3 id="target-visualization-title">{title}</h3><p>{description}</p></div>
    {!preview && <p className="muted">Dataset version {versionNumber} · {rowCount?.toLocaleString() ?? "Unknown"} rows in this profile. {scope === "Training split" ? "Validation and final-test rows are excluded." : ""}</p>}
    {profile.missing_count != null && profile.missing_count > 0 && <Notice>{rowCount != null ? `${(rowCount - profile.missing_count).toLocaleString()} rows have a target value. ` : ""}{profile.missing_count.toLocaleString()} rows have no target value and are not represented in the chart. Training excludes rows with missing targets.</Notice>}
    {!preview && profile.statistics.approximate_top_values && <p className="muted">Category counts use bounded summaries and may be approximate. Up to 15 categories are displayed.</p>}
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
        font: { family: "Open Sans Variable, system-ui, sans-serif", color: "#4e5870", size: 11 },
        showlegend: false,
      }} config={{ displayModeBar: false, responsive: true }} useResizeHandler style={{ width: "100%" }} /></Suspense>}
    {preview && profile.sampled_rows ? <p className="muted">Preview based on {profile.sampled_rows.toLocaleString()} rows.</p> : null}
    {task === "regression" && <div className="target-statistics">
      {[["Minimum", "min"], ["Median", "median"], ["Mean", "mean"], ["Maximum", "max"]].map(([label, key]) =>
        <div key={key}><span>{label}</span><strong>{formatTargetStatistic(profile.statistics[key])}</strong></div>)}
    </div>}
    {task === "time_series" && <div className="target-statistics">
      <div><span>Earliest</span><strong>{formatTargetStatistic(profile.statistics.min)}</strong></div>
      <div><span>Latest</span><strong>{formatTargetStatistic(profile.statistics.max)}</strong></div>
    </div>}
  </section>;
});
