import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Database, FileSpreadsheet, Play, Plus, RefreshCw, Upload, X } from "lucide-react";
import { lazy, Suspense, DragEvent, FormEvent, useEffect, useMemo, useState } from "react";
import { api, json, uploadFormData } from "../api";
import { Badge, Button, Card, EmptyState, ErrorState, Loading, Metric, Modal, Notice, PageHeader } from "../components/ui";
import { formatBytes, titleCase } from "../lib";
import type { Dataset, DatasetVersion, LeakageAnalysis, LeakageFinding, ProfileJob } from "../types";
import { useNavigate, useParams } from "react-router-dom";

type ProfileResult = ProfileJob & {
  feature_profiles_json: Record<string, {
    name?: string; semantic_type?: string; distinct_count?: number; missing_count?: number;
    missing_ratio?: number; quality_flags?: string[]; sample_values?: string[];
    statistics?: Record<string, unknown>; distribution_type?: string;
    distribution?: Array<{ label: string; count: number }>;
  }>;
  preparation_json: Array<{ column: string; action?: string; strategy?: string; reason?: string }>;
  relationships_json: Array<{ source_column: string; target_column: string; method: string; value: number }>;
};

type DatasetUploadResult = { dataset: Dataset; version: DatasetVersion };
const PlotlyChart = lazy(() => import("../components/PlotlyChart"));

export function DataPage() {
  const { projectId = "" } = useParams();
  const navigate = useNavigate();
  const client = useQueryClient();
  const [showUpload, setShowUpload] = useState(false);
  const [selected, setSelected] = useState<Dataset | null>(null);
  const [file, setFile] = useState<File | null>(null);
  const [uploadProgress, setUploadProgress] = useState(0);
  const datasets = useQuery({ queryKey: ["datasets", projectId], queryFn: () => api<Dataset[]>(`/projects/${projectId}/datasets`) });
  useEffect(() => {
    if (!selected && datasets.data?.length) setSelected(datasets.data[0]);
    if (selected && datasets.data && !datasets.data.some((item) => item.id === selected.id)) setSelected(datasets.data[0] || null);
  }, [datasets.data, selected]);

  const upload = useMutation({
    mutationFn: async (values: { name: string; description: string; file: File }) => {
      const body = new FormData();
      body.set("dataset_name", values.name);
      body.set("description", values.description);
      body.set("tags", JSON.stringify({}));
      body.set("file", values.file, values.file.name);
      return uploadFormData<DatasetUploadResult>(
        `/projects/${projectId}/datasets/upload`,
        body,
        setUploadProgress,
      );
    },
    onSuccess: (result) => {
      client.invalidateQueries({ queryKey: ["datasets", projectId] });
      client.invalidateQueries({ queryKey: ["versions", projectId, result.dataset.id] });
      setFile(null);
      setShowUpload(false);
      navigate(`/projects/${projectId}`);
    },
  });
  const openUpload = () => {
    upload.reset();
    setUploadProgress(0);
    setShowUpload(true);
  };
  function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault(); if (!file) return;
    const form = new FormData(event.currentTarget);
    upload.mutate({ name: String(form.get("name")), description: String(form.get("description") || ""), file });
  }
  const acceptFile = (next?: File) => {
    if (!next) return;
    const ext = next.name.split(".").pop()?.toLowerCase();
    if (ext && ["csv", "parquet", "xlsx", "xls", "json", "jsonl"].includes(ext)) setFile(next);
  };

  return <>
    <PageHeader eyebrow="Data workspace" title="Datasets" description="Version, profile, and inspect the data behind every model."
      action={<Button onClick={openUpload}><Upload size={16} />Upload dataset</Button>} />
    {datasets.isLoading ? <Loading /> : datasets.error ? <ErrorState error={datasets.error} retry={() => datasets.refetch()} /> :
      !datasets.data?.length ? <Card><EmptyState icon={<Database />} title="Your model starts with trusted data" description="Upload a table and Sceptre will create an immutable version, inspect its schema, and begin profiling."
        action={<Button onClick={openUpload}><Plus size={16} />Upload first dataset</Button>} /></Card> :
        selected && <DatasetDetail projectId={projectId} dataset={selected} datasets={datasets.data} onDatasetChange={setSelected} />}
    {showUpload && <Modal
      title="Upload a dataset"
      description="A new immutable dataset version will be created. You will choose its target from the project overview."
      onClose={() => setShowUpload(false)}>
      <form className="stack" onSubmit={submit}><label>Dataset name<input name="name" required maxLength={220} autoFocus placeholder="Q2 customer activity" /></label>
        <label>Description <span className="optional">Optional</span><textarea name="description" rows={2} placeholder="Source, purpose, or collection period" /></label>
        <label className="dropzone" onDragOver={(e) => e.preventDefault()} onDrop={(e: DragEvent) => { e.preventDefault(); acceptFile(e.dataTransfer.files[0]); }}>
          <input type="file" accept=".csv,.parquet,.xlsx,.xls,.json,.jsonl" onChange={(e) => acceptFile(e.target.files?.[0])} />
          {file ? <><FileSpreadsheet /><b>{file.name}</b><span>{formatBytes(file.size)}</span><button type="button" onClick={(e) => { e.preventDefault(); setFile(null); }}><X size={15} /> Remove</button></>
            : <><Upload /><b>Drop a file here or browse</b><span>CSV, Parquet, Excel, JSON, or JSONL</span></>}
        </label>
        {upload.isPending && <div className="progress-panel" role="status" aria-live="polite">
          <div><b>{uploadProgress < 100 ? "Uploading dataset" : "Upload complete"}</b><span>{uploadProgress}%</span></div>
          <progress aria-label="Dataset upload progress" value={uploadProgress} max={100} />
          <p>{uploadProgress < 100 ? "Keep this window open while the file is transferred." : "Inspecting the dataset and preparing its profile…"}</p>
        </div>}
        {upload.error && <Notice tone="danger">{upload.error.message}</Notice>}
        <div className="modal__actions"><Button variant="ghost" type="button" onClick={() => setShowUpload(false)}>Cancel</Button><Button disabled={!file} loading={upload.isPending}>Upload dataset</Button></div>
      </form>
    </Modal>}
  </>;
}

function DatasetDetail({
  projectId,
  dataset,
  datasets,
  onDatasetChange,
}: {
  projectId: string;
  dataset: Dataset;
  datasets: Dataset[];
  onDatasetChange: (dataset: Dataset) => void;
}) {
  const navigate = useNavigate();
  const client = useQueryClient();
  const versions = useQuery({ queryKey: ["versions", projectId, dataset.id], queryFn: () => api<DatasetVersion[]>(`/projects/${projectId}/datasets/${dataset.id}/versions`) });
  const [versionId, setVersionId] = useState("");
  useEffect(() => { if (versions.data?.length && !versions.data.some((v) => v.id === versionId)) setVersionId(versions.data[0].id); }, [versions.data, versionId]);
  const version = versions.data?.find((item) => item.id === versionId);
  const profilePath = version ? `/projects/${projectId}/datasets/${dataset.id}/versions/${version.id}` : "";
  const profile = useQuery({
    queryKey: ["profile", versionId], enabled: Boolean(version),
    queryFn: () => api<ProfileJob | null>(`${profilePath}/profile-jobs/latest`),
    refetchInterval: (query) => query.state.data && ["queued", "running"].includes(query.state.data.status) ? 2000 : false,
  });
  const result = useQuery({
    queryKey: ["profile-result", profile.data?.id], enabled: profile.data?.status === "succeeded",
    queryFn: () => api<ProfileResult>(`${profilePath}/profile-jobs/${profile.data!.id}/result`),
  });
  const retryProfile = useMutation({
    mutationFn: () => api<ProfileJob>(`${profilePath}/profile-jobs`, json("POST", {
      target_column: profile.data?.target_column ?? null,
      force: true,
    })),
    onSuccess: () => client.invalidateQueries({ queryKey: ["profile", versionId] }),
  });
  const columns = useMemo(() => Object.values(result.data?.feature_profiles_json || {}), [result.data]);
  if (versions.isLoading) return <Card><Loading label="Loading versions…" /></Card>;
  if (!versionId && versions.data?.length) return <Card><Loading label="Selecting latest version…" /></Card>;
  if (!versions.data?.length) return <Card><EmptyState title="No dataset versions"
    description="Upload a new version before profiling this dataset." /></Card>;
  return <div className="dataset-detail">
    <Card className="dataset-hero"><div><span className="eyebrow">Dataset</span><h2>{dataset.name}</h2><p>{dataset.description || "No description provided."}</p></div>
      <div className="dataset-hero__controls"><label>Dataset<select value={dataset.id} onChange={(e) => { const next = datasets.find((item) => item.id === e.target.value); if (next) onDatasetChange(next); }}>
        {datasets.map((item) => <option value={item.id} key={item.id}>{item.name}</option>)}
      </select></label><label>Version<select value={versionId} onChange={(e) => setVersionId(e.target.value)}>{versions.data?.map((item) => <option value={item.id} key={item.id}>Version {item.version_number} · {titleCase(item.status)}</option>)}</select></label></div></Card>
    {version && <div className="metrics-grid metrics-grid--compact"><Metric label="Rows" value={version.row_count?.toLocaleString() || "Pending"} /><Metric label="Columns" value={version.column_count || "Pending"} /><Metric label="Size" value={formatBytes(version.byte_size)} /><Metric label="Format" value={version.format.toUpperCase()} /></div>}
    <Card className="section-card">
      <div className="section-heading"><div><h2>Data profile</h2><p>Quality, structure, and model-readiness across the full dataset.</p></div>
        {profile.data?.status === "succeeded" ? <Badge status="succeeded" />
          : profile.data && ["failed", "cancelled"].includes(profile.data.status)
            ? <Button variant="secondary" loading={retryProfile.isPending} onClick={() => retryProfile.mutate()}><RefreshCw size={15} />Retry profile</Button>
            : !profile.data && !profile.isLoading
              ? <Button variant="secondary" onClick={() => navigate(`/projects/${projectId}`)}>Choose target & profile</Button>
              : null}</div>
      {profile.isLoading || result.isLoading ? <Loading label="Checking profile…" /> : profile.data && ["queued", "running"].includes(profile.data.status) ?
        <div className="progress-panel"><div><b>{titleCase(profile.data.current_stage || "Preparing")}</b><span>{Math.round((profile.data.progress || 0) * 100)}%</span></div><progress value={profile.data.progress || 0} max={1} /><p>Profiling {profile.data.completed_columns || 0} of {profile.data.total_columns || 0} columns. You can leave this page safely.</p></div>
        : result.data ? <><div className="profile-summary"><div><span>Inferred task</span><strong>{titleCase(result.data.overview_json?.task_inference?.task_type || "Pending")}</strong></div>
          <div><span>Confidence</span><strong>{Math.round((result.data.overview_json?.task_inference?.confidence || 0) * 100)}%</strong></div><p>{result.data.overview_json?.task_inference?.rationale}</p></div>
          <LeakageSummary analysis={result.data.overview_json?.leakage_analysis} />
          <FeatureAccordions columns={columns} target={result.data.target_column} relationships={result.data.relationships_json} preparation={result.data.preparation_json} leakageFindings={result.data.overview_json?.leakage_analysis?.findings || []} />
          <div className="profile-training-action"><Button className="full" onClick={() => navigate(`/projects/${projectId}/training`)}>
            <Play size={15} />Start training</Button></div></>
          : <div className="inline-empty"><Database /><span><b>No completed profile</b><small>Choose the target from the project overview before training.</small></span></div>}
    </Card>
  </div>;
}

function LeakageSummary({ analysis }: { analysis?: LeakageAnalysis }) {
  if (!analysis || analysis.status === "not_applicable") return null;
  if (analysis.excluded_columns.length) return <Notice tone="danger"><span><b>Target leakage removed before training</b><br />{analysis.excluded_columns.join(", ")} {analysis.excluded_columns.length === 1 ? "is" : "are"} a high-confidence target proxy.</span></Notice>;
  return <Notice tone="success">No high-confidence target leakage was detected in {analysis.analyzed_rows.toLocaleString()} profiled rows.</Notice>;
}

type FeatureColumn = NonNullable<ProfileResult["feature_profiles_json"]>[string];

function FeatureAccordions({
  columns,
  target,
  relationships,
  preparation,
  leakageFindings,
}: {
  columns: FeatureColumn[];
  target: string | null | undefined;
  relationships: ProfileResult["relationships_json"];
  preparation: ProfileResult["preparation_json"];
  leakageFindings: LeakageFinding[];
}) {
  const [expanded, setExpanded] = useState<string | null>(null);
  const entries = columns.slice(0, 50);
  return <div className="feature-accordions">
    {entries.map((column, index) => {
      const name = column.name || `Feature ${index + 1}`;
      const open = expanded === name;
      const relationship = relationships.find((item) => item.source_column === name);
      const steps = preparation.filter((step) => step.column === name);
      const leakage = leakageFindings.find((finding) => finding.column === name);
      return <article className={`feature-accordion${open ? " feature-accordion--open" : ""}${leakage?.auto_excluded ? " feature-accordion--excluded" : ""}`} key={name}>
        <button type="button" className="feature-accordion__trigger" aria-expanded={open} onClick={() => setExpanded(open ? null : name)}>
          <span><b>{name}</b><small>{titleCase(column.semantic_type || "unknown")}{name === target ? " · Target" : ""}{leakage?.auto_excluded ? " · Excluded for leakage" : ""}</small></span>
          <i>{open ? "−" : "+"}</i>
        </button>
        {open && <div className="feature-accordion__body">
          <div className="feature-metrics"><Metric label="Distinct" value={column.distinct_count?.toLocaleString() || "—"} /><Metric label="Missing" value={column.missing_count?.toLocaleString() || "0"} /><Metric label="Missing rate" value={column.missing_ratio == null ? "—" : `${(column.missing_ratio * 100).toFixed(1)}%`} /></div>
          <div className="feature-detail-grid"><div><h3>{column.semantic_type === "text" ? "Word cloud" : column.semantic_type === "categorical" ? "Category distribution" : "Histogram"}</h3><FeatureDistribution column={column} /></div><div><h3>Five-number summary & statistics</h3><StatisticsTable statistics={column.statistics || {}} /></div></div>
          {relationship && <p className="feature-association"><b>{relationship.method === "cramers_v" ? "Cramér's V" : titleCase(relationship.method)}</b> with target: {relationship.value.toFixed(4)}</p>}
          {leakage && <Notice tone={leakage.auto_excluded ? "danger" : "info"}>{leakage.reason} Confidence: {Math.round(leakage.confidence * 100)}%.</Notice>}
          <div className="feature-preprocessing"><h3>Preprocessing mechanism</h3>{steps.length ? <ul>{steps.map((step, index) => <li key={`${step.action}-${index}`}><b>{titleCase(step.action || "Step")}</b> — {titleCase(step.strategy || "Recommended")}<small>{step.reason}</small></li>)}</ul> : <p>{name === target ? "Selected target; feature preprocessing is not applied." : "No preprocessing step is currently required."}</p>}</div>
        </div>}
      </article>;
    })}
  </div>;
}

function FeatureDistribution({ column }: { column: FeatureColumn }) {
  const isText = column.semantic_type === "text";
  const words = Array.isArray(column.statistics?.word_frequencies) ? column.statistics!.word_frequencies as Array<{ word: string; count: number }> : [];
  const distribution = column.distribution || [];
  if (isText && !words.length) return <Notice>Word frequencies are unavailable for this profile. Run profiling again to generate the word cloud.</Notice>;
  if (!isText && !distribution.length) return <Notice>A distribution is not available.</Notice>;
  const chart = isText
    ? [buildWordCloudTrace(words)]
    : [{ type: "bar" as const, x: distribution.map((item) => item.label), y: distribution.map((item) => item.count), marker: { color: "#3159e8" }, hovertemplate: "%{x}<br>Count: %{y}<extra></extra>" }];
  const cloudAxis = { visible: false, fixedrange: true, range: [-340, 340] };
  return <Suspense fallback={<Loading label="Loading visualization…" />}><PlotlyChart className={`feature-plot${isText ? " feature-word-cloud" : ""}`} data={chart} layout={{ autosize: true, height: 260, margin: isText ? { l: 8, r: 8, t: 8, b: 8 } : { l: 45, r: 10, t: 10, b: 65 }, paper_bgcolor: "rgba(0,0,0,0)", plot_bgcolor: isText ? "rgba(0,0,0,0)" : "#f8f9fc", showlegend: false, hovermode: "closest", xaxis: isText ? cloudAxis : { visible: true, automargin: true }, yaxis: isText ? { ...cloudAxis, range: [-120, 120] } : { visible: true, automargin: true }, font: { family: "Inter, system-ui, sans-serif", size: 10, color: "#4e5870" } }} config={{ displayModeBar: false, responsive: true }} useResizeHandler style={{ width: "100%" }} /></Suspense>;
}

type CloudWord = { word: string; count: number };
type PlacedCloudWord = CloudWord & { x: number; y: number; size: number; color: string };

function buildWordCloudTrace(words: CloudWord[]) {
  const palette = ["#173b82", "#3159e8", "#5f78d8", "#176b78", "#7048a8", "#2360a8"];
  const candidates = words
    .filter((item) => item.word?.trim() && Number.isFinite(item.count) && item.count > 0)
    .sort((left, right) => right.count - left.count)
    .slice(0, 40);
  const logarithms = candidates.map((item) => Math.log1p(item.count));
  const minimum = Math.min(...logarithms);
  const maximum = Math.max(...logarithms);
  const boxes: Array<{ left: number; right: number; top: number; bottom: number }> = [];
  const placed: PlacedCloudWord[] = [];

  candidates.forEach((item, index) => {
    const scale = maximum === minimum ? 0.5 : (Math.log1p(item.count) - minimum) / (maximum - minimum);
    const requestedSize = 14 + scale * 34;
    const size = Math.max(13, Math.min(requestedSize, 570 / Math.max(1, item.word.length * 0.56)));
    const width = Math.max(size * 1.4, item.word.length * size * 0.56);
    const height = size * 1.12;
    const seed = wordHash(item.word);

    for (let attempt = 0; attempt < 700; attempt += 1) {
      const angle = attempt * 0.53 + (seed % 360) * (Math.PI / 180);
      const radius = attempt === 0 ? 0 : 7 * Math.sqrt(attempt);
      const x = Math.cos(angle) * radius * 1.65;
      const y = Math.sin(angle) * radius * 0.62;
      const box = {
        left: x - width / 2 - 3,
        right: x + width / 2 + 3,
        top: y + height / 2 + 2,
        bottom: y - height / 2 - 2,
      };
      const inBounds = box.left >= -330 && box.right <= 330 && box.bottom >= -112 && box.top <= 112;
      const overlaps = boxes.some((existing) => !(box.right < existing.left || box.left > existing.right || box.top < existing.bottom || box.bottom > existing.top));
      if (!inBounds || overlaps) continue;
      boxes.push(box);
      placed.push({ ...item, x, y, size, color: palette[(index + seed) % palette.length] });
      break;
    }
  });

  return {
    type: "scatter" as const,
    mode: "text" as const,
    x: placed.map((item) => item.x),
    y: placed.map((item) => item.y),
    text: placed.map((item) => item.word),
    customdata: placed.map((item) => item.count),
    textfont: {
      family: "Manrope Variable, Inter, system-ui, sans-serif",
      size: placed.map((item) => item.size),
      color: placed.map((item) => item.color),
    },
    hovertemplate: "<b>%{text}</b><br>Frequency: %{customdata:,}<extra></extra>",
    cliponaxis: false,
  };
}

function wordHash(value: string) {
  let hash = 0;
  for (let index = 0; index < value.length; index += 1) hash = ((hash << 5) - hash + value.charCodeAt(index)) | 0;
  return Math.abs(hash);
}

function StatisticsTable({ statistics }: { statistics: Record<string, unknown> }) {
  const preferred = ["count", "min", "q1", "median", "q3", "max", "mean", "stddev", "variance", "skewness", "kurtosis", "avg_length", "max_length"];
  const rows = preferred.filter((key) => key in statistics);
  const topValues = Array.isArray(statistics.top_values) ? statistics.top_values as Array<{ value?: unknown; count?: unknown }> : [];
  return rows.length || topValues.length ? <>
    {rows.length ? <dl className="statistics-table">{rows.map((key) => <div key={key}><dt>{titleCase(key)}</dt><dd>{formatStatistic(statistics[key])}</dd></div>)}</dl> : null}
    {topValues.length ? <div className="statistics-table statistics-table--top-values">{topValues.slice(0, 10).map((item, index) => <div key={`${String(item.value)}-${index}`}><span>{formatStatistic(item.value)}</span><b>{formatStatistic(item.count)}</b></div>)}</div> : null}
  </> : <p className="muted">No descriptive statistics available.</p>;
}

function formatStatistic(value: unknown) {
  if (value == null) return "—";
  if (typeof value === "number") return value.toLocaleString(undefined, { maximumFractionDigits: 4 });
  return String(value);
}
