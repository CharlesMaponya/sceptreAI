import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Database, FileSpreadsheet, Plus, RefreshCw, Upload, X } from "lucide-react";
import { DragEvent, FormEvent, useEffect, useMemo, useState } from "react";
import { api, fileToBase64, json } from "../api";
import { Badge, Button, Card, EmptyState, ErrorState, Loading, Metric, Modal, Notice, PageHeader } from "../components/ui";
import { formatBytes, formatDate, titleCase } from "../lib";
import type { Dataset, DatasetVersion, ProfileJob } from "../types";
import { useParams } from "react-router-dom";

type ProfileResult = ProfileJob & {
  feature_profiles_json: Record<string, {
    name?: string; semantic_type?: string; distinct_count?: number; missing_count?: number;
    missing_ratio?: number; quality_flags?: string[];
  }>;
  preparation_json: Array<{ column: string; action?: string; reason?: string }>;
  relationships_json: Array<Record<string, unknown>>;
};

export function DataPage() {
  const { projectId = "" } = useParams();
  const client = useQueryClient();
  const [showUpload, setShowUpload] = useState(false);
  const [selected, setSelected] = useState<Dataset | null>(null);
  const [file, setFile] = useState<File | null>(null);
  const datasets = useQuery({ queryKey: ["datasets", projectId], queryFn: () => api<Dataset[]>(`/projects/${projectId}/datasets`) });
  useEffect(() => {
    if (!selected && datasets.data?.length) setSelected(datasets.data[0]);
    if (selected && datasets.data && !datasets.data.some((item) => item.id === selected.id)) setSelected(datasets.data[0] || null);
  }, [datasets.data, selected]);

  const upload = useMutation({
    mutationFn: async (values: { name: string; description: string; file: File }) =>
      api(`/projects/${projectId}/datasets/upload`, json("POST", {
        dataset_name: values.name, description: values.description, filename: values.file.name,
        content_base64: await fileToBase64(values.file), tags: {},
      })),
    onSuccess: () => { client.invalidateQueries({ queryKey: ["datasets", projectId] }); setShowUpload(false); setFile(null); },
  });
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
      action={<Button onClick={() => setShowUpload(true)}><Upload size={16} />Upload dataset</Button>} />
    {datasets.isLoading ? <Loading /> : datasets.error ? <ErrorState error={datasets.error} retry={() => datasets.refetch()} /> :
      !datasets.data?.length ? <Card><EmptyState icon={<Database />} title="Your model starts with trusted data" description="Upload a table and Sceptre will create an immutable version, inspect its schema, and begin profiling."
        action={<Button onClick={() => setShowUpload(true)}><Plus size={16} />Upload first dataset</Button>} /></Card> :
        <div className="data-layout"><Card className="dataset-list"><div className="dataset-list__head"><b>All datasets</b><span>{datasets.data.length}</span></div>
          {datasets.data.map((dataset) => <button className={selected?.id === dataset.id ? "active" : ""} key={dataset.id} onClick={() => setSelected(dataset)}>
            <i><FileSpreadsheet size={18} /></i><span><b>{dataset.name}</b><small>v{dataset.latest_version_number} · {formatDate(dataset.created_at)}</small></span></button>)}</Card>
          {selected && <DatasetDetail projectId={projectId} dataset={selected} />}</div>}
    {showUpload && <Modal title="Upload a dataset" description="A new immutable dataset version will be created and profiled automatically." onClose={() => setShowUpload(false)}>
      <form className="stack" onSubmit={submit}><label>Dataset name<input name="name" required maxLength={220} autoFocus placeholder="Q2 customer activity" /></label>
        <label>Description <span className="optional">Optional</span><textarea name="description" rows={2} placeholder="Source, purpose, or collection period" /></label>
        <label className="dropzone" onDragOver={(e) => e.preventDefault()} onDrop={(e: DragEvent) => { e.preventDefault(); acceptFile(e.dataTransfer.files[0]); }}>
          <input type="file" accept=".csv,.parquet,.xlsx,.xls,.json,.jsonl" onChange={(e) => acceptFile(e.target.files?.[0])} />
          {file ? <><FileSpreadsheet /><b>{file.name}</b><span>{formatBytes(file.size)}</span><button type="button" onClick={(e) => { e.preventDefault(); setFile(null); }}><X size={15} /> Remove</button></>
            : <><Upload /><b>Drop a file here or browse</b><span>CSV, Parquet, Excel, JSON, or JSONL</span></>}
        </label>
        {upload.error && <Notice tone="danger">{upload.error.message}</Notice>}
        <div className="modal__actions"><Button variant="ghost" type="button" onClick={() => setShowUpload(false)}>Cancel</Button><Button disabled={!file} loading={upload.isPending}>Upload and profile</Button></div>
      </form></Modal>}
  </>;
}

function DatasetDetail({ projectId, dataset }: { projectId: string; dataset: Dataset }) {
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
  const startProfile = useMutation({
    mutationFn: () => api<ProfileJob>(`${profilePath}/profile-jobs`, json("POST", { target_column: null, force: true })),
    onSuccess: () => client.invalidateQueries({ queryKey: ["profile", versionId] }),
  });
  const columns = useMemo(() => Object.values(result.data?.feature_profiles_json || {}), [result.data]);
  if (versions.isLoading) return <Card><Loading label="Loading versions…" /></Card>;
  return <div className="dataset-detail">
    <Card className="dataset-hero"><div><span className="eyebrow">Dataset</span><h2>{dataset.name}</h2><p>{dataset.description || "No description provided."}</p></div>
      <label>Version<select value={versionId} onChange={(e) => setVersionId(e.target.value)}>{versions.data?.map((item) => <option value={item.id} key={item.id}>Version {item.version_number} · {titleCase(item.status)}</option>)}</select></label></Card>
    {version && <div className="metrics-grid metrics-grid--compact"><Metric label="Rows" value={version.row_count?.toLocaleString() || "Pending"} /><Metric label="Columns" value={version.column_count || "Pending"} /><Metric label="Size" value={formatBytes(version.byte_size)} /><Metric label="Format" value={version.format.toUpperCase()} /></div>}
    <Card className="section-card">
      <div className="section-heading"><div><h2>Data profile</h2><p>Quality, structure, and model-readiness across the full dataset.</p></div>
        {profile.data?.status === "succeeded" ? <Badge status="succeeded" /> : <Button variant="secondary" loading={startProfile.isPending} onClick={() => startProfile.mutate()}><RefreshCw size={15} />{profile.data ? "Run again" : "Start profiling"}</Button>}</div>
      {profile.isLoading ? <Loading label="Checking profile…" /> : profile.data && ["queued", "running"].includes(profile.data.status) ?
        <div className="progress-panel"><div><b>{titleCase(profile.data.current_stage || "Preparing")}</b><span>{Math.round((profile.data.progress || 0) * 100)}%</span></div><progress value={profile.data.progress || 0} max={1} /><p>Profiling {profile.data.completed_columns || 0} of {profile.data.total_columns || 0} columns. You can leave this page safely.</p></div>
        : result.data ? <><div className="profile-summary"><div><span>Inferred task</span><strong>{titleCase(result.data.overview_json?.task_inference?.task_type || "Pending")}</strong></div>
          <div><span>Confidence</span><strong>{Math.round((result.data.overview_json?.task_inference?.confidence || 0) * 100)}%</strong></div><p>{result.data.overview_json?.task_inference?.rationale}</p></div>
          <div className="table-wrap"><table><thead><tr><th>Feature</th><th>Type</th><th>Distinct</th><th>Missing</th><th>Quality</th></tr></thead><tbody>{columns.slice(0, 50).map((column, index) =>
            <tr key={column.name || index}><td><b>{column.name || "Column"}</b></td><td>{titleCase(column.semantic_type || "unknown")}</td><td>{column.distinct_count?.toLocaleString() || "—"}</td><td>{column.missing_ratio == null ? "—" : `${(column.missing_ratio * 100).toFixed(1)}%`}</td><td>{column.quality_flags?.length ? column.quality_flags.map(titleCase).join(", ") : <span className="good">Looks good</span>}</td></tr>)}</tbody></table></div></>
          : <div className="inline-empty"><Database /><span><b>No completed profile</b><small>Profile this version before configuring a training run.</small></span></div>}
    </Card>
  </div>;
}
