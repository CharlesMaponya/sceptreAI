import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { ArrowRight, Check, Cpu, Database, Info, Sparkles, Zap } from "lucide-react";
import { useEffect, useMemo, useState } from "react";
import { useNavigate, useParams } from "react-router-dom";
import { api, json } from "../api";
import { Button, Card, ErrorState, Loading, Metric, Notice, PageHeader } from "../components/ui";
import { titleCase } from "../lib";
import type { Dataset, DatasetVersion, Estimator, TaskType, TrainingEstimate, TrainingPayload } from "../types";

export function TrainingPage() {
  const { projectId = "" } = useParams();
  const navigate = useNavigate();
  const client = useQueryClient();
  const [datasetId, setDatasetId] = useState("");
  const [versionId, setVersionId] = useState("");
  const [task, setTask] = useState<TaskType>("classification");
  const [target, setTarget] = useState("");
  const [models, setModels] = useState<string[]>([]);
  const [minutes, setMinutes] = useState(10);
  const [folds, setFolds] = useState(3);
  const [iterations, setIterations] = useState(5);
  const [gpu, setGpu] = useState(false);
  const [runName, setRunName] = useState("");

  const datasets = useQuery({ queryKey: ["datasets", projectId], queryFn: () => api<Dataset[]>(`/projects/${projectId}/datasets`) });
  useEffect(() => { if (!datasetId && datasets.data?.length) setDatasetId(datasets.data[0].id); }, [datasets.data, datasetId]);
  const versions = useQuery({ queryKey: ["versions", projectId, datasetId], enabled: Boolean(datasetId), queryFn: () => api<DatasetVersion[]>(`/projects/${projectId}/datasets/${datasetId}/versions`) });
  useEffect(() => { if (versions.data?.length && !versions.data.some((v) => v.id === versionId)) setVersionId(versions.data[0].id); }, [versions.data, versionId]);
  const selectedDataset = datasets.data?.find((item) => item.id === datasetId);
  const version = versions.data?.find((item) => item.id === versionId);
  const columns = useMemo(
    () => (version?.schema_json || version?.dataset_schema)?.columns?.map((column) => column.name) || [],
    [version],
  );
  useEffect(() => { if (columns.length && !columns.includes(target)) setTarget(columns[columns.length - 1]); }, [columns, target]);
  useEffect(() => { if (selectedDataset && version) setRunName(`${selectedDataset.name}-v${version.version_number}`); }, [selectedDataset, version]);

  const estimators = useQuery({ queryKey: ["estimators", projectId, task], enabled: Boolean(projectId), queryFn: () => api<Estimator[]>(`/projects/${projectId}/training/estimators?task_type=${task}`) });
  useEffect(() => { if (estimators.data) setModels(estimators.data.filter((item) => item.default_selected).map((item) => item.name)); }, [estimators.data]);
  const payload = useMemo<TrainingPayload>(() => ({
    dataset_version_id: versionId, target_column: task === "clustering" ? null : target,
    evaluation_column: null, task_type: task, prefer_gpu: gpu, expected_minutes: minutes,
    candidate_limit: models.length, candidate_models: models, optimization_iterations: iterations, cv_folds: folds,
  }), [versionId, target, task, gpu, minutes, models, iterations, folds]);
  const estimate = useMutation({ mutationFn: () => api<TrainingEstimate>(`/projects/${projectId}/training/estimate`, json("POST", payload)) });
  const launch = useMutation({
    mutationFn: () => api(`/projects/${projectId}/training/runs`, json("POST", { ...payload, run_name: runName, params: {} })),
    onSuccess: () => { client.invalidateQueries({ queryKey: ["runs", projectId] }); navigate(`/projects/${projectId}/runs`); },
  });

  if (datasets.isLoading) return <Loading />;
  if (datasets.error) return <ErrorState error={datasets.error} retry={() => datasets.refetch()} />;
  if (!datasets.data?.length) return <><PageHeader eyebrow="Model training" title="Train a model" description="Configure an evidence-led experiment with governed compute." />
    <Card><div className="prerequisite"><Database /><h2>Data comes first</h2><p>Upload and profile a dataset before configuring training.</p><Button onClick={() => navigate(`/projects/${projectId}/data`)}>Go to data<ArrowRight size={16} /></Button></div></Card></>;

  return <>
    <PageHeader eyebrow="Model training" title="Configure training" description="Sceptre estimates resource needs before anything reaches the cluster." />
    <div className="training-layout"><div className="training-form">
      <Card className="form-section"><span className="step-number">1</span><div className="form-section__body"><h2>Choose training data</h2><p>Select an immutable dataset version.</p>
        <div className="form-grid"><label>Dataset<select value={datasetId} onChange={(e) => { setDatasetId(e.target.value); setVersionId(""); estimate.reset(); }}>{datasets.data.map((item) => <option value={item.id} key={item.id}>{item.name}</option>)}</select></label>
          <label>Version<select value={versionId} onChange={(e) => { setVersionId(e.target.value); estimate.reset(); }}>{versions.data?.map((item) => <option value={item.id} key={item.id}>Version {item.version_number} · {titleCase(item.status)}</option>)}</select></label></div></div></Card>
      <Card className="form-section"><span className="step-number">2</span><div className="form-section__body"><h2>Frame the problem</h2><p>Confirm what the model should learn.</p>
        <div className="task-grid">{(["classification", "regression", "time_series", "clustering"] as TaskType[]).map((value) =>
          <button key={value} className={task === value ? "active" : ""} onClick={() => { setTask(value); estimate.reset(); }}><i>{task === value && <Check size={13} />}</i><b>{titleCase(value)}</b></button>)}</div>
        {task !== "clustering" && <label>Target column<select value={target} onChange={(e) => { setTarget(e.target.value); estimate.reset(); }}>{columns.map((column) => <option key={column}>{column}</option>)}</select><small>The outcome the model will predict.</small></label>}
      </div></Card>
      <Card className="form-section"><span className="step-number">3</span><div className="form-section__body"><h2>Select candidate models</h2><p>Start broad; Sceptre will rank compatible candidates with the right metrics.</p>
        {estimators.isLoading ? <Loading label="Discovering compatible estimators…" /> : <div className="model-grid">{estimators.data?.map((model) =>
          <label className={models.includes(model.name) ? "model-option active" : "model-option"} key={model.name}><input type="checkbox" checked={models.includes(model.name)} onChange={() => { setModels((current) => current.includes(model.name) ? current.filter((name) => name !== model.name) : current.length < 20 ? [...current, model.name] : current); estimate.reset(); }} />
            <span><b>{model.name}</b><small>{titleCase(model.cost_tier)} cost · {model.tunable ? "Tuned" : "Fixed"}</small></span><i><Check size={13} /></i></label>)}</div>}
        <span className="selection-count">{models.length} of 20 models selected</span>
      </div></Card>
      <Card className="form-section"><span className="step-number">4</span><div className="form-section__body"><h2>Set the experiment budget</h2><p>These limits shape the search without compromising cluster safeguards.</p>
        <div className="form-grid form-grid--3"><label>Planned duration<input type="number" min={1} max={120} value={minutes} onChange={(e) => { setMinutes(Number(e.target.value)); estimate.reset(); }} /><small>minutes</small></label>
          <label>Cross-validation folds<select value={folds} onChange={(e) => { setFolds(Number(e.target.value)); estimate.reset(); }}>{[2,3,4,5].map((n) => <option key={n}>{n}</option>)}</select></label>
          <label>Search iterations<input type="number" min={1} max={25} value={iterations} onChange={(e) => { setIterations(Number(e.target.value)); estimate.reset(); }} /></label></div>
        <label className="toggle-row"><span><Zap /><span><b>Prefer GPU</b><small>Falls back safely if unavailable.</small></span></span><input type="checkbox" checked={gpu} onChange={(e) => { setGpu(e.target.checked); estimate.reset(); }} /></label>
      </div></Card>
    </div>
    <aside className="launch-card"><Card><span className="eyebrow">Launch summary</span><h2>{runName || "New training run"}</h2>
      <dl><div><dt>Dataset</dt><dd>{selectedDataset?.name} · v{version?.version_number}</dd></div><div><dt>Task</dt><dd>{titleCase(task)}</dd></div><div><dt>Target</dt><dd>{task === "clustering" ? "Unsupervised" : target || "—"}</dd></div><div><dt>Models</dt><dd>{models.length} candidates</dd></div></dl>
      {!estimate.data ? <><Notice><Info size={16} /> Review compute requirements before launch.</Notice><Button className="full" disabled={!versionId || !models.length || (task !== "clustering" && !target)} loading={estimate.isPending} onClick={() => estimate.mutate()}><Cpu size={16} />Estimate resources</Button></>
        : <div className="estimate"><div className="estimate__metrics"><Metric label="CPU" value={`${estimate.data.cpu_request_cores} cores`} /><Metric label="Memory" value={`${estimate.data.memory_request_mb} MiB`} /></div><div className="estimate__metrics"><Metric label="Core-hours" value={estimate.data.estimated_core_hours} /><Metric label="Free CPU" value={estimate.data.capacity.available_cpu_cores.toFixed(1)} /></div>
          {estimate.data.warnings.length > 0 && <Notice>{estimate.data.warnings.join(" ")}</Notice>}{estimate.data.blockers.length > 0 && <Notice tone="danger">{estimate.data.blockers.join(" ")}</Notice>}
          <label>Run name<input value={runName} maxLength={255} onChange={(e) => setRunName(e.target.value)} /></label>
          {launch.error && <Notice tone="danger">{launch.error.message}</Notice>}<Button className="full" disabled={!estimate.data.can_launch} loading={launch.isPending} onClick={() => launch.mutate()}><Sparkles size={16} />Launch training</Button>
          <button className="text-button" onClick={() => estimate.reset()}>Change configuration</button></div>}</Card></aside>
    </div>
  </>;
}
