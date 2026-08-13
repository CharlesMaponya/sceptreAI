import type { DatasetColumnPreview, Leaderboard, TaskType } from "../types";

export type TargetColumnProfile = {
  name: string;
  semantic_type: string;
  statistics: Record<string, string | number | null>;
  distribution: Array<{ label: string; count: number }>;
  preview_values?: Array<string | number>;
  preview_distribution?: Array<{ label: string; count: number }>;
};

type CloudWord = { word: string; count: number };
type PlacedCloudWord = CloudWord & { x: number; y: number; size: number; color: string };
type LeaderboardEntry = Leaderboard["entries"][number];

export function buildWordCloudTrace(words: CloudWord[]) {
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

export function formatStatistic(value: unknown) {
  if (value == null) return "—";
  if (typeof value === "number") return value.toLocaleString(undefined, { maximumFractionDigits: 4 });
  return String(value);
}

export function inferPreviewTask(target: string | null, column?: DatasetColumnPreview): TaskType {
  if (!target) return "clustering";
  if (column?.semantic_type === "temporal") return "time_series";
  if (column?.semantic_type === "numerical_continuous") return "regression";
  return "classification";
}

export function previewTaskRationale(task: TaskType, target: string | null) {
  if (!target) return "No target is selected, so the provisional task is unsupervised clustering.";
  if (task === "regression") return "The selected target is continuous numeric, indicating regression.";
  if (task === "time_series") return "The selected target is temporal, indicating time-series analysis.";
  return "The selected target is categorical, text-like, or low-cardinality numeric, indicating classification.";
}

export function previewColumnProfile(column: DatasetColumnPreview): TargetColumnProfile {
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

export function formatTargetStatistic(value: string | number | null | undefined) {
  if (value == null) return "—";
  return typeof value === "number" ? value.toLocaleString(undefined, { maximumFractionDigits: 3 }) : value;
}

export function fallbackPipeline(entry: LeaderboardEntry, task: TaskType) {
  const completed = entry.status === "succeeded";
  const planned = completed ? "completed" : entry.status === "running" ? "running" : "planned";
  return [
    { key: "data", label: "Immutable data", status: completed ? "completed" : "ready", summary: "Load the selected dataset version." },
    { key: "leakage", label: "Leakage gate", status: planned, summary: "Remove profiling-confirmed leakage features." },
    { key: "split", label: "Validation design", status: planned, summary: task === "time_series" ? "Ordered holdout and time-series folds." : "Task-aware holdout and cross-validation." },
    { key: "processing", label: "Feature processing", status: planned, summary: "Impute and encode the fitted feature contract." },
    { key: "selection", label: "Feature selection", status: planned, summary: task === "clustering" ? "Remove correlated numeric features using completeness." : "Remove correlated numeric features, then keep the top 80% by mutual information." },
    { key: "fit", label: "Tune & fit", status: planned, summary: `Fit ${entry.model} with recorded parameters.` },
    { key: "evaluate", label: "Evaluate", status: planned, summary: "Calculate task-aware metrics and diagnostics." },
    { key: "persist", label: "Persist evidence", status: planned, summary: "Store the fitted pipeline and MLflow evidence." },
  ];
}

export function fallbackPipelineDiagram(entry: LeaderboardEntry, task: TaskType) {
  return {
    input_gates: ["Immutable dataset version", "Leakage gate", "Temporal normalization"],
    correlation_filter: {
      name: "Correlation filter", type: "CorrelatedFeatureFilter",
      summary: "Remove numeric pairs at |r| ≥ 0.90 using task-aware training evidence.",
    },
    transformer: { name: "preprocessor", type: "ColumnTransformer", branches: [
      { key: "numeric", label: "Numeric", steps: ["Median imputation", "Standard scaling"] },
      { key: "categorical", label: "Categorical & text", steps: ["Most-frequent imputation", "Ordinal encoding"] },
    ] },
    selector: task === "clustering" ? null : {
      name: "Feature selection", type: "SelectPercentile", summary: "Keep the top 80% by mutual information.",
    },
    estimator: { name: "estimator", type: entry.model },
  };
}

export function formatNumber(value: number | null, suffix: string) {
  return value == null ? "—" : `${value.toLocaleString()}${suffix}`;
}

export function formatDuration(seconds: number) {
  const minutes = Math.floor(seconds / 60);
  const remainder = Math.round(seconds % 60);
  return minutes ? `${minutes}m ${remainder}s` : `${remainder}s`;
}

export function formatMetric(name: string, value: number) {
  if (name.includes("rate") || name.includes("share") || name.includes("accuracy") || name.includes("f1")) {
    return `${(value * 100).toFixed(1)}%`;
  }
  if (name.includes("latency") || name.endsWith("_ms")) return `${value.toFixed(0)} ms`;
  return value.toLocaleString(undefined, { maximumFractionDigits: 4 });
}
