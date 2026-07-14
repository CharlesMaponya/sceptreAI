export type ID = string;
export type RunStatus =
  | "queued" | "precheck_running" | "running" | "succeeded"
  | "failed" | "cancelled" | "preempted";

export interface User {
  id: ID; email: string; full_name: string | null; global_role: string;
  is_active: boolean; is_verified: boolean; created_at: string;
}
export interface Tokens {
  access_token: string; refresh_token: string; token_type: string; expires_in: number;
}
export interface AuthResponse { user: User; tokens: Tokens }
export interface Project {
  id: ID; owner_id: ID; name: string; description: string | null;
  status: string; settings: Record<string, unknown>; created_at: string; updated_at: string;
}
export interface Member {
  id: ID; user_id: ID; email: string; full_name: string | null; role: string;
  accepted_at: string | null; expires_at: string | null;
}
export interface Dataset {
  id: ID; project_id: ID; name: string; description: string | null;
  latest_version_number: number; tags: Record<string, unknown>; created_at: string;
}
export interface DatasetVersion {
  id: ID; dataset_id: ID; version_number: number; status: string; format: string;
  original_filename: string | null; content_hash: string; byte_size: number | null;
  row_count: number | null; column_count: number | null;
  schema_json?: { columns?: DatasetColumnPreview[] };
  dataset_schema?: { columns?: DatasetColumnPreview[] };
  created_at: string;
}
export interface DatasetColumnPreview {
  name: string; dtype?: string; semantic_type?: string;
  sample_values?: string[];
  preview_kind?: "histogram" | "bar";
  preview_values?: Array<string | number>;
  preview_distribution?: Array<{ label: string; count: number }>;
  statistics?: Record<string, string | number | null>;
}
export interface ProfileJob {
  id: ID; status: RunStatus; current_stage?: string; completed_columns?: number;
  total_columns?: number; row_count?: number; target_column?: string | null;
  progress?: number; overview_json?: {
    task_inference?: { task_type: TaskType; confidence: number; rationale: string };
  };
  warnings_json?: string[]; failure_message?: string | null;
}
export type TaskType = "classification" | "regression" | "time_series" | "clustering";
export interface Estimator {
  name: string; task_type: TaskType; mixin: string; tunable: boolean;
  cost_tier: string; default_selected: boolean;
}
export interface TrainingPayload {
  dataset_version_id: ID; target_column: string | null; evaluation_column: string | null;
  task_type: TaskType; prefer_gpu: boolean; expected_minutes: number;
  candidate_limit: number; candidate_models: string[]; optimization_iterations: number;
  cv_folds: number;
}
export interface Capacity {
  connected: boolean; source: string; available_cpu_cores: number;
  available_memory_mb: number; ready_nodes: number; gpu_available: boolean;
  active_training_jobs: number; warnings: string[];
}
export interface TrainingEstimate {
  capacity: Capacity; estimated_working_set_mb: number; cpu_request_cores: number;
  cpu_limit_cores: number; memory_request_mb: number; memory_limit_mb: number;
  gpu_requested: boolean; gpu_vendor?: "nvidia" | "intel" | null;
  gpu_resource?: string | null; selected_node?: string | null;
  estimated_core_hours: number; can_launch: boolean;
  blockers: string[]; warnings: string[]; active_deadline_seconds: number;
}
export interface ModelRun {
  id: ID; dataset_version_id: ID; run_kind: string; status: RunStatus;
  task_type: TaskType; target_column: string | null; run_name: string | null;
  cpu_request_cores: number | null; memory_request_mb: number | null;
  params: Record<string, unknown>; plain_english_failure: string | null;
  failure_message: string | null; created_at: string; finished_at: string | null;
}
export interface TrainingResourceUsage {
  run_id: ID; status: RunStatus; pod_name: string | null; pod_phase: string | null;
  node_name: string | null; current_candidate: string | null; last_candidate?: string | null;
  current_phase: string | null;
  completed_candidates: number; total_candidates: number; progress: number;
  elapsed_seconds: number; estimated_remaining_seconds: number | null;
  cpu_request_cores: number | null; cpu_limit_cores: number | null;
  cpu_usage_cores: number | null; peak_cpu_usage_cores: number | null;
  memory_request_mb: number | null; memory_limit_mb: number | null;
  memory_usage_mb: number | null; peak_memory_usage_mb: number | null;
  gpu_requested: boolean; gpu_vendor: string | null; gpu_resource: string | null;
  gpu_count: number; gpu_utilization_percent: number | null;
  gpu_memory_used_mb: number | null; gpu_memory_total_mb: number | null;
  gpu_telemetry_available: boolean; telemetry_available: boolean;
  restart_count: number; status_reason: string | null; sampled_at: string;
}
export interface LeaderboardEntry {
  rank: number | null; model: string; status: string; cost_tier: string;
  primary_score: number | null; metrics: Record<string, number>;
  diagnostics: Record<string, unknown>; best_params: Record<string, unknown>;
  duration_seconds: number | null; error: string | null;
}
export interface Leaderboard {
  run_id: ID; status: RunStatus; primary_metric: string | null; winner: string | null;
  metric_directions: Record<string, string>; entries: LeaderboardEntry[];
}
export interface PlatformHealth {
  capacity: Capacity; active_deployments: number; components: Record<string, string>;
}
