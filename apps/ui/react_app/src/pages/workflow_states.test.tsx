import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Route, Routes } from "react-router";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { setSession } from "../api";
import { DataPage } from "./DataPage";
import { MonitoringPage } from "./MonitoringPage";
import { OperationsPage } from "./OperationsPage";
import { RunsPage } from "./RunsPage";
import { TrainingPage } from "./TrainingPage";

vi.mock("../components/PlotlyChart", () => ({
  default: () => <div data-testid="plotly-chart" />,
}));

const response = (data: unknown, status = 200) => Promise.resolve(new Response(JSON.stringify(data), {
  status, headers: { "Content-Type": "application/json" },
}));

function renderRoute(element: React.ReactNode, path: string) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  return render(<QueryClientProvider client={client}><MemoryRouter initialEntries={[path]}>
    <Routes><Route path="/projects/:projectId/*" element={element} /></Routes>
  </MemoryRouter></QueryClientProvider>);
}

const health = {
  capacity: {
    connected: true, source: "kubernetes", available_cpu_cores: 3.5,
    available_memory_mb: 6144, ready_nodes: 2, gpu_available: false,
    active_training_jobs: 0, warnings: ["One worker is under memory pressure."],
  },
  active_deployments: 1, components: { database: "ok" },
};

const trainingRun = {
  id: "run-1", dataset_version_id: "version-1", run_kind: "training", status: "succeeded",
  task_type: "classification", target_column: "churned", run_name: "retention-v1",
  cpu_request_cores: 1, memory_request_mb: 1024, params: {}, plain_english_failure: null,
  failure_message: null, created_at: "2026-01-01T00:00:00Z", finished_at: "2026-01-01T00:10:00Z",
};

const monitoredDeployment = {
  project_id: "project-1", project_name: "Retention", deployment_run_id: "deploy-1",
  model_version_id: "model-v1", registry_entry_id: "registry-1", model_name: "RandomForestClassifier",
  model_version: 2, environment: "production", task_type: "classification",
  deployment_status: "succeeded", health_status: "healthy", deployed_at: "2026-06-01T00:00:00Z",
  last_observation_at: "2026-08-12T08:00:00Z",
  monitoring: {
    enabled: true, schedule: "daily", resource_class: "standard", metrics: ["accuracy"],
    thresholds: { accuracy: { warning: .85, critical: .75, direction: "below" } },
    retraining_enabled: false, approval_required: true, revision: 3,
    updated_at: "2026-08-01T00:00:00Z", updated_by_id: "user-1",
  },
  baseline_metric_name: "accuracy", baseline_metric_value: .94,
  metric_series: [{
    name: "accuracy", kind: "performance", higher_is_better: true,
    points: [{ id: "metric-1", name: "accuracy", kind: "performance", value: .92,
      recorded_at: "2026-08-12T08:00:00Z", sample_count: 400, higher_is_better: true,
      status: "healthy", metadata: {} }],
  }],
  drift_history: [{ id: "drift-1", name: "drift_share", kind: "drift", value: .12,
    recorded_at: "2026-08-11T08:00:00Z", sample_count: 400, higher_is_better: false,
    status: "warning", metadata: {} }],
  retraining_events: 1, open_alerts: 0, governance_reports: 1,
  timeline: [{ kind: "deployment", label: "Production rollout", status: "succeeded",
    occurred_at: "2026-06-01T00:00:00Z", details: {} }],
};

describe("governed workflow states", () => {
  beforeEach(() => {
    setSession(null);
    vi.restoreAllMocks();
  });

  it("renders rich profile evidence, leakage exclusions, and feature details", async () => {
    vi.spyOn(globalThis, "fetch").mockImplementation((input) => {
      const url = String(input);
      if (url.endsWith("/projects/project-1/datasets")) return response([{
        id: "dataset-1", name: "Retention signals", description: "Curated customer events",
        latest_version_number: 1,
      }]);
      if (url.endsWith("/datasets/dataset-1/versions")) return response([{
        id: "version-1", dataset_id: "dataset-1", version_number: 1, status: "ready",
        format: "parquet", byte_size: 4096, row_count: 12500, column_count: 3,
      }]);
      if (url.endsWith("/profile-jobs/latest")) return response({
        id: "profile-1", status: "succeeded", target_column: "churned",
      });
      if (url.endsWith("/profile-jobs/profile-1/result")) return response({
        id: "profile-1", status: "succeeded", target_column: "churned",
        overview_json: {
          task_inference: { task_type: "classification", confidence: .93, rationale: "A binary target was detected." },
          leakage_analysis: {
            status: "completed", analyzed_rows: 12500, excluded_columns: ["outcome_copy"],
            findings: [{ column: "outcome_copy", reason: "Exact target proxy.", confidence: .99, auto_excluded: true }],
          },
        },
        feature_profiles_json: {
          customer_age: {
            name: "customer_age", semantic_type: "numerical_continuous", distinct_count: 67,
            missing_count: 4, missing_ratio: .00032, distribution: [{ label: "18–30", count: 200 }],
            statistics: { count: 12500, min: 18, q1: 31, median: 42, q3: 57, max: 85, mean: 44.25, stddev: 12.4 },
          },
          notes: {
            name: "notes", semantic_type: "text", distinct_count: 800, missing_count: 10,
            missing_ratio: .0008, statistics: { word_frequencies: [
              { word: "retention", count: 80 }, { word: "service", count: 40 },
              { word: "", count: 99 }, { word: "invalid", count: 0 },
            ], avg_length: 42, max_length: 220 },
          },
          outcome_copy: {
            name: "outcome_copy", semantic_type: "categorical", distinct_count: 2,
            missing_count: 0, missing_ratio: 0, distribution: [{ label: "yes", count: 500 }],
            statistics: { top_values: [{ value: "yes", count: 500 }, { value: "no", count: 12000 }] },
          },
          churned: { name: "churned", semantic_type: "categorical", distinct_count: 2 },
        },
        preparation_json: [{ column: "customer_age", action: "impute", strategy: "median", reason: "Four values are missing." }],
        relationships_json: [{ source_column: "customer_age", target_column: "churned", method: "cramers_v", value: .31415 }],
      });
      return response([]);
    });
    const user = userEvent.setup();
    renderRoute(<DataPage />, "/projects/project-1/data");

    expect(await screen.findByText("Target leakage removed before training")).toBeInTheDocument();
    expect(screen.getByText(/outcome_copy is a high-confidence target proxy/)).toBeInTheDocument();
    expect(screen.getByText("Classification")).toBeInTheDocument();
    expect(screen.getByText("93%")).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: /customer_age/i }));
    expect(screen.getByRole("heading", { name: "Histogram" })).toBeInTheDocument();
    expect(screen.getByText("Cramér's V")).toBeInTheDocument();
    expect(screen.getByText("Impute")).toBeInTheDocument();
    expect(screen.getByText("44.25")).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: /notes/i }));
    expect(screen.getByRole("heading", { name: "Word cloud" })).toBeInTheDocument();
    expect(await screen.findByTestId("plotly-chart")).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: /outcome_copy/i }));
    expect(screen.getByText(/Exact target proxy/)).toBeInTheDocument();
    expect(screen.getByText("Category distribution")).toBeInTheDocument();
    expect(screen.getByText("12,000")).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: /churned/i }));
    expect(screen.getByText("Selected target; feature preprocessing is not applied.")).toBeInTheDocument();
    expect(screen.getByText("A distribution is not available.")).toBeInTheDocument();
  });

  it("registers, promotes, deploys, stops, and cleans up governed resources", async () => {
    const requests: Array<{ url: string; body?: unknown }> = [];
    const registryEntry = {
      id: "registry-1", model_run_id: "run-1", stage: "staging", model_name: "RandomForestClassifier",
      version: 2, champion_metric_name: "accuracy", champion_metric_value: .91234,
      is_fallback: false, created_at: "2026-01-01T00:00:00Z", training_dataset_version_id: "version-1",
      training_feature_columns: ["age", "tenure"],
    };
    vi.spyOn(globalThis, "fetch").mockImplementation((input, options) => {
      const url = String(input);
      if (options?.method === "POST") {
        requests.push({ url, body: options.body ? JSON.parse(String(options.body)) : undefined });
        if (url.endsWith("/operations/cleanup")) {
          const dryRun = JSON.parse(String(options.body)).dry_run;
          return response({
            dry_run: dryRun, artifact_count: 2, artifact_bytes: 3072, artifact_ids: ["a", "b"],
            deleted_object_uris: dryRun ? [] : ["s3://artifacts/a"],
            deleted_kubernetes_jobs: dryRun ? [] : ["job-1"], errors: [],
          });
        }
        return response({ status: "accepted" }, 202);
      }
      if (url.endsWith("/operations/health")) return response(health);
      if (url.endsWith("/operations/registry")) return response([registryEntry]);
      if (url.endsWith("/operations/deployments")) return response([{
        run: { ...trainingRun, id: "deploy-1", run_name: "retention-api" },
        runtime_state: "ready", endpoint: null, status: "succeeded",
        platform_endpoint: "/api/v1/projects/project-1/operations/deployments/deploy-1/inference/v1/predict",
      }]);
      if (url.endsWith("/operations/drift-runs")) return response([{
        ...trainingRun, id: "drift-1", run_name: "weekly-drift", tags: {
          diagnostics: { drift_share_percent: 12.5, drifted_feature_count: 2 },
        },
      }]);
      return response([]);
    });
    const user = userEvent.setup();
    renderRoute(<OperationsPage />, "/projects/project-1/operations");

    expect(await screen.findByText("One worker is under memory pressure.")).toBeInTheDocument();
    expect(screen.getAllByText("12.5%")).toHaveLength(2);
    expect(screen.getByText("weekly-drift")).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "Set fallback" }));
    await waitFor(() => expect(requests.some(({ url }) => url.endsWith("/registry/registry-1/fallback"))).toBe(true));

    await user.selectOptions(screen.getByLabelText("Stage for RandomForestClassifier"), "production");
    await user.click(screen.getByRole("button", { name: "Update stage" }));
    await waitFor(() => expect(requests).toContainEqual(expect.objectContaining({
      url: expect.stringMatching(/registry\/registry-1\/stage$/), body: { stage: "production" },
    })));

    await user.click(screen.getByRole("button", { name: "Deploy" }));
    const deployDialog = screen.getByRole("dialog", { name: /Deploy RandomForestClassifier/ });
    await user.click(within(deployDialog).getByRole("button", { name: "Deploy model" }));
    await waitFor(() => expect(requests).toContainEqual(expect.objectContaining({
      url: expect.stringMatching(/registry\/registry-1\/deployments$/),
      body: { replicas: 1, cpu_request: "500m", memory_request: "1Gi" },
    })));

    await user.click(screen.getByRole("button", { name: "Stop" }));
    await user.click(within(screen.getByRole("dialog", { name: "Stop this deployment?" }))
      .getByRole("button", { name: "Stop deployment" }));
    await waitFor(() => expect(requests.some(({ url }) => url.endsWith("/deployments/deploy-1/stop"))).toBe(true));

    await user.click(screen.getByRole("button", { name: "Preview cleanup" }));
    const deleteButton = screen.getByRole("button", { name: /Delete eligible resources/ });
    await waitFor(() => expect(deleteButton).toBeEnabled());
    await user.click(deleteButton);
    await user.click(within(screen.getByRole("dialog", { name: /Delete 2 eligible artifacts/ }))
      .getByRole("button", { name: "Delete resources" }));
    await waitFor(() => expect(requests.some(({ url, body }) =>
      url.endsWith("/operations/cleanup") && (body as { dry_run?: boolean }).dry_run === false)).toBe(true));
  });

  it("registers a successful training candidate", async () => {
    let registrationBody: unknown;
    vi.spyOn(globalThis, "fetch").mockImplementation((input, options) => {
      const url = String(input);
      if (url.endsWith("/operations/health")) return response(health);
      if (url.endsWith("/operations/registry") && options?.method === "POST") {
        registrationBody = JSON.parse(String(options.body));
        return response({ id: "registry-1" }, 201);
      }
      if (url.endsWith("/operations/registry")) return response([]);
      if (url.endsWith("/operations/deployments") || url.endsWith("/operations/drift-runs")) return response([]);
      if (url.endsWith("/training/runs")) return response([trainingRun, { ...trainingRun, id: "failed", status: "failed" }]);
      if (url.endsWith("/training/runs/run-1/leaderboard")) return response({ entries: [
        { model: "RandomForestClassifier", status: "succeeded" },
        { model: "BrokenClassifier", status: "failed" },
      ] });
      return response([]);
    });
    const user = userEvent.setup();
    renderRoute(<OperationsPage />, "/projects/project-1/operations");

    await user.click((await screen.findAllByRole("button", { name: "Register model" }))[0]);
    await user.selectOptions(await screen.findByLabelText("Training run"), "run-1");
    await waitFor(() => expect(screen.getByLabelText("Successful candidate")).toHaveValue("RandomForestClassifier"));
    await user.click(within(screen.getByRole("dialog", { name: "Register a trained model" }))
      .getByRole("button", { name: "Register model" }));
    await waitFor(() => expect(registrationBody).toEqual({
      training_run_id: "run-1", model_name: "RandomForestClassifier",
    }));
    expect(screen.queryByRole("dialog", { name: "Register a trained model" })).not.toBeInTheDocument();
  });

  it("renders every deployment access state and refreshes session credentials", async () => {
    setSession({
      user: {
        id: "user-1", email: "ada@example.com", full_name: "Ada", global_role: "member",
        auth_provider: "simple", is_active: true, is_verified: true,
        created_at: "2026-01-01T00:00:00Z",
      },
      tokens: { access_token: "first-token", refresh_token: "refresh", token_type: "bearer", expires_in: 3600 },
    });
    vi.spyOn(globalThis, "fetch").mockImplementation((input) => {
      const url = String(input);
      if (url.endsWith("/operations/health")) return response(health);
      if (url.endsWith("/operations/registry")) return response([]);
      if (url.endsWith("/operations/drift-runs")) return response([{ ...trainingRun,
        id: "drift-pending", run_name: "pending drift", status: "running", tags: {},
      }]);
      if (url.endsWith("/operations/deployments")) return response([
        { run: { ...trainingRun, id: "ready", run_name: "ready-api" }, runtime_state: "ready",
          status: "succeeded", endpoint: null, platform_endpoint: "/api/v1/predict",
          internal_endpoint: "http://ready.svc/v1/predict", service_name: "ready" },
        { run: { ...trainingRun, id: "failed", run_name: "failed-api" }, runtime_state: "unavailable",
          status: "failed", endpoint: null },
        { run: { ...trainingRun, id: "partial", run_name: "partial-api" }, runtime_state: "starting",
          status: "succeeded", endpoint: null },
        { run: { ...trainingRun, id: "running", run_name: "running-api" }, runtime_state: "running",
          status: "running", endpoint: null },
      ]);
      return response([]);
    });
    const user = userEvent.setup();
    const writeText = vi.fn().mockRejectedValueOnce(new Error("denied")).mockResolvedValue(undefined);
    Object.defineProperty(navigator, "clipboard", { configurable: true, value: { writeText } });
    renderRoute(<OperationsPage />, "/projects/project-1/operations");

    expect((await screen.findAllByText("Unavailable")).length).toBeGreaterThanOrEqual(2);
    expect(screen.getByText("API access unavailable")).toBeInTheDocument();
    expect(screen.getByText("Provisioning")).toBeInTheDocument();
    expect(screen.getByText("pending drift")).toBeInTheDocument();
    expect(screen.getAllByText("—").length).toBeGreaterThanOrEqual(2);

    await user.click(screen.getByRole("button", { name: "API access" }));
    const token = screen.getByLabelText("Sceptre access token");
    await user.click(screen.getByRole("button", { name: "Copy token" }));
    expect(await screen.findByText(/Clipboard access was blocked/)).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Copy header value" }));
    expect(await screen.findByRole("button", { name: "Header copied" })).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Show access token" }));
    expect(token).toHaveAttribute("type", "text");

    setSession({
      user: {
        id: "user-1", email: "ada@example.com", full_name: "Ada", global_role: "member",
        auth_provider: "simple", is_active: true, is_verified: true,
        created_at: "2026-01-01T00:00:00Z",
      },
      tokens: { access_token: "refreshed-token", refresh_token: "refresh", token_type: "bearer", expires_in: 3600 },
    });
    await waitFor(() => expect(token).toHaveValue("refreshed-token"));
    expect(token).toHaveAttribute("type", "password");
    expect(screen.queryByText(/Clipboard access was blocked/)).not.toBeInTheDocument();
    expect(screen.getByText("http://ready.svc/v1/predict")).toBeInTheDocument();
    expect(screen.queryByText(/Service ready in namespace/)).not.toBeInTheDocument();
    await user.click(within(screen.getByRole("dialog", { name: "Model API access" }))
      .getAllByRole("button", { name: "Close" })[1]);
    expect(screen.queryByRole("dialog", { name: "Model API access" })).not.toBeInTheDocument();

    setSession(null);
    await user.click(screen.getByRole("button", { name: "API access" }));
    expect(screen.getByText(/session token is unavailable/i)).toBeInTheDocument();
  });

  it("keeps a failed stop confirmation open with an actionable error", async () => {
    vi.spyOn(globalThis, "fetch").mockImplementation((input, options) => {
      const url = String(input);
      if (url.endsWith("/operations/health")) return response(health);
      if (url.endsWith("/operations/registry") || url.endsWith("/operations/drift-runs")) return response([]);
      if (url.endsWith("/operations/deployments") && !options?.method) return response([{
        run: { ...trainingRun, id: "deploy-stop", run_name: "stop-me" }, runtime_state: "ready",
        status: "succeeded", endpoint: null, platform_endpoint: "/api/v1/predict",
      }]);
      if (url.endsWith("/deployments/deploy-stop/stop")) return response({ detail: "Deployment is protected." }, 409);
      return response([]);
    });
    const user = userEvent.setup();
    renderRoute(<OperationsPage />, "/projects/project-1/operations");

    const row = (await screen.findByText("stop-me")).closest("tr");
    expect(row).not.toBeNull();
    await user.click(within(row!).getByRole("button", { name: "Stop" }));
    await user.click(within(screen.getByRole("dialog", { name: "Stop this deployment?" }))
      .getByRole("button", { name: "Stop deployment" }));

    expect(await screen.findByText("Deployment is protected.")).toBeInTheDocument();
    expect(screen.getByRole("dialog", { name: "Stop this deployment?" })).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Cancel" }));
    expect(screen.queryByRole("dialog", { name: "Stop this deployment?" })).not.toBeInTheDocument();
  });

  it("renders degraded capacity, registry fallbacks, and protected cleanup results", async () => {
    vi.spyOn(globalThis, "fetch").mockImplementation((input, options) => {
      const url = String(input);
      if (url.endsWith("/operations/health")) return response({
        ...health,
        capacity: { ...health.capacity, connected: false, ready_nodes: 0,
          available_cpu_cores: 0, available_memory_mb: 0, warnings: [] },
        active_deployments: 0,
      });
      if (url.endsWith("/operations/registry")) return response([{
        id: "registry-fallback", model_run_id: "run-1", stage: "candidate",
        model_name: "BaselineClassifier", version: 1, champion_metric_name: null,
        champion_metric_value: null, is_fallback: true, created_at: "2026-01-01T00:00:00Z",
        training_dataset_version_id: "version-1", training_feature_columns: [],
      }]);
      if (url.endsWith("/operations/cleanup") && options?.method === "POST") return response({
        dry_run: true, artifact_count: 0, artifact_bytes: 0, artifact_ids: [],
        deleted_object_uris: [], deleted_kubernetes_jobs: [], errors: ["A legal hold protected this artifact."],
      });
      if (url.endsWith("/training/runs") || url.endsWith("/operations/deployments")
        || url.endsWith("/operations/drift-runs")) return response([]);
      return response([]);
    });
    const user = userEvent.setup();
    renderRoute(<OperationsPage />, "/projects/project-1/operations");

    expect(await screen.findByText("Degraded")).toBeInTheDocument();
    expect(screen.getByText("0 ready nodes")).toBeInTheDocument();
    expect(screen.getByText("Not recorded")).toBeInTheDocument();
    expect(screen.getByText("Safe fallback")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Set fallback" })).toBeDisabled();
    expect(screen.getByRole("button", { name: "Deploy" })).toBeDisabled();

    await user.click(screen.getByRole("button", { name: "Preview cleanup" }));
    expect(await screen.findByText("A legal hold protected this artifact.")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /Delete eligible resources/ })).toBeDisabled();

    await user.click(screen.getByRole("button", { name: "Register model" }));
    expect(await screen.findByText("No successful runs")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Configure training" })).toHaveAttribute(
      "href", "/projects/project-1/training",
    );
  });

  it("clears a preselected registration request when the operator cancels", async () => {
    vi.spyOn(globalThis, "fetch").mockImplementation((input) => {
      const url = String(input);
      if (url.endsWith("/operations/health")) return response(health);
      if (url.endsWith("/training/runs/run-missing/leaderboard")) return response({ entries: [] });
      return response([]);
    });
    const user = userEvent.setup();
    renderRoute(<OperationsPage />,
      "/projects/project-1/operations?trainingRunId=run-missing&model=Estimator");

    const dialog = await screen.findByRole("dialog", { name: "Register a trained model" });
    expect(await within(dialog).findByText("No successful runs")).toBeInTheDocument();
    await user.click(within(dialog).getByRole("button", { name: "Close" }));
    expect(screen.queryByRole("dialog", { name: "Register a trained model" })).not.toBeInTheDocument();
  });

  it("adds governed candidates with the inherited search budget", async () => {
    let addBody: Record<string, unknown> | null = null;
    vi.spyOn(globalThis, "fetch").mockImplementation((input, options) => {
      const url = String(input);
      if (url.endsWith("/training/runs")) return response([{
        ...trainingRun,
        params: { optimization_iterations: 5, cv_folds: 3, expected_minutes: 10 },
      }]);
      if (url.endsWith("/leaderboard")) return response({
        run_id: "run-1", status: "succeeded", primary_metric: "balanced_accuracy",
        winner: "RandomForestClassifier", metric_directions: { balanced_accuracy: "maximize" },
        entries: [{
          rank: 1, model: "RandomForestClassifier", status: "succeeded", cost_tier: "medium",
          primary_score: .91, metrics: { balanced_accuracy: .91 }, diagnostics: {},
          best_params: {}, duration_seconds: 12, error: null,
        }],
      });
      if (url.endsWith("/resources")) return response({
        run_id: "run-1", status: "succeeded", completed_candidates: 1,
        total_candidates: 1, progress: 1, elapsed_seconds: 12, restart_count: 0,
      });
      if (url.includes("/training/estimators")) return response([{
        name: "RandomForestClassifier", task_type: "classification", mixin: "ClassifierMixin",
        tunable: true, cost_tier: "medium", default_selected: true,
      }, {
        name: "ExtraTreesClassifier", task_type: "classification", mixin: "ClassifierMixin",
        tunable: true, cost_tier: "medium", default_selected: false,
      }, {
        name: "LogisticRegression", task_type: "classification", mixin: "ClassifierMixin",
        tunable: false, cost_tier: "low", default_selected: false,
      }]);
      if (url.endsWith("/models") && options?.method === "POST") {
        addBody = JSON.parse(String(options.body));
        return response({ accepted: true }, 202);
      }
      return response([]);
    });
    const user = userEvent.setup();
    renderRoute(<RunsPage />, "/projects/project-1/runs");

    await user.click(await screen.findByRole("button", { name: /Add models/ }));
    const dialog = await screen.findByRole("dialog", { name: "Train additional models" });
    expect(within(dialog).queryByText("RandomForestClassifier")).not.toBeInTheDocument();
    await user.click(within(dialog).getByRole("button", { name: "Select all models" }));
    await user.click(within(dialog).getByRole("button", { name: "Clear selection" }));
    await user.click(within(dialog).getByRole("checkbox", { name: /ExtraTreesClassifier/ }));
    await user.click(within(dialog).getByRole("checkbox", { name: /LogisticRegression/ }));
    await user.click(within(dialog).getByRole("checkbox", { name: /ExtraTreesClassifier/ }));
    await user.click(within(dialog).getByRole("checkbox", { name: /ExtraTreesClassifier/ }));
    await user.clear(within(dialog).getByLabelText("Search iterations"));
    await user.type(within(dialog).getByLabelText("Search iterations"), "7");
    await user.selectOptions(within(dialog).getByLabelText("CV folds"), "4");
    await user.clear(within(dialog).getByLabelText("Minutes"));
    await user.type(within(dialog).getByLabelText("Minutes"), "15");
    await user.click(within(dialog).getByRole("button", { name: "Train selected models" }));

    await waitFor(() => expect(addBody).toEqual({
      candidate_models: ["LogisticRegression", "ExtraTreesClassifier"],
      optimization_iterations: 7, cv_folds: 4, expected_minutes: 15, prefer_gpu: false,
    }));
    await waitFor(() => expect(screen.queryByRole("dialog", { name: "Train additional models" }))
      .not.toBeInTheDocument());
  });

  it("uploads schema-compatible validation data and renders the persisted result", async () => {
    let finishUpload: (() => void) | undefined;
    let validationBody: unknown;
    let launched = false;
    let uploadAttempt = 0;
    vi.spyOn(globalThis, "fetch").mockImplementation((input, options) => {
      const url = String(input);
      if (url.endsWith("/training/runs")) return response([trainingRun]);
      if (url.endsWith("/leaderboard")) return response({
        run_id: "run-1", status: "succeeded", primary_metric: "balanced_accuracy",
        winner: "RandomForestClassifier", metric_directions: { balanced_accuracy: "maximize" },
        entries: [{
          rank: 1, model: "RandomForestClassifier", status: "succeeded", cost_tier: "medium",
          primary_score: .91, metrics: { balanced_accuracy: .91 }, diagnostics: {},
          best_params: {}, duration_seconds: 10, error: null,
        }],
      });
      if (url.endsWith("/resources")) return response({
        run_id: "run-1", status: "succeeded", completed_candidates: 1,
        total_candidates: 1, progress: 1, elapsed_seconds: 10,
      });
      if (url === "/api/v1/projects/project-1/datasets") return response([{
        id: "dataset-1", name: "Training data", latest_version_number: 1,
      }]);
      if (url.endsWith("/datasets/dataset-1/versions")) return response([{
        id: "version-1", version_number: 1,
        schema_json: { columns: [{ name: "age" }, { name: "tenure" }] },
      }]);
      if (url.endsWith("/analyses")) return response(launched ? [{
        ...trainingRun, id: "validation-1", run_kind: "validation", status: "succeeded",
        run_name: "External cohort", params: { model_name: "RandomForestClassifier" },
      }] : []);
      if (url.endsWith("/validations") && options?.method === "POST") {
        validationBody = JSON.parse(String(options.body));
        launched = true;
        return response({ run: {
          ...trainingRun, id: "validation-1", run_kind: "validation", status: "queued",
          params: { model_name: "RandomForestClassifier" },
        } }, 202);
      }
      if (url.endsWith("/analyses/validation-1")) return response({
        run_id: "validation-1", status: "succeeded", model_name: "RandomForestClassifier",
        metrics: { balanced_accuracy: .88 }, diagnostics: {}, feature_importance: [],
        artifacts: [{ id: "artifact-1", name: "validation.json", kind: "evidence", byte_size: 512 }],
      });
      return response([]);
    });
    vi.spyOn(XMLHttpRequest.prototype, "open").mockImplementation(() => undefined);
    vi.spyOn(XMLHttpRequest.prototype, "setRequestHeader").mockImplementation(() => undefined);
    vi.spyOn(XMLHttpRequest.prototype, "send").mockImplementation(function (this: XMLHttpRequest) {
      uploadAttempt += 1;
      const columns = uploadAttempt === 1 ? [{ name: "age" }] : [
        { name: "age" }, { name: "tenure" }, { name: "region" },
      ];
      this.upload.dispatchEvent(new ProgressEvent("progress", {
        lengthComputable: true, loaded: 100, total: 100,
      }));
      finishUpload = () => {
        Object.defineProperty(this, "status", { configurable: true, value: 201 });
        Object.defineProperty(this, "responseText", { configurable: true, value: JSON.stringify({
          dataset: { id: "validation-dataset", name: "External cohort" },
          version: {
            id: "validation-version", version_number: 1,
            schema_json: { columns },
          },
        }) });
        this.onload?.call(this, new ProgressEvent("load"));
      };
    });
    const user = userEvent.setup();
    const { container } = renderRoute(<RunsPage />, "/projects/project-1/runs");

    await user.click(await screen.findByRole("tab", { name: "Validate & explain" }));
    const fileInput = container.querySelector<HTMLInputElement>('input[type="file"]');
    expect(fileInput).not.toBeNull();
    await user.upload(fileInput!, new File(["age,tenure,region\n30,4,north"], "cohort.csv", {
      type: "text/csv",
    }));
    await user.click(screen.getByRole("button", { name: "Upload and inspect" }));
    expect(await screen.findByText("100%")).toBeInTheDocument();
    act(() => finishUpload?.());
    expect(await screen.findByText(/Missing training columns: tenure/)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Run validation" })).toBeDisabled();
    await user.upload(fileInput!, new File(["age,tenure,region\n30,4,north"], "compatible.csv", {
      type: "text/csv",
    }));
    await user.click(screen.getByRole("button", { name: "Upload and inspect" }));
    act(() => finishUpload?.());
    expect(await screen.findByText("Schema matched: all 2 training columns are present.")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Run validation" }));

    await waitFor(() => expect(validationBody).toEqual({
      model_name: "RandomForestClassifier", dataset_version_id: "validation-version",
      evaluation_column: null, expected_minutes: 5,
    }));
    expect(await screen.findByText("0.8800")).toBeInTheDocument();
    expect(screen.getByText("validation.json")).toBeInTheDocument();
  });

  it("estimates and launches a governed training configuration", async () => {
    let estimateBody: Record<string, unknown> | null = null;
    let launchBody: Record<string, unknown> | null = null;
    vi.spyOn(globalThis, "fetch").mockImplementation((input, options) => {
      const url = String(input);
      if (url.endsWith("/projects/project-1/datasets")) return response([{
        id: "dataset-1", name: "Retention signals", latest_version_number: 1,
      }]);
      if (url.endsWith("/datasets/dataset-1/versions")) return response([{
        id: "version-1", dataset_id: "dataset-1", version_number: 1, status: "ready",
        schema_json: { columns: [{ name: "tenure" }, { name: "churned" }] },
      }]);
      if (url.endsWith("/profile-jobs/latest")) return response({
        id: "profile-1", status: "succeeded", target_column: "churned",
        overview_json: { task_inference: { task_type: "classification" } },
      });
      if (url.endsWith("/profile-jobs/profile-1/result")) return response({
        feature_profiles_json: { churned: { distribution: [
          { label: "yes", count: 20 }, { label: "no", count: 80 },
        ] } },
      });
      if (url.includes("/training/estimators")) return response([{
        name: "RandomForestClassifier", task_type: "classification", mixin: "ClassifierMixin",
        tunable: true, cost_tier: "medium", default_selected: true,
      }, {
        name: "LogisticRegression", task_type: "classification", mixin: "ClassifierMixin",
        tunable: false, cost_tier: "low", default_selected: false,
      }]);
      if (url.endsWith("/training/estimate") && options?.method === "POST") {
        estimateBody = JSON.parse(String(options.body));
        return response({
          cpu_request_cores: 4, memory_request_mb: 8192, gpu_requested: true,
          gpu_vendor: "nvidia", selected_node: "gpu-worker-1", estimated_core_hours: 1.5,
          capacity: { available_cpu_cores: 8 }, warnings: ["Capacity is reserved at launch."],
          blockers: [], can_launch: true,
        });
      }
      if (url.endsWith("/training/runs") && options?.method === "POST") {
        launchBody = JSON.parse(String(options.body));
        return response({ ...trainingRun, id: "run-new", status: "queued" }, 202);
      }
      return response([]);
    });
    const user = userEvent.setup();
    renderRoute(<TrainingPage />, "/projects/project-1/training");

    expect(await screen.findByText("1 of 2 models selected")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Clear selection" }));
    expect(screen.getByText("0 of 2 models selected")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Select all models" }));
    await user.clear(screen.getByLabelText(/Planned duration/));
    await user.type(screen.getByLabelText(/Planned duration/), "20");
    await user.selectOptions(screen.getByLabelText(/Cross-validation folds/), "4");
    await user.clear(screen.getByLabelText(/Search iterations/));
    await user.type(screen.getByLabelText(/Search iterations/), "7");
    const gpuPreference = screen.getByRole("checkbox", { name: /Prefer GPU/ });
    await user.click(gpuPreference);
    await user.click(gpuPreference);
    await user.selectOptions(screen.getByLabelText(/Primary leaderboard metric/), "roc_auc");
    await user.click(screen.getByRole("button", { name: "Estimate resources" }));

    await waitFor(() => expect(estimateBody).toEqual({
      dataset_version_id: "version-1", target_column: "churned", positive_label: "yes",
      evaluation_column: null, task_type: "classification", primary_metric: "roc_auc",
      prefer_gpu: true, expected_minutes: 20, candidate_limit: 2,
      candidate_models: ["RandomForestClassifier", "LogisticRegression"],
      optimization_iterations: 7, cv_folds: 4,
    }));
    expect(await screen.findByText("Nvidia")).toBeInTheDocument();
    expect(screen.getByText("Capacity is reserved at launch.")).toBeInTheDocument();
    await user.clear(screen.getByLabelText("Run name"));
    await user.type(screen.getByLabelText("Run name"), "retention-qualified");
    await user.click(screen.getByRole("button", { name: "Launch training" }));

    await waitFor(() => expect(launchBody).toEqual({
      ...(estimateBody as Record<string, unknown>), run_name: "retention-qualified", params: {},
    }));
  });

  it("renders time-series and clustering validation diagnostics", async () => {
    const timeRun = {
      ...trainingRun, id: "run-time", run_name: "forecast-v1", task_type: "time_series",
      target_column: "demand",
    };
    const clusterRun = {
      ...trainingRun, id: "run-cluster", run_name: "segments-v1", task_type: "clustering",
      target_column: null,
    };
    vi.spyOn(globalThis, "fetch").mockImplementation((input) => {
      const url = String(input);
      if (url.endsWith("/training/runs")) return response([timeRun, clusterRun]);
      if (url.includes("/run-time/leaderboard")) return response({
        run_id: "run-time", status: "succeeded", primary_metric: "mae",
        winner: "TemporalForest", metric_directions: { mae: "minimize" }, entries: [{
          rank: 1, model: "TemporalForest", status: "succeeded", cost_tier: "medium",
          primary_score: 1.2, metrics: { mae: 1.2 }, best_params: { lag: 7 },
          duration_seconds: 15, error: null, diagnostics: {
            prediction_samples: [
              { order: 1, actual: 10, predicted: 9, residual: 1 },
              { order: 2, actual: 14, predicted: 15, residual: -1 },
            ],
            cross_validation: { mean: .8, standard_deviation: .04 },
          },
        }],
      });
      if (url.includes("/run-cluster/leaderboard")) return response({
        run_id: "run-cluster", status: "succeeded", primary_metric: "silhouette",
        winner: "KMeans", metric_directions: { silhouette: "maximize" }, entries: [{
          rank: 1, model: "KMeans", status: "succeeded", cost_tier: "low",
          primary_score: .72, metrics: { silhouette: .72 }, best_params: { n_clusters: 3 },
          duration_seconds: 8, error: null, diagnostics: {
            cluster_sizes: { "0": 24, "1": 18, "2": 28 },
            cross_validation: { fold_metrics: [
              { silhouette: .69, calinski_harabasz: 105 },
              { silhouette: .72, calinski_harabasz: 111 },
            ] },
          },
        }],
      });
      if (url.endsWith("/resources")) return response({
        status: "succeeded", completed_candidates: 1, total_candidates: 1,
        progress: 1, elapsed_seconds: 15, restart_count: 0,
      });
      return response([]);
    });
    const user = userEvent.setup();
    renderRoute(<RunsPage />, "/projects/project-1/runs");

    await user.click(await screen.findByRole("button", { name: /TemporalForest/ }));
    await user.click(screen.getByRole("tab", { name: "Diagnostics" }));
    expect(await screen.findByRole("heading", { name: "Actual vs predicted" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Residual distribution" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Chronological holdout" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Cross-validation stability" })).toBeInTheDocument();
    await user.click(screen.getByRole("tab", { name: "Parameters" }));
    expect(screen.getByText(/"lag": 7/)).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: /segments-v1/i }));
    await user.click(await screen.findByRole("button", { name: /KMeans/ }));
    await user.click(screen.getByRole("tab", { name: "Diagnostics" }));
    expect(await screen.findByRole("heading", { name: "Cluster sizes" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Cross-validation by fold" })).toBeInTheDocument();
  });

  it("requires drift data to match the registered training schema", async () => {
    let finishUpload: (() => void) | undefined;
    let driftBody: unknown;
    const registryEntry = {
      id: "registry-1", model_run_id: "run-1", stage: "staging", model_name: "RandomForestClassifier",
      version: 1, champion_metric_name: null, champion_metric_value: null, is_fallback: false,
      created_at: "2026-01-01T00:00:00Z", training_dataset_version_id: "version-1",
      training_feature_columns: ["age", "tenure"],
    };
    vi.spyOn(globalThis, "fetch").mockImplementation((input, options) => {
      const url = String(input);
      if (url.endsWith("/operations/health")) return response(health);
      if (url.endsWith("/operations/registry")) return response([registryEntry]);
      if (url.endsWith("/operations/deployments") || url.endsWith("/operations/drift-runs")) return response([]);
      if (url.endsWith("/registry/registry-1/drift") && options?.method === "POST") {
        driftBody = JSON.parse(String(options.body));
        return response({ id: "drift-1", status: "queued" }, 202);
      }
      return response([]);
    });
    vi.spyOn(XMLHttpRequest.prototype, "open").mockImplementation(() => undefined);
    vi.spyOn(XMLHttpRequest.prototype, "setRequestHeader").mockImplementation(() => undefined);
    vi.spyOn(XMLHttpRequest.prototype, "send").mockImplementation(function (this: XMLHttpRequest) {
      this.upload.dispatchEvent(new ProgressEvent("progress", { lengthComputable: true, loaded: 100, total: 100 }));
      finishUpload = () => {
        Object.defineProperty(this, "status", { configurable: true, value: 201 });
        Object.defineProperty(this, "responseText", { configurable: true, value: JSON.stringify({
          dataset: { id: "drift-dataset", name: "Current customers" },
          version: { id: "drift-version", schema_json: { columns: [{ name: "age" }, { name: "tenure" }] } },
        }) });
        this.onload?.call(this, new ProgressEvent("load"));
      };
    });
    const user = userEvent.setup();
    const { container } = renderRoute(<OperationsPage />, "/projects/project-1/operations");

    await user.click(await screen.findByRole("button", { name: "Drift" }));
    const fileInput = container.querySelector<HTMLInputElement>('input[type="file"]');
    expect(fileInput).not.toBeNull();
    await user.upload(fileInput!, new File(["age,tenure\n30,4"], "current.csv", { type: "text/csv" }));
    await user.click(screen.getByRole("button", { name: "Upload and inspect" }));
    expect(await screen.findByText("100%")).toBeInTheDocument();
    act(() => finishUpload?.());
    expect(await screen.findByText("Schema matched: all 2 training features are present.")).toBeInTheDocument();
    await user.clear(screen.getByLabelText("Maximum rows"));
    await user.type(screen.getByLabelText("Maximum rows"), "5000");
    await user.click(screen.getByRole("button", { name: "Run drift check" }));
    await waitFor(() => expect(driftBody).toEqual({
      dataset_version_id: "drift-version", max_rows: 5000, expected_minutes: 10,
    }));
  });

  it("filters portfolio evidence and revisions the monitoring policy", async () => {
    let policyBody: unknown;
    const unmonitored = {
      ...monitoredDeployment, project_id: "project-2", project_name: "Fraud", deployment_run_id: "deploy-2",
      model_name: "IsolationForest", model_version: null, health_status: "unknown",
      monitoring: { ...monitoredDeployment.monitoring, enabled: false, schedule: "manual", revision: 1 },
      baseline_metric_name: null, baseline_metric_value: null, metric_series: [], drift_history: [],
      timeline: [], governance_reports: 0,
    };
    vi.spyOn(globalThis, "fetch").mockImplementation((input, options) => {
      const url = String(input);
      if (url.endsWith("/monitoring/config") && options?.method === "PUT") {
        policyBody = JSON.parse(String(options.body));
        return response({ revision: 4 });
      }
      if (url.endsWith("/monitoring/dashboard")) return response({
        scope: "portfolio", generated_at: "2026-08-12T09:00:00Z", deployment_count: 2,
        healthy_count: 1, attention_count: 0, unmonitored_count: 1, open_alert_count: 0,
        deployments: [monitoredDeployment, unmonitored],
      });
      return response([]);
    });
    const user = userEvent.setup();
    renderRoute(<MonitoringPage />, "/projects/project-1/monitoring");

    expect(await screen.findByText("Controls are operating normally")).toBeInTheDocument();
    expect(screen.getByRole("progressbar", { name: "Monitoring coverage" })).toHaveValue(50);
    expect(screen.getByRole("button", { name: /IsolationForest.*Monitoring disabled/i })).toBeInTheDocument();
    expect(screen.getByText("92.0%")).toBeInTheDocument();
    await user.selectOptions(screen.getByLabelText("Metric for RandomForestClassifier"), "drift_share");
    expect(screen.getByText("12.0%")).toBeInTheDocument();

    await user.selectOptions(screen.getByLabelText("Project"), "project-2");
    expect(screen.getByText("Awaiting production evidence")).toBeInTheDocument();
    expect(screen.getByText("No production series yet")).toBeInTheDocument();
    expect(screen.getByText("No deployment events have been recorded.")).toBeInTheDocument();
    await user.selectOptions(screen.getByLabelText("Health"), "healthy");
    expect(screen.getByText("No deployments match this view")).toBeInTheDocument();
    await user.selectOptions(screen.getByLabelText("Project"), "project-1");

    await user.click(screen.getByRole("button", { name: "Configure" }));
    const dialog = screen.getByRole("dialog", { name: "Configure deployment monitoring" });
    await user.selectOptions(within(dialog).getByLabelText("Evaluation cadence"), "weekly");
    await user.selectOptions(within(dialog).getByLabelText("Monitoring Job size"), "xlarge");
    await user.click(within(dialog).getByRole("checkbox", { name: /Allow retraining proposals/ }));
    await user.click(within(dialog).getByRole("button", { name: "Save monitoring policy" }));
    await waitFor(() => expect(policyBody).toEqual({
      enabled: true, schedule: "weekly", resource_class: "xlarge", metrics: ["accuracy"],
      thresholds: { accuracy: { warning: .85, critical: .75, direction: "below" } },
      retraining_enabled: true, approval_required: true,
    }));
  });

  it("opens, generates, and downloads immutable governance evidence", async () => {
    const download = vi.fn();
    const revoke = vi.fn();
    Object.defineProperty(URL, "createObjectURL", { configurable: true, value: () => "blob:report" });
    Object.defineProperty(URL, "revokeObjectURL", { configurable: true, value: revoke });
    vi.spyOn(HTMLAnchorElement.prototype, "click").mockImplementation(download);
    setSession({
      user: { id: "user-1", email: "owner@example.test", full_name: "Owner", global_role: "user",
        is_active: true, is_verified: true, created_at: "2026-01-01T00:00:00Z" },
      tokens: { access_token: "governance-token", refresh_token: "refresh", token_type: "bearer", expires_in: 3600 },
    });
    const summary = {
      id: "report-1", project_id: "project-1", deployment_run_id: "deploy-1",
      model_version_id: "model-v1", version: 1, generated_at: "2026-08-12T08:00:00Z",
      evidence_cutoff_at: "2026-08-12T07:59:00Z", generated_by_id: "user-1",
      content_hash: "abc123", json_download_url: "/governance/report-1.json",
      html_download_url: "/governance/report-1.html",
    };
    const report = { ...summary, report: {
      schema_version: "v1", model_development: { estimator: "RandomForestClassifier", approved: true },
      preprocessing: ["imputation", { scaling: null }], empty_evidence: [],
    } };
    vi.spyOn(globalThis, "fetch").mockImplementation((input, options) => {
      const url = String(input);
      if (url === "/governance/report-1.json" || url === "/governance/report-1.html") {
        expect(new Headers(options?.headers).get("Authorization")).toBe("Bearer governance-token");
        return Promise.resolve(new Response("report", { status: 200 }));
      }
      if (url.endsWith("/monitoring/dashboard")) return response({
        scope: "portfolio", generated_at: "2026-08-12T09:00:00Z", deployment_count: 1,
        healthy_count: 1, attention_count: 0, unmonitored_count: 0, open_alert_count: 0,
        deployments: [monitoredDeployment],
      });
      if (url.endsWith("/governance/reports") && options?.method === "POST") return response(report, 201);
      if (url.endsWith("/governance/reports")) return response([summary]);
      if (url.endsWith("/governance/reports/report-1")) return response(report);
      return response([]);
    });
    const user = userEvent.setup();
    renderRoute(<MonitoringPage />, "/projects/project-1/monitoring");

    await user.click(await screen.findByRole("button", { name: /Governance & audit/ }));
    await user.click(await screen.findByRole("button", { name: /Version 1/ }));
    expect(await screen.findByText("SHA-256 abc123")).toBeInTheDocument();
    expect(screen.getByText("RandomForestClassifier", { selector: "span" })).toBeInTheDocument();
    expect(screen.getByText("Not recorded")).toBeInTheDocument();
    expect(screen.getByText("No evidence recorded")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "JSON" }));
    await waitFor(() => expect(download).toHaveBeenCalled());
    expect(revoke).toHaveBeenCalledWith("blob:report");

    await user.click(screen.getByRole("button", { name: /Generate snapshot/ }));
    expect(await screen.findByText("SHA-256 abc123")).toBeInTheDocument();
  });
});
