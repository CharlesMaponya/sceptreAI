import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { setSession } from "../api";
import { DataPage } from "./DataPage";
import { OperationsPage } from "./OperationsPage";
import { ProjectOverview } from "./ProjectOverview";
import { RunsPage } from "./RunsPage";

const response = (data: unknown, status = 200) => Promise.resolve(new Response(JSON.stringify(data), {
  status, headers: { "Content-Type": "application/json" },
}));

function renderRoute(element: React.ReactNode, path: string) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  return render(<QueryClientProvider client={client}><MemoryRouter initialEntries={[path]}>
    <Routes><Route path="/projects/:projectId/*" element={element} /></Routes>
  </MemoryRouter></QueryClientProvider>);
}

const run = {
  id: "run-1", dataset_version_id: "version-1", run_kind: "training", status: "succeeded",
  task_type: "classification", target_column: "churned", run_name: "retention-v1",
  cpu_request_cores: 1, memory_request_mb: 1024, params: {}, plain_english_failure: null,
  failure_message: null, created_at: "2026-01-01T00:00:00Z", finished_at: "2026-01-01T00:10:00Z",
};

describe("core workflow integrations", () => {
  beforeEach(() => {
    setSession(null);
    vi.restoreAllMocks();
  });

  it("renders progressive run evidence and fetches logs on demand", async () => {
    const fetchMock = vi.spyOn(globalThis, "fetch").mockImplementation((input) => {
      const url = String(input);
      if (url.endsWith("/training/runs")) return response([run]);
      if (url.endsWith("/leaderboard")) return response({
        run_id: "run-1", status: "succeeded", primary_metric: "balanced_accuracy",
        winner: "RandomForestClassifier", metric_directions: { balanced_accuracy: "maximize" },
        entries: [{
          rank: 1, model: "RandomForestClassifier", status: "succeeded", cost_tier: "medium",
          primary_score: .91, metrics: { balanced_accuracy: .91, roc_auc: .94 }, diagnostics: {
            labels: ["no", "yes"], confusion_matrix: [[18, 2], [3, 17]],
            roc_curves: [{ label: "yes", points: [
              { false_positive_rate: 0, true_positive_rate: 0 },
              { false_positive_rate: .1, true_positive_rate: .85 },
              { false_positive_rate: 1, true_positive_rate: 1 },
            ] }],
            precision_recall_curves: [{ label: "yes", points: [
              { recall: 0, precision: 1 }, { recall: 1, precision: .5 },
            ] }],
            learning_curve: { scoring: "balanced_accuracy", points: [
              { training_rows: 20, training_mean: .96, validation_mean: .82 },
              { training_rows: 80, training_mean: .92, validation_mean: .9 },
            ] },
          },
          best_params: { n_estimators: 100 }, duration_seconds: 34, error: null,
        }],
      });
      if (url.endsWith("/logs")) return response({
        run_id: "run-1", status: "succeeded", lines: ["training started", "candidate completed"],
      });
      if (url.endsWith("/resources")) return response({
        run_id: "run-1", status: "succeeded", pod_name: "automl-run-1", pod_phase: "Succeeded",
        node_name: "worker-1", current_candidate: null, current_phase: "complete",
        completed_candidates: 1, total_candidates: 1, progress: 1, elapsed_seconds: 600,
        estimated_remaining_seconds: 0, cpu_request_cores: 1, cpu_limit_cores: 2,
        cpu_usage_cores: .8, peak_cpu_usage_cores: 1.4, memory_request_mb: 1024,
        memory_limit_mb: 2048, memory_usage_mb: 900, peak_memory_usage_mb: 1400,
        gpu_requested: true, gpu_vendor: "nvidia", gpu_resource: "nvidia.com/gpu", gpu_count: 1,
        gpu_utilization_percent: null, gpu_memory_used_mb: null, gpu_memory_total_mb: null,
        gpu_telemetry_available: false, telemetry_available: true, restart_count: 0,
        status_reason: null, sampled_at: "2026-01-01T00:10:00Z",
      });
      return response([]);
    });
    renderRoute(<RunsPage />, "/projects/project-1/runs");
    expect((await screen.findAllByText("RandomForestClassifier")).length).toBeGreaterThan(0);
    await userEvent.click(screen.getByRole("button", { name: /RandomForestClassifier/i }));
    await userEvent.click(screen.getByRole("tab", { name: "Diagnostics" }));
    expect(await screen.findByRole("heading", { name: "Confusion matrix" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "ROC curve" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: /Learning curve/i })).toBeInTheDocument();
    await userEvent.click(screen.getByRole("tab", { name: "Resources" }));
    expect(screen.getByText("0.80 cores")).toBeInTheDocument();
    expect(screen.getByText("Nvidia × 1")).toBeInTheDocument();
    const logTabs = screen.getAllByRole("tab", { name: "Logs" });
    await userEvent.click(logTabs[logTabs.length - 1]);
    expect(await screen.findByLabelText("Training logs")).toHaveTextContent("candidate completed");
    expect(fetchMock.mock.calls.some(([url]) => String(url).endsWith("/logs"))).toBe(true);
  });

  it("uploads first, then waits for target selection before profiling", async () => {
    let uploadBody: unknown = null;
    let profileBody: unknown = null;
    let finishUpload: (() => void) | undefined;
    const fetchMock = vi.spyOn(globalThis, "fetch").mockImplementation((input, options) => {
      if (String(input).endsWith("/profile-jobs") && options?.method === "POST") {
        profileBody = JSON.parse(String(options.body));
        return response({ id: "profile-1", status: "queued" }, 202);
      }
      return response([]);
    });
    vi.spyOn(XMLHttpRequest.prototype, "open").mockImplementation(() => undefined);
    vi.spyOn(XMLHttpRequest.prototype, "send").mockImplementation(function (
      this: XMLHttpRequest,
      body?: Document | XMLHttpRequestBodyInit | null,
    ) {
      uploadBody = body ?? null;
      this.upload.dispatchEvent(new ProgressEvent("progress", {
        lengthComputable: true, loaded: 50, total: 100,
      }));
      finishUpload = () => {
        Object.defineProperty(this, "status", { configurable: true, value: 201 });
        Object.defineProperty(this, "responseText", { configurable: true, value: JSON.stringify({
          dataset: { id: "dataset-1", name: "Customer activity", latest_version_number: 1 },
          version: {
            id: "version-1", dataset_id: "dataset-1", version_number: 1,
            schema_json: { columns: [{ name: "id" }, { name: "value" }] },
          },
        }) });
        this.onload?.call(this, new ProgressEvent("load"));
      };
    });
    const user = userEvent.setup();
    const { container } = renderRoute(<DataPage />, "/projects/project-1/data");
    await screen.findByText("Your model starts with trusted data");
    await user.click(screen.getByRole("button", { name: "Upload dataset" }));
    await user.type(screen.getByLabelText("Dataset name"), "Customer activity");
    const fileInput = container.querySelector<HTMLInputElement>('input[type="file"]');
    expect(fileInput).not.toBeNull();
    await user.upload(fileInput!, new File(["id,value\n1,42\n"], "customers.csv", {
      type: "text/csv",
    }));
    await user.click(screen.getAllByRole("button", { name: "Upload dataset" }).at(-1)!);

    expect(await screen.findByText("50%")).toBeInTheDocument();
    expect(screen.getByRole("progressbar", { name: "Dataset upload progress" }))
      .toHaveValue(50);
    await waitFor(() => expect(uploadBody).toBeInstanceOf(FormData));
    if (!(uploadBody instanceof FormData)) throw new Error("Expected multipart upload body.");
    const form = uploadBody;
    expect(form.get("dataset_name")).toBe("Customer activity");
    expect(form.get("description")).toBe("");
    expect(form.get("tags")).toBe("{}");
    expect(form.get("file")).toBeInstanceOf(File);
    expect((form.get("file") as File).name).toBe("customers.csv");
    act(() => finishUpload?.());
    expect(await screen.findByRole("heading", { name: "Choose the profiling target" }))
      .toBeInTheDocument();
    expect(screen.getByRole("option", { name: "No target" })).toBeInTheDocument();
    expect(screen.getByRole("option", { name: "value" })).toBeInTheDocument();
    expect(fetchMock.mock.calls.some(([url]) => String(url).endsWith("/profile-jobs"))).toBe(false);

    await user.selectOptions(screen.getByLabelText("Target column"), "value");
    await user.click(screen.getByRole("button", { name: "Start profile" }));
    await waitFor(() => expect(profileBody).toEqual({ target_column: "value", force: false }));
    await waitFor(() => expect(screen.queryByRole("heading", {
      name: "Choose the profiling target",
    })).not.toBeInTheDocument());
  });

  it("profiles only on request and shows regression target guidance", async () => {
    let latestProfile: Record<string, unknown> | null = null;
    let profileBody: unknown = null;
    const fetchMock = vi.spyOn(globalThis, "fetch").mockImplementation((input, options) => {
      const url = String(input);
      if (url.endsWith("/projects/project-1")) return response({
        id: "project-1", name: "Retention", description: "Customer retention models",
        updated_at: "2026-01-01T00:00:00Z",
      });
      if (url.endsWith("/training/runs")) return response([]);
      if (url.endsWith("/projects/project-1/datasets")) return response([{
        id: "dataset-1", name: "Customers", latest_version_number: 1,
        created_at: "2026-01-01T00:00:00Z",
      }]);
      if (url.endsWith("/datasets/dataset-1/versions")) return response([{
        id: "version-1", dataset_id: "dataset-1", version_number: 1,
        schema_json: { columns: [
          { name: "age", semantic_type: "numerical_discrete" },
          {
            name: "revenue", semantic_type: "numerical_continuous",
            preview_kind: "histogram", preview_values: [10, 18, 25, 42.5, 58, 71, 100],
            preview_distribution: [], statistics: { min: 10, median: 42.5, mean: 46.36, max: 100 },
          },
        ] },
      }]);
      if (url.endsWith("/profile-jobs/profile-1/result")) return response({
        id: "profile-1", feature_profiles_json: { revenue: {
          name: "revenue", semantic_type: "numerical_continuous",
          statistics: { min: 10, median: 42.5, mean: 48.25, max: 100 },
          distribution: [
            { label: "10 - 40", count: 12 },
            { label: "40 - 70", count: 25 },
            { label: "70 - 100", count: 8 },
          ],
        } },
      });
      if (url.endsWith("/profile-jobs/latest")) return response(latestProfile);
      if (url.endsWith("/profile-jobs") && options?.method === "POST") {
        profileBody = JSON.parse(String(options.body));
        latestProfile = {
          id: "profile-1", status: "succeeded", current_stage: "completed", progress: 1,
          target_column: "revenue", overview_json: { task_inference: {
            task_type: "regression", confidence: .91,
            rationale: "The selected target is continuous numeric.",
          } },
        };
        return response(latestProfile, 202);
      }
      return response([]);
    });
    const user = userEvent.setup();
    renderRoute(<ProjectOverview />, "/projects/project-1");

    expect(await screen.findByRole("heading", { name: "Choose what you want to predict" }))
      .toBeInTheDocument();
    expect(screen.getByRole("option", { name: "No target" })).toBeInTheDocument();
    expect(await screen.findByRole("option", { name: "revenue" })).toBeInTheDocument();
    expect(fetchMock.mock.calls.some(([url, options]) =>
      String(url).endsWith("/profile-jobs") && options?.method === "POST")).toBe(false);

    await user.selectOptions(screen.getByLabelText("Target column"), "revenue");
    expect(await screen.findByRole("heading", { name: "Regression task preview" }))
      .toBeInTheDocument();
    expect(screen.getByText("Instant target preview")).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Regression target distribution" }))
      .toBeInTheDocument();
    expect(profileBody).toBeNull();
    await user.click(screen.getByRole("button", { name: "Start profile" }));
    await waitFor(() => expect(profileBody).toEqual({ target_column: "revenue", force: false }));
    expect(await screen.findByRole("heading", { name: "Regression task identified" }))
      .toBeInTheDocument();
    expect(screen.getByText("Profile confidence: 91%.")).toBeInTheDocument();
    expect(await screen.findByRole("heading", { name: "Regression target distribution" }))
      .toBeInTheDocument();
    expect(screen.getByText("42.5")).toBeInTheDocument();
    expect(screen.getByText(/Nothing will launch until you explicitly confirm it/i))
      .toBeInTheDocument();
    expect(screen.getByRole("link", { name: /Configure training/i }))
      .toHaveAttribute("href", "/projects/project-1/training");
  });

  it("previews cleanup before enabling destructive execution", async () => {
    const fetchMock = vi.spyOn(globalThis, "fetch").mockImplementation((input, options) => {
      const url = String(input);
      if (url.endsWith("/operations/health")) return response({
        capacity: {
          connected: true, source: "kubernetes", available_cpu_cores: 4,
          available_memory_mb: 8192, ready_nodes: 1, gpu_available: false,
          active_training_jobs: 0, warnings: [],
        },
        active_deployments: 0, components: { database: "ok" },
      });
      if (url.endsWith("/operations/registry")) return response([]);
      if (url.endsWith("/operations/deployments")) return response([]);
      if (url.endsWith("/operations/drift-runs")) return response([]);
      if (url.endsWith("/datasets")) return response([]);
      if (url.endsWith("/operations/cleanup")) {
        const body = JSON.parse(String(options?.body));
        expect(body).toMatchObject({ older_than_days: 30, dry_run: true, cleanup_finished_jobs: true });
        return response({
          dry_run: true, artifact_count: 3, artifact_bytes: 2048, artifact_ids: ["1", "2", "3"],
          deleted_object_uris: [], deleted_kubernetes_jobs: [], errors: [],
        });
      }
      return response([]);
    });
    renderRoute(<OperationsPage />, "/projects/project-1/operations");
    await screen.findByText("Resource cleanup");
    const deleteButton = screen.getByRole("button", { name: /Delete eligible resources/i });
    expect(deleteButton).toBeDisabled();
    await userEvent.click(screen.getByRole("button", { name: "Preview cleanup" }));
    await waitFor(() => expect(deleteButton).toBeEnabled());
    expect(screen.getByText("3", { selector: "strong" })).toBeInTheDocument();
    expect(fetchMock.mock.calls.some(([url]) => String(url).endsWith("/operations/cleanup"))).toBe(true);
  });
});
