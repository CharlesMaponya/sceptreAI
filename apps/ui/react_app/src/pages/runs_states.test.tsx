import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Route, Routes } from "react-router";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { setSession } from "../api";
import { RunsPage } from "./RunsPage";

vi.mock("../components/PlotlyChart", () => ({
  default: () => <div data-testid="evidence-chart" />,
}));

const response = (data: unknown, status = 200) => Promise.resolve(new Response(JSON.stringify(data), {
  status, headers: { "Content-Type": "application/json" },
}));

function renderRuns() {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  });
  return render(<QueryClientProvider client={client}>
    <MemoryRouter initialEntries={["/projects/project-1/runs"]}>
      <Routes><Route path="/projects/:projectId/runs" element={<RunsPage />} /></Routes>
    </MemoryRouter>
  </QueryClientProvider>);
}

const run = {
  id: "run-1", dataset_version_id: "version-1", run_kind: "training", status: "succeeded",
  task_type: "classification", target_column: "churned", run_name: "retention-v1",
  params: {}, plain_english_failure: null, failure_message: null,
  created_at: "2026-01-01T00:00:00Z", finished_at: "2026-01-01T00:10:00Z",
};

const resources = {
  run_id: "run-1", status: "succeeded", completed_candidates: 1, total_candidates: 1,
  progress: 1, elapsed_seconds: 9, estimated_remaining_seconds: null,
  current_candidate: null, last_candidate: null, current_phase: null, pod_phase: "Succeeded",
  cpu_usage_cores: null, cpu_limit_cores: null, peak_cpu_usage_cores: null,
  memory_usage_mb: null, memory_limit_mb: null, peak_memory_usage_mb: null,
  gpu_count: 0, gpu_vendor: null, gpu_resource: null, gpu_utilization_percent: null,
  gpu_telemetry_available: false, telemetry_available: false, pod_name: null,
  node_name: null, restart_count: 0,
};

describe("run evidence qualification states", () => {
  beforeEach(() => {
    setSession(null);
    vi.restoreAllMocks();
  });

  it("recovers the run collection and renders an empty workspace", async () => {
    let fail = true;
    vi.spyOn(globalThis, "fetch").mockImplementation(() => fail
      ? response({ detail: "Run history unavailable" }, 503)
      : response([]));

    renderRuns();
    expect(await screen.findByText("Run history unavailable")).toBeInTheDocument();
    fail = false;
    await userEvent.click(screen.getByRole("button", { name: /try again/i }));
    expect(await screen.findByText("No experiment results yet")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: /Configure training/ })).toHaveAttribute(
      "href", "/projects/project-1/training",
    );
  });

  it("restarts a failed run and preserves both operator-facing failure messages", async () => {
    let restarted = false;
    vi.spyOn(globalThis, "fetch").mockImplementation((input) => {
      const url = String(input);
      if (url.endsWith("/restart")) {
        restarted = true;
        return response({ ...run, status: "queued" }, 202);
      }
      if (url.endsWith("/training/runs")) return response([{
        ...run, status: restarted ? "queued" : "failed",
        plain_english_failure: "The worker ran out of memory.",
        failure_message: "OOMKilled: container limit exceeded", finished_at: null,
      }]);
      if (url.endsWith("/leaderboard")) return response({
        run_id: "run-1", status: "failed", primary_metric: null,
        winner: null, metric_directions: {}, entries: [],
      });
      if (url.endsWith("/resources")) return response({ ...resources, status: "failed" });
      return response([]);
    });

    renderRuns();
    expect(await screen.findByText("The worker ran out of memory.")).toBeInTheDocument();
    expect(screen.getByText("OOMKilled: container limit exceeded")).toBeInTheDocument();
    expect(await screen.findByText("Results are on their way")).toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: "Restart run" }));
    await waitFor(() => expect(restarted).toBe(true));
    expect(await screen.findByRole("button", { name: "Cancel run" })).toBeInTheDocument();
  });

  it("renders unavailable telemetry and multiclass evidence fallbacks", async () => {
    vi.spyOn(globalThis, "fetch").mockImplementation((input) => {
      const url = String(input);
      if (url.endsWith("/training/runs")) return response([run]);
      if (url.endsWith("/leaderboard")) return response({
        run_id: "run-1", status: "succeeded", primary_metric: "f1_macro",
        winner: null, metric_directions: {}, entries: [{
          rank: 0, model: "LinearSVC", status: "succeeded", cost_tier: "low",
          primary_score: 0, metrics: { f1_macro: 0 }, best_params: {}, duration_seconds: 0,
          error: "A non-fatal convergence warning was recorded.", pipeline: null,
          diagnostics: {
            labels: ["low", "medium", "high"],
            confusion_matrix: [[0, 0, 0], [1, 2, 0], [0, 1, 3]],
            classification_report: {
              low: { precision: 0, recall: 0, "f1-score": 0 },
              medium: { precision: .66, recall: .66, "f1-score": .66 },
              accuracy: .71,
            },
          },
        }],
      });
      if (url.endsWith("/resources")) return response(resources);
      return response([]);
    });
    const user = userEvent.setup();
    renderRuns();

    await user.click(await screen.findByRole("button", { name: /LinearSVC/ }));
    expect(screen.getByText("A non-fatal convergence warning was recorded.")).toBeInTheDocument();
    expect(screen.getByText("Results are still being collected")).toBeInTheDocument();
    await user.click(screen.getByRole("tab", { name: "Diagnostics" }));
    expect(await screen.findByRole("heading", { name: "Per-class quality" })).toBeInTheDocument();
    await user.click(screen.getByRole("tab", { name: "Resources" }));
    expect(screen.getAllByText("Unavailable").length).toBeGreaterThanOrEqual(2);
    expect(screen.getByText(/Metrics Server/)).toBeInTheDocument();
    expect(screen.getByText("Expired")).toBeInTheDocument();
    expect(screen.getByText("Estimating…")).toBeInTheDocument();
  });

  it("recovers leaderboard and resource queries independently", async () => {
    let leaderboardFails = true;
    let resourceFails = true;
    vi.spyOn(globalThis, "fetch").mockImplementation((input) => {
      const url = String(input);
      if (url.endsWith("/training/runs")) return response([run]);
      if (url.endsWith("/leaderboard")) return leaderboardFails
        ? response({ detail: "Leaderboard unavailable" }, 503)
        : response({ run_id: "run-1", status: "succeeded", entries: [], metric_directions: {} });
      if (url.endsWith("/resources")) return resourceFails
        ? response({ detail: "Telemetry unavailable" }, 503)
        : response(resources);
      return response([]);
    });

    renderRuns();
    expect(await screen.findByText("Telemetry unavailable")).toBeInTheDocument();
    resourceFails = false;
    await userEvent.click(screen.getByText("Telemetry unavailable").closest("div")!
      .querySelector<HTMLButtonElement>("button")!);
    await waitFor(() => expect(screen.queryByText("Telemetry unavailable")).not.toBeInTheDocument());
    expect(screen.getByText("No model active")).toBeInTheDocument();
    expect(screen.getByText("Leaderboard unavailable")).toBeInTheDocument();
    leaderboardFails = false;
    await userEvent.click(screen.getByText("Leaderboard unavailable").closest("div")!
      .querySelector<HTMLButtonElement>("button")!);
    expect(await screen.findByText("Results are on their way")).toBeInTheDocument();
  });

  it("shows missing correlation evidence and a fully completed estimator catalog", async () => {
    vi.spyOn(globalThis, "fetch").mockImplementation((input) => {
      const url = String(input);
      if (url.endsWith("/training/runs")) return response([run]);
      if (url.endsWith("/leaderboard")) return response({
        run_id: "run-1", status: "succeeded", primary_metric: "accuracy",
        winner: "LogisticRegression", metric_directions: {}, entries: [{
          rank: 1, model: "LogisticRegression", status: "succeeded", cost_tier: "low",
          primary_score: .8, metrics: {}, diagnostics: {}, best_params: {},
          duration_seconds: null, error: null,
        }],
      });
      if (url.endsWith("/resources")) return response(resources);
      if (url.includes("/training/estimators")) return response([{
        name: "LogisticRegression", task_type: "classification", cost_tier: "low",
        default_selected: true, tunable: false,
      }]);
      return response([]);
    });
    const user = userEvent.setup();
    renderRuns();

    await user.click(await screen.findByRole("tab", { name: "Feature selection" }));
    expect(screen.getByText(/Correlation heatmaps are unavailable/)).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Add models" }));
    const dialog = await screen.findByRole("dialog", { name: "Train additional models" });
    expect(within(dialog).getByText("Every compatible model is already included")).toBeInTheDocument();
  });

  it("recovers log polling and blocks analysis when no model succeeded", async () => {
    let logsFail = true;
    vi.spyOn(globalThis, "fetch").mockImplementation((input) => {
      const url = String(input);
      if (url.endsWith("/training/runs")) return response([run]);
      if (url.endsWith("/leaderboard")) return response({
        run_id: "run-1", status: "failed", primary_metric: "accuracy", winner: null,
        metric_directions: {}, entries: [{
          rank: null, model: "BrokenModel", status: "failed", cost_tier: "low",
          primary_score: null, metrics: {}, diagnostics: {}, best_params: {},
          duration_seconds: null, error: "Fit failed",
        }],
      });
      if (url.endsWith("/resources")) return response(resources);
      if (url.endsWith("/logs")) return logsFail
        ? response({ detail: "Logs unavailable" }, 503)
        : response({ run_id: "run-1", status: "failed", lines: [] });
      return response([]);
    });
    const user = userEvent.setup();
    renderRuns();

    await user.click(await screen.findByRole("tab", { name: "Validate & explain" }));
    expect(screen.getByText("Successful model candidates are required before analysis."))
      .toBeInTheDocument();
    await user.click(screen.getByRole("tab", { name: "Logs" }));
    expect(await screen.findByText("Logs unavailable")).toBeInTheDocument();
    logsFail = false;
    await user.click(screen.getByRole("button", { name: /try again/i }));
    expect(await screen.findByLabelText("Training logs")).toHaveTextContent("No log output yet.");
  });
});
