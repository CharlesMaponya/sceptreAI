import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { setSession } from "../api";
import { OperationsPage } from "./OperationsPage";
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
          primary_score: .91, metrics: { balanced_accuracy: .91 }, diagnostics: {},
          best_params: { n_estimators: 100 }, duration_seconds: 34, error: null,
        }],
      });
      if (url.endsWith("/logs")) return response({
        run_id: "run-1", status: "succeeded", lines: ["training started", "candidate completed"],
      });
      return response([]);
    });
    renderRoute(<RunsPage />, "/projects/project-1/runs");
    expect((await screen.findAllByText("RandomForestClassifier")).length).toBeGreaterThan(0);
    await userEvent.click(screen.getByRole("tab", { name: "Logs" }));
    expect(await screen.findByLabelText("Training logs")).toHaveTextContent("candidate completed");
    expect(fetchMock.mock.calls.some(([url]) => String(url).endsWith("/logs"))).toBe(true);
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
