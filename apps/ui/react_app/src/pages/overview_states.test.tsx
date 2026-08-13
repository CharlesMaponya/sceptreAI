import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Route, Routes } from "react-router";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { setSession } from "../api";
import { ProjectOverview } from "./ProjectOverview";

vi.mock("../components/PlotlyChart", () => ({
  default: () => <div data-testid="target-chart" />,
}));

const response = (data: unknown, status = 200) => Promise.resolve(new Response(JSON.stringify(data), {
  status, headers: { "Content-Type": "application/json" },
}));

const project = {
  id: "project-1", name: "Risk lab", description: "Governed models",
  updated_at: "2026-01-01T00:00:00Z",
};

function renderOverview() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(<QueryClientProvider client={client}>
    <MemoryRouter initialEntries={["/projects/project-1"]}>
      <Routes><Route path="/projects/:projectId" element={<ProjectOverview />} /></Routes>
    </MemoryRouter>
  </QueryClientProvider>);
}

describe("project overview qualification states", () => {
  beforeEach(() => {
    setSession(null);
    vi.restoreAllMocks();
  });

  it("guides an empty project to its first immutable dataset", async () => {
    vi.spyOn(globalThis, "fetch").mockImplementation((input) => {
      const url = String(input);
      if (url.endsWith("/projects/project-1")) return response(project);
      return response([]);
    });

    renderOverview();

    expect(await screen.findByRole("heading", { name: "Bring in your first dataset" }))
      .toBeInTheDocument();
    expect(screen.getByRole("link", { name: /Upload data/ })).toHaveAttribute(
      "href", "/projects/project-1/data",
    );
    expect(screen.getByText("No training runs yet")).toBeInTheDocument();
  });

  it("shows active temporal profiling progress without allowing target changes", async () => {
    vi.spyOn(globalThis, "fetch").mockImplementation((input) => {
      const url = String(input);
      if (url.endsWith("/projects/project-1")) return response(project);
      if (url.endsWith("/training/runs")) return response([{
        id: "run-active", run_name: "temporal-active", task_type: "time_series", status: "running",
        created_at: "2026-01-02T00:00:00Z",
      }]);
      if (url.endsWith("/projects/project-1/datasets")) return response([{
        id: "dataset-1", name: "Events", latest_version_number: 1,
      }]);
      if (url.endsWith("/datasets/dataset-1/versions")) return response([{
        id: "version-1", version_number: 1, schema_json: { columns: [{
          name: "observed_at", semantic_type: "temporal",
          sample_values: ["2026-01-01", "2026-02-01"],
          statistics: { min: "2026-01-01", max: "2026-02-01" },
        }] },
      }]);
      if (url.endsWith("/profile-jobs/latest")) return response({
        id: "profile-1", status: "running", current_stage: "feature statistics",
        progress: .42, target_column: "observed_at",
      });
      return response([]);
    });

    renderOverview();

    expect(await screen.findByRole("heading", { name: "Profiling your dataset" })).toBeInTheDocument();
    expect(screen.getByLabelText("Target column")).toBeDisabled();
    expect(screen.getByText("42%")).toBeInTheDocument();
    expect(screen.getAllByText("Time Series").length).toBeGreaterThan(0);
    expect(screen.getByText("2026-01-01")).toBeInTheDocument();
    expect(screen.getByText("2026-02-01")).toBeInTheDocument();
  });

  it("surfaces a failed profile and permits an explicit retry", async () => {
    let body: unknown;
    vi.spyOn(globalThis, "fetch").mockImplementation((input, options) => {
      const url = String(input);
      if (url.endsWith("/projects/project-1")) return response(project);
      if (url.endsWith("/training/runs")) return response([]);
      if (url.endsWith("/projects/project-1/datasets")) return response([{
        id: "dataset-1", name: "Customers", latest_version_number: 1,
      }]);
      if (url.endsWith("/datasets/dataset-1/versions")) return response([{
        id: "version-1", version_number: 1, schema_json: { columns: [{
          name: "churned", semantic_type: "categorical", sample_values: ["yes", "no", "no"],
        }] },
      }]);
      if (url.endsWith("/profile-jobs/latest")) return response({
        id: "profile-1", status: "failed", target_column: "churned",
        failure_message: "Worker capacity was reclaimed.",
      });
      if (url.endsWith("/profile-jobs") && options?.method === "POST") {
        body = JSON.parse(String(options.body));
        return response({ id: "profile-2", status: "queued", target_column: "churned" }, 202);
      }
      return response([]);
    });

    renderOverview();

    expect(await screen.findByText("Worker capacity was reclaimed.")).toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: "Start profile" }));
    await waitFor(() => expect(body).toEqual({ target_column: "churned", force: false }));
  });

  it("renders completed classification evidence and leakage exclusions", async () => {
    vi.spyOn(globalThis, "fetch").mockImplementation((input) => {
      const url = String(input);
      if (url.endsWith("/projects/project-1")) return response(project);
      if (url.endsWith("/training/runs")) return response([{
        id: "run-1", run_name: null, task_type: "classification", status: "succeeded",
        created_at: "2026-01-02T00:00:00Z",
      }]);
      if (url.endsWith("/projects/project-1/datasets")) return response([{
        id: "dataset-1", name: "Customers", latest_version_number: 1,
      }]);
      if (url.endsWith("/datasets/dataset-1/versions")) return response([{
        id: "version-1", version_number: 1, schema_json: { columns: [{
          name: "churned", semantic_type: "categorical", sample_values: ["yes", "no"],
        }] },
      }]);
      if (url.endsWith("/profile-jobs/latest")) return response({
        id: "profile-1", status: "succeeded", target_column: "churned",
        overview_json: { task_inference: {
          task_type: "classification", confidence: .96, rationale: "A categorical target was selected.",
        } },
      });
      if (url.endsWith("/profile-jobs/profile-1/result")) return response({
        overview_json: { leakage_analysis: { excluded_columns: ["post_outcome_code"] } },
        feature_profiles_json: { churned: {
          name: "churned", semantic_type: "categorical", statistics: {},
          distribution: [{ label: "yes", count: 20 }, { label: "no", count: 80 }],
        } },
      });
      return response([]);
    });

    renderOverview();

    expect(await screen.findByRole("heading", { name: "Classification task identified" }))
      .toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Class balance" })).toBeInTheDocument();
    expect(await screen.findByText(/post_outcome_code/)).toBeInTheDocument();
    expect(screen.getByText("run-1")).toBeInTheDocument();
  });

  it("handles a dataset with no usable version", async () => {
    vi.spyOn(globalThis, "fetch").mockImplementation((input) => {
      const url = String(input);
      if (url.endsWith("/projects/project-1")) return response(project);
      if (url.endsWith("/training/runs")) return response([]);
      if (url.endsWith("/projects/project-1/datasets")) return response([{
        id: "dataset-1", name: "Empty source", latest_version_number: null,
      }]);
      return response([]);
    });

    renderOverview();

    await screen.findByText("The selected dataset has no available version.");
    expect(screen.getByLabelText("Target column")).toBeDisabled();
  });
});
