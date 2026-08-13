import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Route, Routes } from "react-router";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { setSession } from "../api";
import { TrainingPage } from "./TrainingPage";

const response = (data: unknown, status = 200) => Promise.resolve(new Response(JSON.stringify(data), {
  status, headers: { "Content-Type": "application/json" },
}));

function renderTraining() {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  });
  return render(<QueryClientProvider client={client}>
    <MemoryRouter initialEntries={["/projects/project-1/training"]}>
      <Routes>
        <Route path="/projects/:projectId/training" element={<TrainingPage />} />
        <Route path="/projects/:projectId/data" element={<h1>Data workspace</h1>} />
        <Route path="/projects/:projectId" element={<h1>Project overview</h1>} />
      </Routes>
    </MemoryRouter>
  </QueryClientProvider>);
}

const dataset = { id: "dataset-1", name: "Customers", latest_version_number: 1 };
const version = {
  id: "version-1", dataset_id: "dataset-1", version_number: 1, status: "ready",
  schema_json: { columns: [{ name: "age" }, { name: "churned" }] },
};

describe("training qualification states", () => {
  beforeEach(() => {
    setSession(null);
    vi.restoreAllMocks();
  });

  it("recovers the dataset prerequisite query and routes an empty project to data", async () => {
    let attempts = 0;
    vi.spyOn(globalThis, "fetch").mockImplementation(() => {
      attempts += 1;
      return attempts === 1 ? response({ detail: "Dataset lookup failed" }, 503) : response([]);
    });

    renderTraining();
    expect(await screen.findByText("Dataset lookup failed")).toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: /try again/i }));
    expect(await screen.findByRole("heading", { name: "Data comes first" })).toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: /Go to data/ }));
    expect(await screen.findByRole("heading", { name: "Data workspace" })).toBeInTheDocument();
  });

  it("recovers a version query and handles a dataset without versions", async () => {
    let failVersions = true;
    vi.spyOn(globalThis, "fetch").mockImplementation((input) => {
      const url = String(input);
      if (url.endsWith("/datasets")) return response([dataset]);
      return failVersions ? response({ detail: "Versions unavailable" }, 503) : response([]);
    });

    renderTraining();
    expect(await screen.findByText("Versions unavailable")).toBeInTheDocument();
    failVersions = false;
    await userEvent.click(screen.getByRole("button", { name: /try again/i }));
    expect(await screen.findByRole("heading", { name: "No dataset version is available" }))
      .toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: /Go to data/ }));
    expect(await screen.findByRole("heading", { name: "Data workspace" })).toBeInTheDocument();
  });

  it("recovers a profile query and identifies active profiling", async () => {
    let profileAttempts = 0;
    vi.spyOn(globalThis, "fetch").mockImplementation((input) => {
      const url = String(input);
      if (url.endsWith("/datasets")) return response([dataset]);
      if (url.endsWith("/versions")) return response([version]);
      if (url.endsWith("/profile-jobs/latest")) {
        profileAttempts += 1;
        return profileAttempts === 1
          ? response({ detail: "Profile status unavailable" }, 503)
          : response({ id: "profile-1", status: "running", target_column: "churned" });
      }
      return response([]);
    });

    renderTraining();
    expect(await screen.findByText("Profile status unavailable")).toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: /try again/i }));
    expect(await screen.findByText("Profiling is still running. Return when it completes."))
      .toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: /Go to profile setup/ }));
    expect(await screen.findByRole("heading", { name: "Project overview" })).toBeInTheDocument();
  });

  it("reframes tasks, toggles models, and displays a blocked estimate", async () => {
    let estimateBody: Record<string, unknown> | undefined;
    vi.spyOn(globalThis, "fetch").mockImplementation((input, options) => {
      const url = String(input);
      if (url.endsWith("/datasets")) return response([
        dataset, { ...dataset, id: "dataset-2", name: "Transactions" },
      ]);
      if (url.includes("/datasets/dataset-2/versions")) return response([{
        ...version, id: "version-2", dataset_id: "dataset-2", version_number: 2,
      }]);
      if (url.endsWith("/versions")) return response([version]);
      if (url.endsWith("/profile-jobs/latest")) return response({
        id: "profile-1", status: "succeeded", target_column: "churned",
        overview_json: { task_inference: { task_type: "classification" } },
      });
      if (url.includes("/training/estimators")) return response([{
        name: "LogisticRegression", task_type: "classification", tunable: false,
        cost_tier: "low", default_selected: true,
      }, {
        name: "RandomForestClassifier", task_type: "classification", tunable: true,
        cost_tier: "medium", default_selected: false,
      }]);
      if (url.endsWith("/training/estimate") && options?.method === "POST") {
        estimateBody = JSON.parse(String(options.body));
        return response({
          cpu_request_cores: 8, memory_request_mb: 16384, gpu_requested: false,
          gpu_vendor: null, selected_node: null, estimated_core_hours: 2,
          capacity: { available_cpu_cores: 2 }, warnings: [],
          blockers: ["Insufficient qualified CPU capacity."], can_launch: false,
        });
      }
      return response({ feature_profiles_json: {} });
    });
    const user = userEvent.setup();
    renderTraining();
    await screen.findByText("LogisticRegression");

    await user.click(screen.getByRole("button", { name: "Regression" }));
    expect(screen.getByLabelText(/Primary leaderboard metric/)).toHaveValue("rmse");
    await user.click(screen.getByRole("button", { name: "Time Series" }));
    await user.click(screen.getByRole("button", { name: "Clustering" }));
    expect(screen.getByText("Unsupervised")).toBeInTheDocument();
    expect(screen.getByText(/does not match the completed profile/)).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Classification" }));
    await user.click(screen.getByRole("checkbox", { name: /RandomForestClassifier/ }));
    await user.click(screen.getByRole("checkbox", { name: /LogisticRegression/ }));
    await user.click(screen.getByRole("checkbox", { name: /LogisticRegression/ }));
    await user.click(screen.getByRole("button", { name: "Select all models" }));
    await user.click(screen.getByRole("button", { name: /Estimate resources/ }));

    await waitFor(() => expect(estimateBody).toBeDefined());
    expect(await screen.findByText("Insufficient qualified CPU capacity.")).toBeInTheDocument();
    expect(screen.getByText("CPU", { selector: "strong" })).toBeInTheDocument();
    expect(screen.getByText("Pending")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Launch training" })).toBeDisabled();
    await user.click(screen.getByRole("button", { name: "Change configuration" }));
    expect(screen.getByRole("button", { name: /Estimate resources/ })).toBeInTheDocument();
  });

  it("reports an empty task-specific estimator catalog", async () => {
    vi.spyOn(globalThis, "fetch").mockImplementation((input) => {
      const url = String(input);
      if (url.endsWith("/datasets")) return response([dataset]);
      if (url.endsWith("/versions")) return response([version]);
      if (url.endsWith("/profile-jobs/latest")) return response({
        id: "profile-1", status: "succeeded", target_column: "churned",
        overview_json: { task_inference: { task_type: "classification" } },
      });
      if (url.includes("/training/estimators")) return response([]);
      return response({ feature_profiles_json: {} });
    });

    renderTraining();
    expect(await screen.findByText("No compatible estimators were reported for this task."))
      .toBeInTheDocument();
    expect(screen.getByText("0 of 0 models selected")).toBeInTheDocument();
  });
});
