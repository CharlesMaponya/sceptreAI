import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, fireEvent, render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Route, Routes } from "react-router";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { setSession } from "../api";
import { DataPage } from "./DataPage";

vi.mock("../components/PlotlyChart", () => ({
  default: () => <div data-testid="feature-chart" />,
}));

const response = (data: unknown, status = 200) => Promise.resolve(new Response(JSON.stringify(data), {
  status, headers: { "Content-Type": "application/json" },
}));

function renderData() {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  });
  const result = render(<QueryClientProvider client={client}>
    <MemoryRouter initialEntries={["/projects/project-1/data"]}>
      <Routes>
        <Route path="/projects/:projectId/data" element={<DataPage />} />
        <Route path="/projects/:projectId" element={<h1>Project overview</h1>} />
        <Route path="/projects/:projectId/training" element={<h1>Training workspace</h1>} />
      </Routes>
    </MemoryRouter>
  </QueryClientProvider>);
  return { ...result, client };
}

const dataset = {
  id: "dataset-1", name: "Customers", description: null, latest_version_number: 1,
};
const version = {
  id: "version-1", dataset_id: "dataset-1", version_number: 1, status: "ready",
  format: "csv", byte_size: null, row_count: null, column_count: null,
};

describe("dataset qualification states", () => {
  beforeEach(() => {
    setSession(null);
    vi.restoreAllMocks();
  });

  it("recovers the dataset collection after a loading failure", async () => {
    let attempts = 0;
    vi.spyOn(globalThis, "fetch").mockImplementation(() => {
      attempts += 1;
      return attempts === 1 ? response({ detail: "Dataset catalog unavailable" }, 503) : response([]);
    });

    renderData();

    expect(await screen.findByText("Dataset catalog unavailable")).toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: /try again/i }));
    expect(await screen.findByText("Your model starts with trusted data")).toBeInTheDocument();
  });

  it("validates file selection, removal, drag-and-drop, and upload errors", async () => {
    vi.spyOn(globalThis, "fetch").mockImplementation(() => response([]));
    vi.spyOn(XMLHttpRequest.prototype, "open").mockImplementation(() => undefined);
    vi.spyOn(XMLHttpRequest.prototype, "setRequestHeader").mockImplementation(() => undefined);
    vi.spyOn(XMLHttpRequest.prototype, "send").mockImplementation(function (this: XMLHttpRequest) {
      Object.defineProperty(this, "status", { configurable: true, value: 500 });
      Object.defineProperty(this, "responseText", {
        configurable: true, value: JSON.stringify({ detail: "Object store rejected the upload" }),
      });
      this.onload?.call(this, new ProgressEvent("load"));
    });
    const user = userEvent.setup({ applyAccept: false });
    const { container } = renderData();
    await screen.findByText("Your model starts with trusted data");
    await user.click(screen.getByRole("button", { name: "Upload dataset" }));
    const dialog = screen.getByRole("dialog", { name: "Upload a dataset" });
    const input = container.querySelector<HTMLInputElement>('input[type="file"]')!;
    await user.upload(input, new File(["bad"], "notes.txt", { type: "text/plain" }));
    expect(within(dialog).getAllByRole("button", { name: "Upload dataset" }).at(-1)).toBeDisabled();

    const dropzone = input.closest("label")!;
    fireEvent.dragOver(dropzone);
    fireEvent.drop(dropzone, {
      dataTransfer: { files: [new File(["a,b\n1,2"], "valid.csv", { type: "text/csv" })] },
    });
    expect(screen.getByText("valid.csv")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: /Remove/ }));
    expect(screen.queryByText("valid.csv")).not.toBeInTheDocument();
    await user.upload(input, new File(["a,b\n1,2"], "valid.csv", { type: "text/csv" }));
    await user.type(screen.getByLabelText("Dataset name"), "Qualified source");
    await user.click(within(dialog).getAllByRole("button", { name: "Upload dataset" }).at(-1)!);
    expect(await screen.findByText("Object store rejected the upload")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Cancel" }));
    expect(screen.queryByRole("dialog", { name: "Upload a dataset" })).not.toBeInTheDocument();
  });

  it("shows an empty-version state for the selected dataset", async () => {
    vi.spyOn(globalThis, "fetch").mockImplementation((input) => {
      const url = String(input);
      if (url.endsWith("/projects/project-1/datasets")) return response([dataset]);
      return response([]);
    });

    renderData();

    expect(await screen.findByText("No dataset versions")).toBeInTheDocument();
    expect(screen.getByText("Upload a new version before profiling this dataset.")).toBeInTheDocument();
  });

  it("renders active profiling progress and switches between datasets", async () => {
    vi.spyOn(globalThis, "fetch").mockImplementation((input) => {
      const url = String(input);
      if (url.endsWith("/projects/project-1/datasets")) return response([
        dataset, { ...dataset, id: "dataset-2", name: "Transactions" },
      ]);
      if (url.endsWith("/dataset-2/versions")) return response([{
        ...version, id: "version-2", dataset_id: "dataset-2", version_number: 2,
        format: "parquet", byte_size: 4096, row_count: 2500, column_count: 8,
      }]);
      if (url.endsWith("/profile-jobs/latest")) return response({
        id: "profile-active", status: "running", current_stage: "relationships",
        progress: .6, completed_columns: 4, total_columns: 8,
      });
      return response([version]);
    });

    renderData();
    await screen.findByRole("heading", { name: "Customers" });
    await userEvent.selectOptions(screen.getByLabelText("Dataset"), "dataset-2");

    expect(await screen.findByRole("heading", { name: "Transactions" })).toBeInTheDocument();
    expect(await screen.findByText("60%")).toBeInTheDocument();
    expect(screen.getByText(/Profiling 4 of 8 columns/)).toBeInTheDocument();
    expect(screen.getByText("2,500")).toBeInTheDocument();
    expect(screen.getByText("PARQUET")).toBeInTheDocument();
  });

  it("reconciles selection when a refreshed catalog removes the active dataset", async () => {
    const second = { ...dataset, id: "dataset-2", name: "Transactions" };
    vi.spyOn(globalThis, "fetch").mockImplementation((input) => {
      const url = String(input);
      if (url.endsWith("/projects/project-1/datasets")) return response([dataset, second]);
      if (url.endsWith("/dataset-2/versions")) return response([]);
      return response([version]);
    });

    const { client } = renderData();
    expect(await screen.findByRole("heading", { name: "Customers" })).toBeInTheDocument();
    act(() => client.setQueryData(["datasets", "project-1"], [second]));

    expect(await screen.findByText("No dataset versions")).toBeInTheDocument();
    expect(screen.queryByRole("heading", { name: "Customers" })).not.toBeInTheDocument();
  });

  it("renders queued profiling defaults without inventing progress", async () => {
    vi.spyOn(globalThis, "fetch").mockImplementation((input) => {
      const url = String(input);
      if (url.endsWith("/projects/project-1/datasets")) return response([dataset]);
      if (url.endsWith("/profile-jobs/latest")) return response({
        id: "profile-queued", status: "queued", current_stage: null,
        progress: null, completed_columns: null, total_columns: null,
      });
      return response([version]);
    });

    renderData();

    expect(await screen.findByText("Preparing")).toBeInTheDocument();
    expect(screen.getByText("0%")).toBeInTheDocument();
    expect(screen.getByText(/Profiling 0 of 0 columns/)).toBeInTheDocument();
  });

  it("routes an unprofiled dataset to target selection", async () => {
    vi.spyOn(globalThis, "fetch").mockImplementation((input) => {
      const url = String(input);
      if (url.endsWith("/projects/project-1/datasets")) return response([dataset]);
      if (url.endsWith("/profile-jobs/latest")) return response(null);
      return response([version]);
    });

    renderData();

    await userEvent.click(await screen.findByRole("button", { name: "Choose target & profile" }));
    expect(await screen.findByRole("heading", { name: "Project overview" })).toBeInTheDocument();
  });

  it("renders non-leaking profile evidence and starts training", async () => {
    vi.spyOn(globalThis, "fetch").mockImplementation((input) => {
      const url = String(input);
      if (url.endsWith("/projects/project-1/datasets")) return response([dataset]);
      if (url.endsWith("/profile-jobs/latest")) return response({
        id: "profile-1", status: "succeeded", target_column: "segment",
      });
      if (url.endsWith("/profile-jobs/profile-1/result")) return response({
        id: "profile-1", status: "succeeded", target_column: "segment",
        overview_json: {
          task_inference: { task_type: "clustering", confidence: 0, rationale: "No target." },
          leakage_analysis: { status: "completed", analyzed_rows: 1200, excluded_columns: [], findings: [] },
        },
        feature_profiles_json: { comments: {
          name: "comments", semantic_type: "text", distinct_count: 0,
          missing_count: null, missing_ratio: null, statistics: {}, distribution: [],
        } },
        preparation_json: [], relationships_json: [],
      });
      return response([version]);
    });

    renderData();

    expect(await screen.findByText(/No high-confidence target leakage/)).toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: /comments/i }));
    expect(screen.getByText(/Word frequencies are unavailable for this profile/)).toBeInTheDocument();
    expect(screen.getByText("No descriptive statistics available.")).toBeInTheDocument();
    expect(screen.getByText("No preprocessing step is currently required.")).toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: "Start training" }));
    expect(await screen.findByRole("heading", { name: "Training workspace" })).toBeInTheDocument();
  });

  it("uses safe labels for an incomplete preprocessing recommendation", async () => {
    vi.spyOn(globalThis, "fetch").mockImplementation((input) => {
      const url = String(input);
      if (url.endsWith("/projects/project-1/datasets")) return response([dataset]);
      if (url.endsWith("/profile-jobs/latest")) return response({
        id: "profile-steps", status: "succeeded", target_column: "segment",
      });
      if (url.endsWith("/profile-jobs/profile-steps/result")) return response({
        id: "profile-steps", status: "succeeded", target_column: "segment",
        overview_json: {},
        feature_profiles_json: { tenure: {
          name: "tenure", semantic_type: "numerical_continuous", distinct_count: 12,
          missing_count: 0, missing_ratio: 0, statistics: {}, distribution: [],
        } },
        preparation_json: [{ column: "tenure", action: null, strategy: null, reason: null }],
        relationships_json: [],
      });
      return response([version]);
    });

    renderData();
    await userEvent.click(await screen.findByRole("button", { name: /tenure/i }));
    expect(screen.getByText("Step")).toBeInTheDocument();
    expect(screen.getByText(/Recommended/)).toBeInTheDocument();
  });
});
