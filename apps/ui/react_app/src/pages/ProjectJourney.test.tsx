import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor, within } from "@testing-library/react";
import { MemoryRouter } from "react-router";
import { afterEach, expect, it, vi } from "vitest";
import { ProjectJourney } from "./ProjectJourney";

afterEach(() => vi.restoreAllMocks());

function renderJourney() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(<QueryClientProvider client={client}><MemoryRouter>
    <ProjectJourney projectId="project-1" />
  </MemoryRouter></QueryClientProvider>);
}

it("refreshes all stages as training progresses into analysis and deployment", async () => {
  let status = {
    dataset_uploaded: true, profile_status: "succeeded", training_status: "running",
    analysis_status: null as string | null, deployment_status: null as string | null,
  };
  const fetch = vi.spyOn(globalThis, "fetch").mockImplementation(async () =>
    new Response(JSON.stringify(status), { headers: { "Content-Type": "application/json" } }));
  renderJourney();
  const journey = await screen.findByRole("navigation", { name: "Model journey" });
  expect(within(journey).getByText("Profile complete")).toBeInTheDocument();
  expect(within(journey).getByText("Training underway")).toBeInTheDocument();
  expect(within(journey).getByRole("link", { name: /Train/ })).toHaveAttribute("aria-current", "step");
  status = { ...status, training_status: "succeeded", analysis_status: "succeeded", deployment_status: "running" };
  await waitFor(() => expect(within(journey).getByText("Deployment active")).toBeInTheDocument(), { timeout: 6500 });
  expect(within(journey).getByText("Training complete")).toBeInTheDocument();
  expect(within(journey).getByText("Analysis complete")).toBeInTheDocument();
  expect(fetch.mock.calls.every(([url]) => String(url) === "/api/v1/projects/project-1/journey")).toBe(true);
}, 10_000);

it("does not mark a failed stage complete or invent unavailable progress", async () => {
  vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response(JSON.stringify({
    dataset_uploaded: true, profile_status: "succeeded", training_status: "failed",
    analysis_status: null, deployment_status: null,
  }), { headers: { "Content-Type": "application/json" } }));
  renderJourney();
  const journey = await screen.findByRole("navigation", { name: "Model journey" });
  expect(within(journey).getByRole("link", { name: /Train/ })).toHaveTextContent("Needs attention");
  expect(within(journey).getByRole("link", { name: /Validate/ })).toHaveTextContent("Not started");
});

it("shows a refresh error without presenting fabricated stage statuses", async () => {
  vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response("{}", { status: 503 }));
  renderJourney();
  expect(await screen.findByText("Project progress is temporarily unavailable.")).toBeInTheDocument();
  expect(screen.queryByRole("navigation", { name: "Model journey" })).not.toBeInTheDocument();
});
