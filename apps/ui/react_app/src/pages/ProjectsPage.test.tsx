import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { setSession } from "../api";
import { ProjectsPage } from "./ProjectsPage";

const projects = Array.from({ length: 8 }, (_, index) => ({
  id: `project-${index + 1}`,
  owner_id: "owner-1",
  name: `Project ${index + 1}`,
  description: `Description ${index + 1}`,
  status: "active",
  settings: {},
  created_at: "2026-01-01T00:00:00Z",
  updated_at: "2026-01-01T00:00:00Z",
}));

describe("projects pagination", () => {
  beforeEach(() => {
    setSession(null);
    vi.restoreAllMocks();
    vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response(JSON.stringify(projects), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    }));
  });

  it("paginates filtered projects and resets to the first page when searching", async () => {
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    render(<QueryClientProvider client={client}><MemoryRouter><ProjectsPage /></MemoryRouter></QueryClientProvider>);

    expect(await screen.findByRole("heading", { name: "Project 1" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Project 6" })).toBeInTheDocument();
    expect(screen.queryByRole("heading", { name: "Project 7" })).not.toBeInTheDocument();
    expect(screen.getByText("Showing 1–6 of 8")).toBeInTheDocument();

    await userEvent.click(screen.getByRole("button", { name: "Next" }));
    expect(screen.getByRole("heading", { name: "Project 7" })).toBeInTheDocument();
    expect(screen.queryByRole("heading", { name: "Project 1" })).not.toBeInTheDocument();
    expect(screen.getByText("Showing 7–8 of 8")).toBeInTheDocument();
    expect(screen.getByText("Page 2 of 2")).toBeInTheDocument();

    await userEvent.type(screen.getByRole("textbox", { name: "Search projects" }), "Project 1");
    expect(screen.getByRole("heading", { name: "Project 1" })).toBeInTheDocument();
    expect(screen.queryByRole("navigation", { name: "Projects pagination" })).not.toBeInTheDocument();
  });
});
