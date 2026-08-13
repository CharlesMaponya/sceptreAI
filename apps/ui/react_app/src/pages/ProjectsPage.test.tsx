import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Route, Routes } from "react-router";
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

  it("creates a project and opens its workspace", async () => {
    const user = userEvent.setup();
    const created = { ...projects[0], id: "created-project", name: "Churn prevention" };
    const fetchMock = vi.spyOn(globalThis, "fetch")
      .mockResolvedValueOnce(new Response("[]", { status: 200, headers: { "Content-Type": "application/json" } }))
      .mockResolvedValueOnce(new Response(JSON.stringify(created), { status: 201, headers: { "Content-Type": "application/json" } }))
      .mockResolvedValue(new Response(JSON.stringify([created]), { status: 200, headers: { "Content-Type": "application/json" } }));
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    render(<QueryClientProvider client={client}><MemoryRouter initialEntries={["/projects"]}><Routes>
      <Route path="/projects" element={<ProjectsPage />} />
      <Route path="/projects/:projectId" element={<h1>Opened project</h1>} />
    </Routes></MemoryRouter></QueryClientProvider>);

    expect(await screen.findByText("Create your first project")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Create project" }));
    await user.type(screen.getByLabelText("Project name"), "Churn prevention");
    await user.type(screen.getByLabelText("Description"), "Reduce preventable churn");
    await user.click(within(screen.getByRole("dialog")).getByRole("button", { name: "Create project" }));

    expect(await screen.findByRole("heading", { name: "Opened project" })).toBeInTheDocument();
    expect(fetchMock.mock.calls[1][0]).toBe("/api/v1/projects");
    expect(fetchMock.mock.calls[1][1]?.method).toBe("POST");
    expect(JSON.parse(String(fetchMock.mock.calls[1][1]?.body))).toEqual({
      name: "Churn prevention", description: "Reduce preventable churn", settings: {},
    });
  });

  it("shows create errors and lets the user cancel the dialog", async () => {
    const user = userEvent.setup();
    vi.spyOn(globalThis, "fetch")
      .mockResolvedValueOnce(new Response("[]", { status: 200, headers: { "Content-Type": "application/json" } }))
      .mockResolvedValueOnce(new Response(JSON.stringify({ detail: "Project name already exists" }), {
        status: 409, headers: { "Content-Type": "application/json" },
      }));
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    render(<QueryClientProvider client={client}><MemoryRouter><ProjectsPage /></MemoryRouter></QueryClientProvider>);

    expect(await screen.findByText("Create your first project")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "New project" }));
    await user.type(screen.getByLabelText("Project name"), "Duplicate");
    await user.click(within(screen.getByRole("dialog")).getByRole("button", { name: "Create project" }));
    expect(await screen.findByText("Project name already exists")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Cancel" }));
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
  });

  it("joins a project from a secure invitation", async () => {
    const user = userEvent.setup();
    const joined = { ...projects[0], id: "joined-project" };
    const fetchMock = vi.spyOn(globalThis, "fetch")
      .mockResolvedValueOnce(new Response(JSON.stringify(projects.slice(0, 1)), { status: 200, headers: { "Content-Type": "application/json" } }))
      .mockResolvedValueOnce(new Response(JSON.stringify(joined), { status: 200, headers: { "Content-Type": "application/json" } }))
      .mockResolvedValue(new Response(JSON.stringify(projects), { status: 200, headers: { "Content-Type": "application/json" } }));
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    render(<QueryClientProvider client={client}><MemoryRouter initialEntries={["/projects"]}><Routes>
      <Route path="/projects" element={<ProjectsPage />} />
      <Route path="/projects/:projectId" element={<h1>Joined project</h1>} />
    </Routes></MemoryRouter></QueryClientProvider>);

    expect(await screen.findByRole("heading", { name: "Project 1" })).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Join project" }));
    await user.type(screen.getByLabelText("Invite token"), "invite-token-long-enough");
    await user.click(within(screen.getByRole("dialog")).getByRole("button", { name: "Join project" }));

    expect(await screen.findByRole("heading", { name: "Joined project" })).toBeInTheDocument();
    expect(fetchMock.mock.calls[1][0]).toBe("/api/v1/projects/share-links/accept");
    expect(JSON.parse(String(fetchMock.mock.calls[1][1]?.body))).toEqual({ invite_token: "invite-token-long-enough" });
  });

  it("recovers from project loading errors and distinguishes empty searches", async () => {
    const user = userEvent.setup();
    vi.spyOn(globalThis, "fetch")
      .mockResolvedValueOnce(new Response(JSON.stringify({ detail: "Project service unavailable" }), {
        status: 503, headers: { "Content-Type": "application/json" },
      }))
      .mockResolvedValue(new Response(JSON.stringify(projects.slice(0, 1)), {
        status: 200, headers: { "Content-Type": "application/json" },
      }));
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    render(<QueryClientProvider client={client}><MemoryRouter><ProjectsPage /></MemoryRouter></QueryClientProvider>);

    expect(await screen.findByText("Project service unavailable")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Try again" }));
    expect(await screen.findByRole("heading", { name: "Project 1" })).toBeInTheDocument();
    await user.type(screen.getByRole("textbox", { name: "Search projects" }), "not present");
    expect(screen.getByText("No matching projects")).toBeInTheDocument();
  });
});
