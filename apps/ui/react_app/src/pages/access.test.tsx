import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Route, Routes } from "react-router";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { Layout } from "../Layout";
import { setSession } from "../api";
import { MembersPage } from "./MembersPage";
import { SettingsPage } from "./SettingsPage";

const response = (data: unknown, status = 200) => Promise.resolve(new Response(JSON.stringify(data), {
  status, headers: { "Content-Type": "application/json" },
}));

const session = {
  user: {
    id: "user-1", email: "ada@example.com", full_name: "Ada Lovelace", auth_provider: "simple",
    global_role: "member", is_active: true, is_verified: true, created_at: "2026-01-01T00:00:00Z",
  },
  tokens: {
    access_token: "access", refresh_token: "refresh-token-long-enough",
    token_type: "bearer", expires_in: 3600,
  },
};

function renderProjectPage(element: React.ReactNode, path: string) {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  });
  return render(<QueryClientProvider client={client}>
    <MemoryRouter initialEntries={[path]}>
      <Routes><Route path="/projects/:projectId/*" element={element} /></Routes>
    </MemoryRouter>
  </QueryClientProvider>);
}

describe("project access and settings", () => {
  beforeEach(() => {
    setSession(session);
    vi.restoreAllMocks();
  });

  it("creates and copies a scoped project invitation", async () => {
    const copied = vi.fn();
    Object.defineProperty(navigator, "clipboard", {
      configurable: true,
      value: { writeText: copied },
    });
    const requests: Array<{ url: string; options?: RequestInit }> = [];
    vi.spyOn(globalThis, "fetch").mockImplementation((input, options) => {
      const url = String(input);
      requests.push({ url, options });
      if (url.endsWith("/members")) return response([{
        id: "membership-1", user_id: "user-1", email: "ada@example.com",
        full_name: "Ada Lovelace", role: "owner", accepted_at: "2026-01-01T00:00:00Z",
      }]);
      return response({
        invite_token: "single-use-invite-token", role: "editor",
        expires_at: "2026-01-08T00:00:00Z", max_uses: 1,
      });
    });

    renderProjectPage(<MembersPage />, "/projects/project-1/members");
    expect(await screen.findByText("Ada Lovelace")).toBeInTheDocument();
    expect(screen.getByText("Owner")).toBeInTheDocument();
    await userEvent.selectOptions(screen.getByLabelText("Project role"), "editor");
    await userEvent.selectOptions(screen.getByLabelText("Token expires in"), "14");
    await userEvent.click(screen.getByRole("button", { name: "Create invite token" }));

    expect(await screen.findByText("single-use-invite-token")).toBeInTheDocument();
    const post = requests.find(({ options }) => options?.method === "POST");
    expect(post?.url.endsWith("/projects/project-1/share-links")).toBe(true);
    expect(JSON.parse(String(post?.options?.body))).toMatchObject({
      role: "editor", expires_in_days: 14, max_uses: 1,
    });
    await userEvent.click(screen.getByRole("button", { name: "Copy" }));
    expect(copied).toHaveBeenCalledWith("single-use-invite-token");
    expect(screen.getByRole("button", { name: "Copied" })).toBeInTheDocument();
  });

  it("shows member loading failures and recovers on retry", async () => {
    let attempts = 0;
    vi.spyOn(globalThis, "fetch").mockImplementation(() => {
      attempts += 1;
      return attempts === 1
        ? response({ detail: "Members unavailable" }, 503)
        : response([]);
    });
    renderProjectPage(<MembersPage />, "/projects/project-1/members");

    expect(await screen.findByText("Members unavailable")).toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: /try again/i }));
    expect(await screen.findByText("No collaborators yet")).toBeInTheDocument();
  });

  it("renders unnamed members and reports invite creation failures", async () => {
    vi.spyOn(globalThis, "fetch").mockImplementation((input, options) => {
      const url = String(input);
      if (url.endsWith("/members")) return response([{
        id: "membership-2", user_id: "user-2", email: "member@example.com",
        full_name: "", role: "viewer", accepted_at: null,
      }]);
      if (options?.method === "POST") return response({ detail: "Invites are temporarily disabled" }, 503);
      return response([]);
    });
    renderProjectPage(<MembersPage />, "/projects/project-1/members");

    expect(await screen.findByText("Project member")).toBeInTheDocument();
    expect(screen.getByText("Viewer")).toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: "Create invite token" }));
    expect(await screen.findByText("Invites are temporarily disabled")).toBeInTheDocument();
  });

  it("updates project identity and invalidates the portfolio query", async () => {
    const project = {
      id: "project-1", owner_id: "user-1", created_by_id: "user-1",
      name: "Risk lab", description: "Original", status: "active", settings: {},
      created_at: "2026-01-01T00:00:00Z", updated_at: "2026-01-01T00:00:00Z",
    };
    const requests: RequestInit[] = [];
    vi.spyOn(globalThis, "fetch").mockImplementation((_input, options) => {
      requests.push(options ?? {});
      if (options?.method === "PATCH") {
        return response({ ...project, name: "Production risk", description: "Qualified", status: "archived" });
      }
      return response(project);
    });
    renderProjectPage(<SettingsPage />, "/projects/project-1/settings");

    const name = await screen.findByLabelText("Project name");
    await userEvent.clear(name);
    await userEvent.type(name, "Production risk");
    const description = screen.getByLabelText("Description");
    await userEvent.clear(description);
    await userEvent.type(description, "Qualified");
    await userEvent.selectOptions(screen.getByLabelText("Status"), "archived");
    await userEvent.click(screen.getByRole("button", { name: "Save changes" }));

    expect(await screen.findByText("Project settings saved.")).toBeInTheDocument();
    const patch = requests.find((request) => request.method === "PATCH");
    expect(JSON.parse(String(patch?.body))).toEqual({
      name: "Production risk", description: "Qualified", status: "archived",
    });
  });

  it("recovers project settings and reports an update rejection", async () => {
    let reads = 0;
    vi.spyOn(globalThis, "fetch").mockImplementation((_input, options) => {
      if (options?.method === "PATCH") return response({ detail: "Only owners can archive this project" }, 403);
      reads += 1;
      return reads === 1 ? response({ detail: "Project unavailable" }, 503) : response({
        id: "project-1", owner_id: "user-1", created_by_id: "user-1",
        name: "Risk lab", description: null, status: "active", settings: {},
        created_at: "2026-01-01T00:00:00Z", updated_at: "2026-01-01T00:00:00Z",
      });
    });
    renderProjectPage(<SettingsPage />, "/projects/project-1/settings");

    expect(await screen.findByText("Project unavailable")).toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: /try again/i }));
    expect(await screen.findByLabelText("Description")).toHaveValue("");
    await userEvent.selectOptions(screen.getByLabelText("Status"), "archived");
    await userEvent.click(screen.getByRole("button", { name: "Save changes" }));
    expect(await screen.findByText("Only owners can archive this project")).toBeInTheDocument();
  });

  it("renders the project layout, mobile controls, and sign-out navigation", async () => {
    vi.spyOn(globalThis, "fetch").mockImplementation((input) => {
      if (String(input).endsWith("/projects")) return response([{
        id: "project-1", name: "Fraud controls", description: "", status: "active",
        settings: {}, created_at: "2026-01-01T00:00:00Z", updated_at: "2026-01-01T00:00:00Z",
      }]);
      return Promise.resolve(new Response(null, { status: 204 }));
    });
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    render(<QueryClientProvider client={client}>
      <MemoryRouter initialEntries={["/projects/project-1/data"]}>
        <Routes>
          <Route path="/projects/:projectId" element={<Layout />}>
            <Route path="data" element={<h1>Dataset workspace</h1>} />
          </Route>
          <Route path="/" element={<h1>Signed out</h1>} />
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>);

    expect((await screen.findAllByText("Fraud controls")).length).toBeGreaterThanOrEqual(2);
    expect(screen.getByRole("heading", { name: "Dataset workspace" })).toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: "Open menu" }));
    const closeButtons = screen.getAllByRole("button", { name: "Close menu" });
    expect(closeButtons).toHaveLength(2);
    await userEvent.click(closeButtons[0]);
    await userEvent.click(screen.getByRole("button", { name: "Sign out" }));
    expect(await screen.findByRole("heading", { name: "Signed out" })).toBeInTheDocument();
    await waitFor(() => expect(localStorage.getItem("sceptre.session")).toBeNull());
  });

  it("closes mobile navigation and routes every project and account shortcut", async () => {
    vi.spyOn(globalThis, "fetch").mockImplementation((input) => {
      if (String(input).endsWith("/projects")) return response([{
        id: "project-1", name: "Fraud controls", description: "", status: "active",
        settings: {}, created_at: "2026-01-01T00:00:00Z", updated_at: "2026-01-01T00:00:00Z",
      }]);
      return response([]);
    });
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    render(<QueryClientProvider client={client}>
      <MemoryRouter initialEntries={["/projects/project-1"]}>
        <Routes>
          <Route path="/projects/:projectId" element={<Layout />}>
            <Route index element={<h1>Overview page</h1>} />
            <Route path="data" element={<h1>Data page</h1>} />
          </Route>
          <Route path="/projects" element={<h1>Project portfolio</h1>} />
          <Route path="/monitoring" element={<h1>Governance page</h1>} />
          <Route path="/account" element={<h1>Account page</h1>} />
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>);

    await screen.findAllByText("Fraud controls");
    await userEvent.click(screen.getByRole("button", { name: "Open menu" }));
    await userEvent.click(screen.getAllByRole("button", { name: "Close menu" })[1]);
    expect(screen.getAllByRole("button", { name: "Close menu" })).toHaveLength(1);
    await userEvent.click(screen.getByRole("button", { name: "Open menu" }));
    await userEvent.click(screen.getByRole("link", { name: "Data" }));
    expect(await screen.findByRole("heading", { name: "Data page" })).toBeInTheDocument();
    await userEvent.click(screen.getByRole("link", { name: "Governance dashboard" }));
    expect(await screen.findByRole("heading", { name: "Governance page" })).toBeInTheDocument();
  });

  it.each([
    ["Projects", "Project portfolio"],
    ["Profile & security", "Account page"],
  ])("routes the %s shortcut", async (linkName, heading) => {
    vi.spyOn(globalThis, "fetch").mockImplementation(() => response([]));
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    render(<QueryClientProvider client={client}>
      <MemoryRouter initialEntries={["/projects/project-1"]}>
        <Routes>
          <Route path="/projects/:projectId" element={<Layout />}><Route index element={<h1>Workspace</h1>} /></Route>
          <Route path="/projects" element={<h1>Project portfolio</h1>} />
          <Route path="/account" element={<h1>Account page</h1>} />
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>);

    await userEvent.click(await screen.findByRole("link", { name: linkName }));
    expect(await screen.findByRole("heading", { name: heading })).toBeInTheDocument();
  });
});
