import { render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

const bootstrap = vi.hoisted(() => {
  const renderRoot = vi.fn();
  return {
    renderRoot,
    createRoot: vi.fn(() => ({ render: renderRoot })),
    createBrowserRouter: vi.fn((routes: unknown) => ({ routes })),
    authState: {
      current: { session: null as object | null, isChecking: false, isAuthenticated: false },
    },
  };
});

vi.mock("react-dom/client", () => ({
  default: { createRoot: bootstrap.createRoot },
}));

vi.mock("react-router", async (importOriginal) => {
  const actual = await importOriginal<typeof import("react-router")>();
  return {
    ...actual,
    createBrowserRouter: bootstrap.createBrowserRouter,
    Navigate: ({ to }: { to: string }) => <div>Navigate to {to}</div>,
    RouterProvider: () => <div>Application router</div>,
  };
});

vi.mock("./useAuthState", () => ({
  useAuthState: () => bootstrap.authState.current,
}));
vi.mock("./Auth", () => ({ Auth: () => <div>Authentication screen</div> }));
vi.mock("./Layout", () => ({ Layout: () => <div>Protected layout</div> }));
vi.mock("./Landing", () => ({ Landing: () => <div>Landing screen</div> }));
vi.mock("./NotFound", () => ({ NotFound: () => <div>Not found</div> }));
vi.mock("./pages/AccountPage", () => ({ AccountPage: () => null }));
vi.mock("./pages/DataPage", () => ({ DataPage: () => null }));
vi.mock("./pages/MembersPage", () => ({ MembersPage: () => null }));
vi.mock("./pages/MonitoringPage", () => ({ MonitoringPage: () => null }));
vi.mock("./pages/OperationsPage", () => ({ OperationsPage: () => null }));
vi.mock("./pages/ProjectOverview", () => ({ ProjectOverview: () => null }));
vi.mock("./pages/ProjectsPage", () => ({ ProjectsPage: () => null }));
vi.mock("./pages/RunsPage", () => ({ RunsPage: () => null }));
vi.mock("./pages/SettingsPage", () => ({ SettingsPage: () => null }));
vi.mock("./pages/TrainingPage", () => ({ TrainingPage: () => null }));

type RouteEntry = {
  path?: string;
  element?: React.ReactNode;
  children?: RouteEntry[];
};

describe("application bootstrap", () => {
  afterEach(() => {
    bootstrap.authState.current = { session: null, isChecking: false, isAuthenticated: false };
  });

  it("registers the complete route graph and mounts the application root", async () => {
    await import("./main");

    expect(bootstrap.createRoot).toHaveBeenCalledWith(document.getElementById("root"));
    expect(bootstrap.renderRoot).toHaveBeenCalledOnce();
    expect(bootstrap.createBrowserRouter).toHaveBeenCalledOnce();
    const routes = bootstrap.createBrowserRouter.mock.calls[0][0] as RouteEntry[];
    expect(routes.map((route) => route.path)).toEqual(["/", "/auth", undefined, "*"]);
    expect(routes[2].children?.map((route) => route.path)).toEqual([
      "/projects", "/monitoring", "/account", "/projects/:projectId",
    ]);
    expect(routes[2].children?.[3].children?.map((route) => route.path)).toEqual([
      undefined, "data", "training", "runs", "operations", "members", "settings",
    ]);
  });

  it("enforces protected and authentication-route session states", async () => {
    await import("./main");
    const routes = bootstrap.createBrowserRouter.mock.calls[0][0] as RouteEntry[];
    const protectedRoute = routes[2].element!;
    const authRoute = routes[1].element!;

    const protectedView = render(protectedRoute);
    expect(screen.getByText("Navigate to /auth")).toBeInTheDocument();
    protectedView.unmount();
    bootstrap.authState.current = { session: {}, isChecking: true, isAuthenticated: false };
    const checkingProtected = render(protectedRoute);
    expect(screen.getByRole("status")).toHaveTextContent("Verifying your session");
    checkingProtected.unmount();
    bootstrap.authState.current = { session: {}, isChecking: false, isAuthenticated: true };
    const authenticatedProtected = render(protectedRoute);
    expect(screen.getByText("Protected layout")).toBeInTheDocument();
    authenticatedProtected.unmount();

    bootstrap.authState.current = { session: {}, isChecking: true, isAuthenticated: false };
    const authView = render(authRoute);
    expect(screen.getByRole("status")).toHaveTextContent("Verifying your session");
    authView.unmount();
    bootstrap.authState.current = { session: null, isChecking: false, isAuthenticated: false };
    const anonymousAuth = render(authRoute);
    expect(screen.getByText("Authentication screen")).toBeInTheDocument();
    anonymousAuth.unmount();
    bootstrap.authState.current = { session: {}, isChecking: false, isAuthenticated: true };
    render(authRoute);
    expect(screen.getByText("Navigate to /projects")).toBeInTheDocument();
  });
});
