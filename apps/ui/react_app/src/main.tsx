import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import React from "react";
import ReactDOM from "react-dom/client";
import { Navigate, RouterProvider, createBrowserRouter } from "react-router-dom";
import { Auth } from "./Auth";
import { Layout } from "./Layout";
import { Landing } from "./Landing";
import { DataPage } from "./pages/DataPage";
import { AccountPage } from "./pages/AccountPage";
import { MembersPage } from "./pages/MembersPage";
import { MonitoringPage } from "./pages/MonitoringPage";
import { OperationsPage } from "./pages/OperationsPage";
import { ProjectOverview } from "./pages/ProjectOverview";
import { ProjectsPage } from "./pages/ProjectsPage";
import { RunsPage } from "./pages/RunsPage";
import { SettingsPage } from "./pages/SettingsPage";
import { TrainingPage } from "./pages/TrainingPage";
import { NotFound } from "./NotFound";
import "@fontsource-variable/manrope/wght.css";
import "./styles.css";
import { useAuthState } from "./useAuthState";

const queryClient = new QueryClient({
  defaultOptions: { queries: { staleTime: 15_000, retry: 1, refetchOnWindowFocus: false } },
});

function Protected() {
  const { session, isChecking, isAuthenticated } = useAuthState();
  if (!session) return <Navigate to="/auth" replace />;
  if (isChecking) return <div className="session-screen" role="status">Verifying your session…</div>;
  return isAuthenticated ? <Layout /> : <Navigate to="/auth" replace />;
}

function AuthRoute() {
  const { session, isChecking, isAuthenticated } = useAuthState();
  if (session && isChecking) return <div className="session-screen" role="status">Verifying your session…</div>;
  return isAuthenticated ? <Navigate to="/projects" replace /> : <Auth />;
}
const router = createBrowserRouter([
  { path: "/", element: <Landing /> },
  { path: "/auth", element: <AuthRoute /> },
  { element: <Protected />, children: [
    { path: "/projects", element: <ProjectsPage /> },
    { path: "/monitoring", element: <MonitoringPage /> },
    { path: "/account", element: <AccountPage /> },
    { path: "/projects/:projectId", children: [
      { index: true, element: <ProjectOverview /> },
      { path: "data", element: <DataPage /> },
      { path: "training", element: <TrainingPage /> },
      { path: "runs", element: <RunsPage /> },
      { path: "operations", element: <OperationsPage /> },
      { path: "members", element: <MembersPage /> },
      { path: "settings", element: <SettingsPage /> },
    ]},
  ]},
  { path: "*", element: <NotFound /> },
]);

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode><a className="skip-link" href="#main-content">Skip to main content</a>
    <QueryClientProvider client={queryClient}><RouterProvider router={router} /></QueryClientProvider></React.StrictMode>,
);
