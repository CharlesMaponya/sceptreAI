import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import React from "react";
import ReactDOM from "react-dom/client";
import { Navigate, RouterProvider, createBrowserRouter } from "react-router-dom";
import { getSession } from "./api";
import { Auth } from "./Auth";
import { Layout } from "./Layout";
import { DataPage } from "./pages/DataPage";
import { MembersPage } from "./pages/MembersPage";
import { OperationsPage } from "./pages/OperationsPage";
import { ProjectOverview } from "./pages/ProjectOverview";
import { ProjectsPage } from "./pages/ProjectsPage";
import { RunsPage } from "./pages/RunsPage";
import { SettingsPage } from "./pages/SettingsPage";
import { TrainingPage } from "./pages/TrainingPage";
import "./styles.css";

const queryClient = new QueryClient({
  defaultOptions: { queries: { staleTime: 15_000, retry: 1, refetchOnWindowFocus: false } },
});

function Protected() { return getSession() ? <Layout /> : <Navigate to="/" replace />; }
function Landing() { return getSession() ? <Navigate to="/projects" replace /> : <Auth />; }

const router = createBrowserRouter([
  { path: "/", element: <Landing /> },
  { element: <Protected />, children: [
    { path: "/projects", element: <ProjectsPage /> },
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
  { path: "*", element: <Navigate to="/" replace /> },
]);

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode><QueryClientProvider client={queryClient}><RouterProvider router={router} /></QueryClientProvider></React.StrictMode>,
);
