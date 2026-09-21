import { useQuery } from "@tanstack/react-query";
import { ArrowRight, Check } from "lucide-react";
import { Link } from "react-router";
import { api } from "../api";
import { Card, Loading, Notice } from "../components/ui";

interface Journey {
  dataset_uploaded: boolean;
  profile_status: string | null;
  training_status: string | null;
  analysis_status: string | null;
  deployment_status: string | null;
}

function stage(status: string | null, ready: boolean, progress: string, complete: string) {
  if (status === "succeeded") return { state: "complete", label: complete };
  if (status === "failed" || status === "preempted") return { state: "failed", label: "Needs attention" };
  if (status === "cancelled") return { state: "ready", label: "Stopped" };
  if (status) return { state: "current", label: progress };
  return { state: ready ? "ready" : "pending", label: ready ? "Ready to start" : "Not started" };
}

export function ProjectJourney({ projectId }: { projectId: string }) {
  const journey = useQuery({
    queryKey: ["project-journey", projectId],
    queryFn: () => api<Journey>(`/projects/${projectId}/journey`),
    refetchInterval: 5000,
  });
  const data = journey.data;
  const steps = data ? [
    { name: "Data", to: "data", ...stage(data.profile_status, data.dataset_uploaded, "Preparing data", "Profile complete") },
    { name: "Train", to: "training", ...stage(data.training_status, data.profile_status === "succeeded", "Training underway", "Training complete") },
    { name: "Validate", to: "runs", ...stage(data.analysis_status, data.training_status === "succeeded", "Analysis underway", "Analysis complete") },
    { name: "Operate", to: "operations", ...stage(data.deployment_status,
      data.analysis_status === "succeeded" || data.training_status === "succeeded",
      data.deployment_status === "running" ? "Deployment active" : "Deploying", "Deployed") },
  ] : [];
  const current = steps.map(item => item.state).lastIndexOf("current");
  return <Card className="journey"><h2>Model journey</h2>
    <p className="muted">Live activity across this project.</p>
    {journey.isLoading ? <Loading label="Checking project progress…" />
      : journey.error ? <Notice tone="danger">Project progress is temporarily unavailable.</Notice>
        : <nav aria-label="Model journey">{steps.map((item, index) =>
          <Link className={`journey__step journey__step--${item.state}`} to={item.to}
            aria-current={index === current ? "step" : undefined} key={item.name}>
            <i>{item.state === "complete" ? <Check size={16} /> : index + 1}</i>
            <div><b>{item.name}</b><small>{item.label}</small></div><ArrowRight size={15} />
          </Link>)}</nav>}
  </Card>;
}
