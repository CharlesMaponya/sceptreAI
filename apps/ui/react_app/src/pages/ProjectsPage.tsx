import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { ArrowRight, FolderKanban, Link2, Plus, Search } from "lucide-react";
import { FormEvent, useMemo, useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import { api, json } from "../api";
import { Badge, Button, Card, EmptyState, ErrorState, Loading, Modal, PageHeader } from "../components/ui";
import { formatDate, initials } from "../lib";
import type { Project } from "../types";

export function ProjectsPage() {
  const navigate = useNavigate();
  const client = useQueryClient();
  const [dialog, setDialog] = useState<"create" | "invite" | null>(null);
  const [search, setSearch] = useState("");
  const projects = useQuery({ queryKey: ["projects"], queryFn: () => api<Project[]>("/projects") });
  const create = useMutation({
    mutationFn: (body: object) => api<Project>("/projects", json("POST", body)),
    onSuccess: (project) => { client.invalidateQueries({ queryKey: ["projects"] }); setDialog(null); navigate(`/projects/${project.id}`); },
  });
  const accept = useMutation({
    mutationFn: (invite_token: string) => api<Project>("/projects/share-links/accept", json("POST", { invite_token })),
    onSuccess: (project) => { client.invalidateQueries({ queryKey: ["projects"] }); setDialog(null); navigate(`/projects/${project.id}`); },
  });
  const filtered = useMemo(() => projects.data?.filter((project) =>
    `${project.name} ${project.description}`.toLowerCase().includes(search.toLowerCase())
  ), [projects.data, search]);

  function submitCreate(event: FormEvent<HTMLFormElement>) {
    event.preventDefault(); const values = Object.fromEntries(new FormData(event.currentTarget));
    create.mutate({ ...values, settings: {} });
  }
  function submitInvite(event: FormEvent<HTMLFormElement>) {
    event.preventDefault(); accept.mutate(String(new FormData(event.currentTarget).get("invite_token")));
  }

  return <>
    <PageHeader eyebrow="Your workspaces" title="Projects" description="Build, compare, and operate models in isolated team workspaces."
      action={<div className="button-row"><Button variant="secondary" onClick={() => setDialog("invite")}><Link2 size={16} />Join project</Button>
        <Button onClick={() => setDialog("create")}><Plus size={16} />New project</Button></div>} />
    <div className="toolbar"><label className="search"><Search size={17} /><input value={search} onChange={(e) => setSearch(e.target.value)} placeholder="Search projects…" aria-label="Search projects" /></label>
      <span>{projects.data?.length || 0} project{projects.data?.length === 1 ? "" : "s"}</span></div>
    {projects.isLoading ? <Loading /> : projects.error ? <ErrorState error={projects.error} retry={() => projects.refetch()} /> :
      filtered?.length ? <div className="project-grid">{filtered.map((project) =>
        <Link className="card project-card" key={project.id} to={`/projects/${project.id}`}>
          <div className="project-card__top"><div className="project-mark">{initials(project.name)}</div><Badge status={project.status} /></div>
          <h2>{project.name}</h2><p>{project.description || "A governed Sceptre AI workspace."}</p>
          <div className="project-card__foot"><span><FolderKanban size={15} /> Updated {formatDate(project.updated_at)}</span><ArrowRight size={18} /></div>
        </Link>)}</div> :
        <Card><EmptyState icon={<FolderKanban />} title={search ? "No matching projects" : "Create your first project"}
          description={search ? "Try a different name or clear the search." : "Projects keep datasets, experiments, models, and team access together."}
          action={!search && <Button onClick={() => setDialog("create")}><Plus size={16} />Create project</Button>} /></Card>}
    {dialog === "create" && <Modal title="Create a project" description="Give this model initiative a clear, outcome-focused name." onClose={() => setDialog(null)}>
      <form className="stack" onSubmit={submitCreate}><label>Project name<input name="name" required maxLength={180} autoFocus placeholder="Customer churn prevention" /></label>
        <label>Description<textarea name="description" rows={3} placeholder="What decision will this model support?" /></label>
        {create.error && <p className="field-error">{create.error.message}</p>}<div className="modal__actions"><Button variant="ghost" type="button" onClick={() => setDialog(null)}>Cancel</Button><Button loading={create.isPending}>Create project</Button></div></form>
    </Modal>}
    {dialog === "invite" && <Modal title="Join a project" description="Paste the secure invitation token shared by a project owner." onClose={() => setDialog(null)}>
      <form className="stack" onSubmit={submitInvite}><label>Invite token<input name="invite_token" required minLength={16} autoFocus placeholder="Paste token" /></label>
        {accept.error && <p className="field-error">{accept.error.message}</p>}<div className="modal__actions"><Button variant="ghost" type="button" onClick={() => setDialog(null)}>Cancel</Button><Button loading={accept.isPending}>Join project</Button></div></form>
    </Modal>}
  </>;
}
