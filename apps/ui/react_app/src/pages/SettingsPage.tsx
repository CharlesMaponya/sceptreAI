import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { FormEvent, useState } from "react";
import { useNavigate, useParams } from "react-router";
import { api, json } from "../api";
import { Button, Card, ConfirmModal, ErrorState, Loading, Notice, PageHeader } from "../components/ui";
import type { Project } from "../types";

export function SettingsPage() {
  const { projectId = "" } = useParams();
  const client = useQueryClient();
  const navigate = useNavigate();
  const [confirmDelete, setConfirmDelete] = useState(false);
  const remove = useMutation({
    mutationFn: () => api(`/projects/${projectId}`, json("DELETE")),
    onSuccess: () => { client.invalidateQueries({ queryKey: ["projects"] }); navigate("/projects"); },
  });
  const project = useQuery({ queryKey: ["project", projectId], queryFn: () => api<Project>(`/projects/${projectId}`), refetchInterval: 3000 });
  const update = useMutation({
    mutationFn: (body: object) => api<Project>(`/projects/${projectId}`, json("PATCH", body)),
    onSuccess: (data) => { client.setQueryData(["project", projectId], data); client.invalidateQueries({ queryKey: ["projects"] }); },
  });
  function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault(); const values = Object.fromEntries(new FormData(event.currentTarget));
    update.mutate({ name: values.name, description: values.description, status: values.status });
  }
  if (project.isLoading) return <Loading />;
  if (project.error) return <ErrorState error={project.error} retry={() => project.refetch()} />;
  return <><PageHeader eyebrow="Project management" title="Project settings" description="Keep the workspace clear and recognizable for every collaborator." />
    {project.data?.settings?.deletion_in_progress === true && <Notice>Deletion is in progress. Stored files and associated records are being removed.</Notice>}
    {typeof project.data?.settings?.deletion_error === "string" && <Notice tone="danger">{project.data.settings.deletion_error} Cleanup will retry; use Retry project cleanup if it remains blocked.</Notice>}
    <Card className="settings-card"><form className="stack" onSubmit={submit}><div><h2>General</h2><p className="muted">Names and descriptions appear throughout the workspace.</p></div>
      <label>Project name<input name="name" required maxLength={180} defaultValue={project.data?.name} /></label>
      <label>Description<textarea name="description" rows={4} defaultValue={project.data?.description || ""} /></label>
      <label>Status<select name="status" defaultValue={project.data?.status}><option value="active">Active</option><option value="archived">Archived</option></select></label>
      {update.isSuccess && <Notice tone="success">Project settings saved.</Notice>}{update.error && <Notice tone="danger">{update.error.message}</Notice>}
      <div><Button loading={update.isPending}>Save changes</Button></div></form></Card>
    <Card className="settings-card"><h2>Delete project</h2><p>Permanently remove this project, its datasets, runs, saved models, and invitations. Cancel active work and shut down and clean up deployed models first.</p><Button variant="danger" onClick={() => setConfirmDelete(true)} >{project.data?.settings?.deletion_in_progress === true ? "Retry project cleanup" : "Delete project"}</Button></Card>
    {confirmDelete && <ConfirmModal danger title={`Delete ${project.data?.name}?`} description="All project datasets, runs, saved models, and collaborator access will be permanently removed. This cannot be undone." confirmLabel="Delete project" close={() => setConfirmDelete(false)} action={() => remove.mutateAsync()} />}
  </>;
}
