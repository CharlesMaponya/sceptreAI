import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { FormEvent } from "react";
import { useParams } from "react-router-dom";
import { api, json } from "../api";
import { Button, Card, ErrorState, Loading, Notice, PageHeader } from "../components/ui";
import type { Project } from "../types";

export function SettingsPage() {
  const { projectId = "" } = useParams();
  const client = useQueryClient();
  const project = useQuery({ queryKey: ["project", projectId], queryFn: () => api<Project>(`/projects/${projectId}`) });
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
    <Card className="settings-card"><form className="stack" onSubmit={submit}><div><h2>General</h2><p className="muted">Names and descriptions appear throughout the workspace.</p></div>
      <label>Project name<input name="name" required maxLength={180} defaultValue={project.data?.name} /></label>
      <label>Description<textarea name="description" rows={4} defaultValue={project.data?.description || ""} /></label>
      <label>Status<select name="status" defaultValue={project.data?.status}><option value="active">Active</option><option value="archived">Archived</option></select></label>
      {update.isSuccess && <Notice tone="success">Project settings saved.</Notice>}{update.error && <Notice tone="danger">{update.error.message}</Notice>}
      <div><Button loading={update.isPending}>Save changes</Button></div></form></Card></>;
}
