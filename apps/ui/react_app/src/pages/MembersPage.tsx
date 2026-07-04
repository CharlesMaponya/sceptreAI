import { useMutation, useQuery } from "@tanstack/react-query";
import { Check, Copy, Link2, Users } from "lucide-react";
import { useState } from "react";
import { useParams } from "react-router-dom";
import { api, json } from "../api";
import { Badge, Button, Card, EmptyState, ErrorState, Loading, Notice, PageHeader } from "../components/ui";
import { formatDate, initials, titleCase } from "../lib";
import type { Member } from "../types";

interface ShareLink { invite_token: string; role: string; expires_at: string; max_uses: number }

export function MembersPage() {
  const { projectId = "" } = useParams();
  const [role, setRole] = useState("viewer");
  const [days, setDays] = useState(7);
  const [copied, setCopied] = useState(false);
  const members = useQuery({ queryKey: ["members", projectId], queryFn: () => api<Member[]>(`/projects/${projectId}/members`) });
  const invite = useMutation({ mutationFn: () => api<ShareLink>(`/projects/${projectId}/share-links`, json("POST", { role, permissions: {}, expires_in_days: days, max_uses: 1 })) });
  async function copy() { if (!invite.data) return; await navigator.clipboard.writeText(invite.data.invite_token); setCopied(true); window.setTimeout(() => setCopied(false), 2000); }
  return <>
    <PageHeader eyebrow="Project access" title="Team" description="Invite collaborators with an explicit project role and time-limited link." />
    <div className="team-layout"><Card className="section-card"><div className="section-heading"><div><h2>Project members</h2><p>{members.data?.length || 0} people can access this workspace.</p></div><Users className="section-icon" /></div>
      {members.isLoading ? <Loading /> : members.error ? <ErrorState error={members.error} retry={() => members.refetch()} /> : members.data?.length ?
        <div className="member-list">{members.data.map((member) => <div key={member.id}><div className="avatar">{initials(member.full_name, member.email)}</div><span><b>{member.full_name || "Project member"}</b><small>{member.email}</small></span><Badge>{titleCase(member.role)}</Badge><small>Joined {formatDate(member.accepted_at)}</small></div>)}</div>
        : <EmptyState title="No collaborators yet" description="Create a secure invite link to bring your team into this workspace." />}</Card>
      <Card className="invite-card"><Link2 /><h2>Invite a collaborator</h2><p>Create a single-use, expiring token. You choose the level of access.</p>
        <label>Project role<select value={role} onChange={(e) => setRole(e.target.value)}><option value="viewer">Viewer · read only</option><option value="editor">Editor · data and training</option><option value="owner">Owner · full access</option></select></label>
        <label>Link expires in<select value={days} onChange={(e) => setDays(Number(e.target.value))}><option value={1}>1 day</option><option value={7}>7 days</option><option value={14}>14 days</option><option value={30}>30 days</option></select></label>
        {invite.error && <Notice tone="danger">{invite.error.message}</Notice>}
        {invite.data ? <div className="token-box"><span>{invite.data.invite_token}</span><Button variant="secondary" onClick={copy}>{copied ? <Check size={15} /> : <Copy size={15} />}{copied ? "Copied" : "Copy"}</Button><small>Expires {formatDate(invite.data.expires_at)} · Share this token securely.</small></div>
          : <Button className="full" loading={invite.isPending} onClick={() => invite.mutate()}>Create invite link</Button>}</Card></div>
  </>;
}
