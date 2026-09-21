import { useMutation, useQuery } from "@tanstack/react-query";
import { Check, Copy, Link2, Users } from "lucide-react";
import { useState } from "react";
import { useParams } from "react-router";
import { api, json } from "../api";
import { Badge, Button, Card, ConfirmModal, EmptyState, ErrorState, Loading, Notice, PageHeader } from "../components/ui";
import { formatDate, initials, titleCase } from "../lib";
import { Pagination } from "../components/Pagination";
import type { Member } from "../types";

interface Invitation { id: string; role: string; expires_at: string; max_uses: number; used_count: number; revoked_at: string | null }
interface ShareLink extends Invitation { invite_token: string }
const PAGE_SIZE = 10;

export function MembersPage() {
  const { projectId = "" } = useParams();
  const [role, setRole] = useState("viewer");
  const [days, setDays] = useState(7);
  const [copied, setCopied] = useState(false);
  const [memberPage, setMemberPage] = useState(0);
  const [invitePage, setInvitePage] = useState(0);
  const [removeTarget, setRemoveTarget] = useState<Member | null>(null);
  const [revokeTarget, setRevokeTarget] = useState<Invitation | null>(null);
  const invitations = useQuery({ queryKey: ["invitations", projectId, invitePage], queryFn: () => api<Invitation[]>(`/projects/${projectId}/share-links?offset=${invitePage * PAGE_SIZE}&limit=${PAGE_SIZE + 1}`) });
  const members = useQuery({ queryKey: ["members", projectId, memberPage], queryFn: () => api<Member[]>(`/projects/${projectId}/members?offset=${memberPage * PAGE_SIZE}&limit=${PAGE_SIZE + 1}`) });
  const invite = useMutation({ mutationFn: () => api<ShareLink>(`/projects/${projectId}/share-links`, json("POST", { role, permissions: {}, expires_in_days: days, max_uses: 1 })), onSuccess: () => { setInvitePage(0); invitations.refetch(); } });
  const revoke = useMutation({ mutationFn: () => api(`/projects/${projectId}/share-links/${revokeTarget!.id}`, json("DELETE")), onSuccess: () => { if (invite.data?.id === revokeTarget?.id) invite.reset(); setRevokeTarget(null); invitations.refetch(); } });
  const remove = useMutation({ mutationFn: () => api(`/projects/${projectId}/members/${removeTarget!.id}`, json("DELETE")), onSuccess: () => { setRemoveTarget(null); setMemberPage(0); members.refetch(); } });
  async function copy() { if (!invite.data) return; await navigator.clipboard.writeText(invite.data.invite_token); setCopied(true); window.setTimeout(() => setCopied(false), 2000); }
  return <>
    <PageHeader eyebrow="Project access" title="Team" description="Invite collaborators with an explicit project role and time-limited token." />
    <div className="team-layout"><Card className="section-card"><div className="section-heading"><div><h2>Project members</h2><p>Manage the people who can access this workspace.</p></div><Users className="section-icon" /></div>
      {members.isLoading ? <Loading /> : members.error ? <ErrorState error={members.error} retry={() => members.refetch()} /> : members.data?.length ?
        <div className="member-list">{members.data.slice(0, PAGE_SIZE).map((member) => <div key={member.id}><div className="avatar">{initials(member.full_name, member.email)}</div><span><b>{member.full_name || "Project member"}</b><small>{member.email}</small></span><Badge>{titleCase(member.role)}</Badge><small>Joined {formatDate(member.accepted_at)}</small>{member.role !== "owner" && <Button variant="ghost" onClick={() => setRemoveTarget(member)}>Remove access</Button>}</div>)}</div>
        : <EmptyState title="No collaborators yet" description="Create a secure invite token to bring your team into this workspace." />}
      <Pagination label="Members" page={memberPage} hasNext={(members.data?.length || 0) > PAGE_SIZE} onChange={setMemberPage} loading={members.isFetching} /></Card>
      <Card className="invite-card"><Link2 /><h2>Invite a collaborator</h2><p>Create a single-use, expiring token. You choose the level of access.</p>
        <label>Project role<select value={role} onChange={(e) => setRole(e.target.value)}><option value="viewer">Viewer · read only</option><option value="editor">Editor · data and training</option><option value="admin">Admin · manage operations</option><option value="owner">Owner · full access</option></select></label>
        <label>Token expires in<select value={days} onChange={(e) => setDays(Number(e.target.value))}><option value={1}>1 day</option><option value={7}>7 days</option><option value={14}>14 days</option><option value={30}>30 days</option></select></label>
        {invite.error && <Notice tone="danger">{invite.error.message}</Notice>}
        {invite.data ? <div className="token-box"><span>{invite.data.invite_token}</span><Button variant="secondary" onClick={copy}>{copied ? <Check size={15} /> : <Copy size={15} />}{copied ? "Copied" : "Copy"}</Button><small>Expires {formatDate(invite.data.expires_at)} · Share this token securely.</small><Button variant="ghost" onClick={() => { invite.reset(); setCopied(false); }}>Create another invite</Button></div>
          : <Button className="full" loading={invite.isPending} onClick={() => invite.mutate()}>Create invite token</Button>}</Card></div>
    <Card className="section-card"><h2>Invitation history</h2><p>Revoking an invitation prevents future use. Remove a member's access separately after they have joined.</p>
      {invitations.isLoading ? <Loading /> : invitations.error ? <ErrorState error={invitations.error} retry={() => invitations.refetch()} /> : invitations.data?.length ? <div className="table-wrap"><table><thead><tr><th>Role</th><th>Expires</th><th>Status</th><th>Actions</th></tr></thead><tbody>{invitations.data.slice(0, PAGE_SIZE).map((item) => {
        const state = item.revoked_at ? "Revoked" : item.used_count >= item.max_uses ? "Used" : new Date(item.expires_at) <= new Date() ? "Expired" : "Active";
        return <tr key={item.id}><td>{titleCase(item.role)}</td><td>{formatDate(item.expires_at)}</td><td>{state}</td><td>{state === "Active" && <Button variant="ghost" onClick={() => setRevokeTarget(item)}>Revoke invite</Button>}</td></tr>;
      })}</tbody></table></div> : <p className="muted">No invitations yet.</p>}
      <Pagination label="Invitations" page={invitePage} hasNext={(invitations.data?.length || 0) > PAGE_SIZE} onChange={setInvitePage} loading={invitations.isFetching} />
    </Card>
    {revokeTarget && <ConfirmModal danger title="Revoke this invitation?" description="This token will no longer grant project access. Existing members keep their access." confirmLabel="Revoke invite" close={() => setRevokeTarget(null)} action={() => revoke.mutateAsync()} />}
    {removeTarget && <ConfirmModal danger title="Remove project access?" description={`${removeTarget.email} will lose access to this project. Their account and work remain available.`} confirmLabel="Remove access" close={() => setRemoveTarget(null)} action={() => remove.mutateAsync()} />}
  </>;
}
