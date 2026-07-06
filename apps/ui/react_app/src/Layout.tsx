import { useQuery } from "@tanstack/react-query";
import {
  Activity, BarChart3, Boxes, ChevronDown, Database, FolderKanban, Gauge,
  LogOut, Menu, Settings, ShieldCheck, Users, X,
} from "lucide-react";
import { useState } from "react";
import { NavLink, Outlet, useNavigate, useParams } from "react-router-dom";
import { api, getSession, signOut } from "./api";
import { Badge } from "./components/ui";
import { cx, initials } from "./lib";
import type { Project } from "./types";

const nav = [
  { to: "", label: "Overview", icon: Gauge, end: true },
  { to: "data", label: "Data", icon: Database },
  { to: "training", label: "Train", icon: Boxes },
  { to: "runs", label: "Results & validation", icon: BarChart3 },
  { to: "operations", label: "Deploy & monitor", icon: Activity },
  { to: "members", label: "Team", icon: Users },
];

export function Layout() {
  const { projectId } = useParams();
  const navigate = useNavigate();
  const [open, setOpen] = useState(false);
  const session = getSession()!;
  const projects = useQuery({ queryKey: ["projects"], queryFn: () => api<Project[]>("/projects") });
  const current = projects.data?.find((project) => project.id === projectId);

  async function logout() { await signOut(); navigate("/"); }

  return <div className="shell">
    <aside className={cx("sidebar", open && "sidebar--open")}>
      <div className="sidebar__head">
        <NavLink to="/projects" className="brand"><i className="brand-mark"><img src="/sceptre-icon.png" alt="" /></i><span>Sceptre <b>AI</b></span></NavLink>
        <button className="icon-button sidebar__close" onClick={() => setOpen(false)} aria-label="Close menu"><X /></button>
      </div>
      {projectId && <>
        <div className="project-switcher">
          <span>Current project</span>
          <button onClick={() => navigate("/projects")}><i>{initials(current?.name)}</i>
            <span><b>{current?.name || "Loading…"}</b><small>Switch project</small></span><ChevronDown size={15} /></button>
        </div>
        <nav className="sidebar__nav" aria-label="Project navigation">
          <span>Workspace</span>
          {nav.map(({ to, label, icon: Icon, end }) =>
            <NavLink key={label} to={`/projects/${projectId}/${to}`} end={end} onClick={() => setOpen(false)}>
              <Icon size={18} /><span>{label}</span>
            </NavLink>)}
          <span>Management</span>
          <NavLink to={`/projects/${projectId}/settings`}><Settings size={18} /><span>Project settings</span></NavLink>
        </nav>
      </>}
      {!projectId && <div className="sidebar__pitch"><ShieldCheck /><b>Private by design</b><p>Project access and model lineage stay governed at every step.</p></div>}
      <div className="sidebar__user"><div className="avatar">{initials(session.user.full_name, session.user.email)}</div>
        <div><b>{session.user.full_name || "Sceptre user"}</b><small>{session.user.email}</small></div>
        <button className="icon-button" onClick={logout} title="Sign out" aria-label="Sign out"><LogOut size={17} /></button>
      </div>
    </aside>
    {open && <button className="sidebar-scrim" onClick={() => setOpen(false)} aria-label="Close menu" />}
    <div className="shell__main">
      <header className="topbar"><button className="icon-button topbar__menu" onClick={() => setOpen(true)} aria-label="Open menu"><Menu /></button>
        <div className="topbar__trail"><FolderKanban size={17} /><span>{current?.name || "Projects"}</span></div>
        <div className="topbar__status"><Badge status="ok">Platform online</Badge></div>
      </header>
      <div className="page"><Outlet /></div>
    </div>
  </div>;
}
