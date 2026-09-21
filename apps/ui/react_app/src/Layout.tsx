import { useQuery } from "@tanstack/react-query";
import {
  Activity, BarChart3, Boxes, ChevronDown, Database, FolderKanban, Gauge,
  Home, LogOut, Menu, Settings, ShieldCheck, UserRound, Users, X,
} from "lucide-react";
import { useState } from "react";
import { Link, NavLink, Outlet, useLocation, useNavigate, useParams } from "react-router";
import { api, getSession, signOut } from "./api";
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
  const { pathname } = useLocation();
  const [open, setOpen] = useState(false);
  const session = getSession()!;
  const projects = useQuery({ queryKey: ["projects"], queryFn: () => api<Project[]>("/projects") });
  const current = projects.data?.find((project) => project.id === projectId);
  const section = projectId ? pathname.split("/")[3] || "" : pathname.split("/")[1];
  const pageTitle = nav.find((item) => projectId && item.to === section)?.label
    || ({ projects: "Projects", monitoring: "Governance dashboard", account: "Profile & security", settings: "Project settings" }[section])
    || "Workspace";

  async function logout() { await signOut(); navigate("/"); }

  return <div className="shell">
    <aside className={cx("sidebar", open && "sidebar--open")}>
      <div className="sidebar__head">
        <NavLink to="/projects" className="brand"><i className="brand-mark"><img src="/sceptre-icon.png" alt="" /></i><span>Sceptre <b>AI</b></span></NavLink>
        <button className="icon-button sidebar__close" onClick={() => setOpen(false)} aria-label="Close menu"><X /></button>
      </div>
      <nav className="sidebar__nav sidebar__nav--portfolio" aria-label="Portfolio navigation">
        <span>Portfolio</span>
        <NavLink to="/projects" end onClick={() => setOpen(false)}><i className="nav-icon"><FolderKanban size={17} /></i><span>Projects</span></NavLink>
      </nav>
      {projectId && <>
        <div className="project-switcher">
          <span>Current project</span>
          <button onClick={() => navigate("/projects")}><i>{initials(current?.name)}</i>
            <span><b>{current?.name || "Loading…"}</b><small>Switch project</small></span><ChevronDown size={15} /></button>
        </div>
        <nav className="sidebar__nav" aria-label="Project navigation">
          <span>Workspace</span>
          {nav.map(({ to, label, icon: Icon, end }) =>
            <NavLink key={label} to={`/projects/${projectId}${to ? `/${to}` : ""}`} end={end} onClick={() => setOpen(false)}>
              <i className="nav-icon"><Icon size={17} /></i><span>{label}</span>
            </NavLink>)}
          <span>Management</span>
          <NavLink to={`/projects/${projectId}/settings`} onClick={() => setOpen(false)}><i className="nav-icon"><Settings size={17} /></i><span>Project settings</span></NavLink>
        </nav>
      </>}
      {!projectId && <div className="sidebar__pitch"><ShieldCheck /><b>Private by design</b><p>Project access and model lineage stay governed at every step.</p></div>}
      <nav className="sidebar__account" aria-label="Account and governance">
        <NavLink to="/monitoring" onClick={() => setOpen(false)}><i className="nav-icon"><ShieldCheck size={17} /></i><span>Governance dashboard</span></NavLink>
        <NavLink to="/account" onClick={() => setOpen(false)}><i className="nav-icon"><UserRound size={17} /></i><span>Profile & security</span></NavLink>
      </nav>
      <div className="sidebar__user"><div className="avatar">{initials(session.user.full_name, session.user.email)}</div>
        <div><b>{session.user.full_name || "Sceptre user"}</b><small>{session.user.email}</small></div>
        <button className="icon-button" onClick={logout} title="Sign out" aria-label="Sign out"><LogOut size={17} /></button>
      </div>
    </aside>
    {open && <button className="sidebar-scrim" onClick={() => setOpen(false)} aria-label="Close menu" />}
    <main className="shell__main" id="main-content">
      <header className="topbar"><button className="icon-button topbar__menu" onClick={() => setOpen(true)} aria-label="Open menu"><Menu /></button>
        <div className="topbar__location"><div className="topbar__trail"><Link to="/projects" aria-label="All projects"><Home size={14} /></Link><span aria-hidden>/</span><span>{current?.name || "Workspace"}</span><span aria-hidden>/</span><span>{pageTitle}</span></div><b>{pageTitle}</b></div>
        <div className="topbar__actions"><Link to="/account" className="topbar__profile" aria-label="My account"><UserRound size={16} /><span>{session.user.full_name || "My account"}</span></Link><Link to={projectId ? `/projects/${projectId}/settings` : "/account"} className="icon-button" aria-label="Workspace settings"><Settings size={17} /></Link></div>
      </header>
      <div className="page"><Outlet /></div>
      <footer className="workspace-footer"><span><b>Sceptre AI</b> · Your model workspace</span><span>Data. Models. Decisions.</span></footer>
    </main>
  </div>;
}
