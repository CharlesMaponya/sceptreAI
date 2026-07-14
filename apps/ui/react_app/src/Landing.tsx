import {
  ArrowRight, BarChart3, Boxes, Check, ChevronRight, CloudCog, Cpu, Database,
  Fingerprint, Gauge, Menu, Network, ShieldCheck, Sparkles, Target, X,
} from "lucide-react";
import { useState } from "react";
import { Link } from "react-router-dom";
import { initials } from "./lib";
import { useAuthState } from "./useAuthState";

const outcomes = [
  {
    icon: Sparkles,
    title: "Ship sooner",
    text: "Move from raw data to ranked, validated candidates in one workflow—not after weeks of platform assembly.",
  },
  {
    icon: Target,
    title: "Choose with evidence",
    text: "Go beyond a headline score with diagnostics, holdout results, external validation, and feature contributions.",
  },
  {
    icon: Cpu,
    title: "Protect shared infrastructure",
    text: "Estimate demand before launch, cap every job, and keep business-critical workloads in control.",
  },
];

const capabilities = [
  { icon: Database, stage: "Understand", title: "Know your data before you model it", text: "Full-dataset profiling reveals quality issues, inferred types, relationships, distributions, and practical preparation steps." },
  { icon: BarChart3, stage: "Compare", title: "Turn experiments into a decision", text: "Progressive leaderboards combine task-aware metrics, diagnostics, parameters, and experiment history in one evidence trail." },
  { icon: Fingerprint, stage: "Explain", title: "Make every model reviewable", text: "Challenge candidates on external data and use SHAP contributions to explain what drives their decisions." },
  { icon: CloudCog, stage: "Operate", title: "Go from winner to working endpoint", text: "Register, promote, deploy, monitor, stop, and safely fall back without passing model files between teams." },
];

const workflow = [
  ["01", "Bring the data", "Upload an immutable dataset version and inspect its quality."],
  ["02", "Frame the problem", "Confirm the task, target, candidate models, and experiment budget."],
  ["03", "Build the evidence", "Train, compare, validate, and explain the strongest candidates."],
  ["04", "Operate the winner", "Promote, deploy, monitor drift, and preserve a safe fallback."],
];

export function Landing() {
  const [menuOpen, setMenuOpen] = useState(false);
  const { user, isChecking, isAuthenticated } = useAuthState();
  const workspaceLink = isAuthenticated ? "/projects" : "/auth?mode=register";
  return <div className="marketing">
    <header className="marketing-nav">
      <Link to="/" className="brand"><i className="brand-mark"><img src="/sceptre-icon.png" alt="" /></i>
        <span>Sceptre <b>AI</b></span></Link>
      <button className="marketing-nav__menu" onClick={() => setMenuOpen(!menuOpen)}
        aria-label={menuOpen ? "Close navigation" : "Open navigation"}>
        {menuOpen ? <X /> : <Menu />}
      </button>
      <nav className={menuOpen ? "open" : ""} aria-label="Main navigation">
        <a href="#platform">Platform</a><a href="#workflow">How it works</a>
        <a href="#value">Business value</a><a href="#governance">Governance</a>
      </nav>
      <div className="marketing-nav__actions">
        {isChecking ? <span className="session-check" role="status"><i />Checking session…</span>
          : user ? <><Link className="nav-profile" to="/projects" aria-label={`Open ${user.full_name || user.email}'s workspace`}>
            <span className="avatar">{initials(user.full_name, user.email)}</span>
            <span><b>{user.full_name?.split(" ")[0] || "Workspace"}</b><small>Signed in</small></span>
          </Link><Link className="button button--secondary" to="/projects">Open workspace</Link></> : <>
          <Link className="marketing-signin" to="/auth">Sign in</Link>
          <Link className="button button--primary" to="/auth?mode=register">Start building</Link>
        </>}
      </div>
    </header>

    <main>
      <section className="marketing-hero">
        <div className="marketing-hero__copy">
          <span className="marketing-kicker"><i />Governed AutoML for teams that mean business</span>
          <h1>Build models your business can trust   <em>and put them to work.</em></h1>
          <p>From raw table to governed production endpoint, Sceptre gives growing data teams one place to profile, train, compare, validate, explain, and deploy on infrastructure they control.</p>
          <div className="marketing-hero__actions">
            <Link className="button button--primary marketing-cta" to={workspaceLink}>
              {isAuthenticated ? "Open your workspace" : "Build your first model"}<ArrowRight size={17} />
            </Link>
            <a className="button button--secondary marketing-cta" href="#workflow">See how it works<ChevronRight size={17} /></a>
          </div>
          <div className="marketing-proof">
            <span><Check />Infrastructure you control</span>
            <span><Check />Evidence attached by default</span>
            <span><Check />No black-box hand-offs</span>
          </div>
        </div>
        <div className="product-preview" aria-label="Sceptre model workspace preview">
          <div className="product-preview__top"><span><i /><i /><i /></span><b>Customer retention</b><small>Platform online</small></div>
          <div className="product-preview__body">
            <div className="preview-sidebar"><div className="preview-brand"><img src="/sceptre-icon.png" alt="" /></div>
              {[Gauge, Database, Boxes, BarChart3, CloudCog].map((Icon, index) =>
                <i className={index === 3 ? "active" : ""} key={index}><Icon /></i>)}</div>
            <div className="preview-main"><span>Evidence workspace</span><h2>Model leaderboard</h2>
              <div className="preview-metrics"><i><small>Top candidate</small><b>GradientBoosting</b></i>
                <i><small>Balanced accuracy</small><b>0.9142</b></i><i><small>Models compared</small><b>8</b></i></div>
              <div className="preview-chart"><div><span>GradientBoosting</span><i><b style={{ width: "91%" }} /></i><strong>0.914</strong></div>
                <div><span>RandomForest</span><i><b style={{ width: "88%" }} /></i><strong>0.881</strong></div>
                <div><span>LogisticRegression</span><i><b style={{ width: "82%" }} /></i><strong>0.823</strong></div></div>
              <div className="preview-status"><ShieldCheck /><span><b>Validation passed</b><small>Evidence ready for review</small></span><em>View report</em></div>
            </div>
          </div>
          <div className="preview-float"><Network /><span><small>Deployment</small><b>Endpoint ready</b></span><i /></div>
        </div>
      </section>

      <section className="control-strip" aria-label="Platform principles">
        <span>Your cloud.</span><i /> <span>Your data.</span><i /> <strong>Your rules.</strong>
        <p>Kubernetes-native · Project-isolated · Audit-ready</p>
      </section>

      <section className="marketing-section outcomes" id="value">
        <div className="marketing-heading"><span>Business value</span>
          <h2>Production discipline without the platform tax.</h2>
          <p>Sceptre removes the glue work between experimentation and operation, so your team can focus on decisions—not infrastructure assembly.</p></div>
        <div className="outcome-grid">{outcomes.map(({ icon: Icon, title, text }) =>
          <article key={title}><Icon /><h3>{title}</h3><p>{text}</p></article>)}</div>
      </section>

      <section className="marketing-section capability-section" id="platform">
        <div className="marketing-heading marketing-heading--left"><span>The complete model journey</span>
          <h2>Most AutoML tools stop at a leaderboard.<br />Sceptre closes the gap.</h2></div>
        <div className="capability-grid">{capabilities.map(({ icon: Icon, stage, title, text }) =>
          <article key={stage}><div><Icon /></div><span>{stage}</span><h3>{title}</h3><p>{text}</p>
            <a href="#workflow">Explore the workflow <ArrowRight size={14} /></a></article>)}</div>
      </section>

      <section className="marketing-section workflow-section" id="workflow">
        <div className="workflow-copy"><span className="marketing-kicker"><i />One connected workflow</span>
          <h2>Every step leaves the next team better prepared.</h2>
          <p>Data context, training parameters, model evidence, artifacts, and operational state stay connected to the project. No mystery files. No lost notebook history.</p>
          <ul><li><Check />Immutable dataset versions and hashes</li>
            <li><Check />Reproducible experiments backed by MLflow</li>
            <li><Check />Explicit promotion and fallback controls</li>
            <li><Check />Project-scoped access and lineage</li></ul>
        </div>
        <div className="workflow-steps">{workflow.map(([number, title, text]) =>
          <article key={number}><i>{number}</i><div><h3>{title}</h3><p>{text}</p></div></article>)}</div>
      </section>

      <section className="governance-section" id="governance">
        <div><span>Built for trust</span><h2>Control is not an enterprise add-on. It is the foundation.</h2>
          <p>Sceptre runs on infrastructure you control and keeps project access, model lineage, resource limits, promotion state, and operational evidence attached from day one.</p>
          <Link className="button button--secondary marketing-cta" to={workspaceLink}>
            See Sceptre in action<ArrowRight size={16} /></Link></div>
        <div className="governance-card"><ShieldCheck /><h3>Governance by design</h3>
          <ul><li><span>Project isolation</span><b>Every artifact carries project lineage</b></li>
            <li><span>Resource fairness</span><b>Capacity checked before every launch</b></li>
            <li><span>Human review</span><b>Promotion remains explicit and auditable</b></li>
            <li><span>Safe operation</span><b>Fallback, drift, stop, and cleanup controls</b></li></ul></div>
      </section>

      <section className="closing-cta"><span className="closing-mark"><img src="/sceptre-icon.png" alt="" /></span>
        <span>Move beyond the experiment</span><h2>Build models your business can actually trust and operate.</h2>
        <p>Give your team one governed path from raw table to production endpoint.</p>
        <Link className="button button--primary marketing-cta" to={workspaceLink}>
          {isAuthenticated ? "Return to your workspace" : "Start building with Sceptre"}<ArrowRight size={17} /></Link></section>
    </main>

    <footer className="marketing-footer"><Link to="/" className="brand"><i className="brand-mark"><img src="/sceptre-icon.png" alt="" /></i>
      <span>Sceptre <b>AI</b></span></Link><p>Governed tabular AutoML, from evidence to endpoint.</p>
      <div><a href="#platform">Platform</a><a href="#workflow">Workflow</a>
        <a href="#governance">Governance</a>
        <Link to={isAuthenticated ? "/projects" : "/auth"}>{isAuthenticated ? "Workspace" : "Sign in"}</Link></div>
      <small>© {new Date().getFullYear()} Sceptre AI. Built for teams that take model trust seriously.</small>
    </footer>
  </div>;
}
