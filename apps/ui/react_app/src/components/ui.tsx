import { useEffect, type ButtonHTMLAttributes, type HTMLAttributes, type ReactNode } from "react";
import { AlertCircle, CheckCircle2, LoaderCircle, Plus } from "lucide-react";
import { cx, titleCase } from "../lib";

export function Button({
  variant = "primary", loading, children, className, ...props
}: ButtonHTMLAttributes<HTMLButtonElement> & {
  variant?: "primary" | "secondary" | "ghost" | "danger"; loading?: boolean;
}) {
  return (
    <button className={cx("button", `button--${variant}`, className)} disabled={loading || props.disabled} {...props}>
      {loading && <LoaderCircle size={16} className="spin" aria-hidden />}
      {children}
    </button>
  );
}

export function Card({ className, ...props }: HTMLAttributes<HTMLDivElement>) {
  return <div className={cx("card", className)} {...props} />;
}

export function Badge({ status, children }: { status?: string; children?: ReactNode }) {
  const tone = ["succeeded", "active", "ready", "production", "ok"].includes(status || "")
    ? "success" : ["failed", "cancelled", "preempted", "degraded"].includes(status || "")
      ? "danger" : ["running", "precheck_running", "queued", "staging"].includes(status || "")
        ? "info" : "neutral";
  return <span className={cx("badge", `badge--${tone}`)}><i />{children || titleCase(status || "unknown")}</span>;
}

export function PageHeader({ eyebrow, title, description, action }: {
  eyebrow?: string; title: string; description?: string; action?: ReactNode;
}) {
  return (
    <header className="page-header">
      <div>{eyebrow && <span className="eyebrow">{eyebrow}</span>}<h1>{title}</h1>{description && <p>{description}</p>}</div>
      {action && <div className="page-header__action">{action}</div>}
    </header>
  );
}

export function EmptyState({ icon, title, description, action }: {
  icon?: ReactNode; title: string; description: string; action?: ReactNode;
}) {
  return <div className="empty-state">{icon || <Plus aria-hidden />}<h3>{title}</h3><p>{description}</p>{action}</div>;
}

export function Loading({ label = "Loading workspace…" }: { label?: string }) {
  return <div className="loading"><LoaderCircle className="spin" aria-hidden /><span>{label}</span></div>;
}

export function Notice({ tone = "info", children }: {
  tone?: "info" | "danger" | "success"; children: ReactNode;
}) {
  const Icon = tone === "danger" ? AlertCircle : tone === "success" ? CheckCircle2 : AlertCircle;
  return <div className={cx("notice", `notice--${tone}`)} role={tone === "danger" ? "alert" : "status"}>
    <Icon size={18} aria-hidden /><div>{children}</div>
  </div>;
}

export function Metric({ label, value, hint }: { label: string; value: ReactNode; hint?: string }) {
  return <div className="metric"><span>{label}</span><strong>{value}</strong>{hint && <small>{hint}</small>}</div>;
}

export function Modal({ title, description, children, onClose }: {
  title: string; description?: string; children: ReactNode; onClose: () => void;
}) {
  useEffect(() => {
    const closeOnEscape = (event: KeyboardEvent) => {
      if (event.key === "Escape") onClose();
    };
    document.addEventListener("keydown", closeOnEscape);
    return () => document.removeEventListener("keydown", closeOnEscape);
  }, [onClose]);
  return <div className="modal-backdrop" onMouseDown={onClose}>
    <section className="modal" role="dialog" aria-modal="true" aria-labelledby="modal-title" onMouseDown={(e) => e.stopPropagation()}>
      <button type="button" className="modal__close" onClick={onClose} aria-label="Close">×</button>
      <h2 id="modal-title">{title}</h2>{description && <p className="muted">{description}</p>}{children}
    </section>
  </div>;
}

export function ErrorState({ error, retry }: { error: Error; retry?: () => void }) {
  return <Notice tone="danger"><strong>Something went wrong</strong><p>{error.message}</p>
    {retry && <Button variant="secondary" onClick={retry}>Try again</Button>}</Notice>;
}
