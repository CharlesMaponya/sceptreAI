import { useEffect, useEffectEvent, useRef, useState, type ButtonHTMLAttributes, type HTMLAttributes, type ReactNode } from "react";
import { AlertCircle, AlertTriangle, CheckCircle2, LoaderCircle, Plus } from "lucide-react";
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
  return <div className="loading" role="status" aria-live="polite">
    <div className="loading__skeleton" aria-hidden><i /><i /><i /></div>
    <span className="loading__label">{label}</span>
  </div>;
}

export function Notice({ tone = "info", children }: {
  tone?: "info" | "danger" | "success"; children: ReactNode;
}) {
  const Icon = tone === "danger" ? AlertCircle : tone === "success" ? CheckCircle2 : AlertCircle;
  return <div className={cx("notice", `notice--${tone}`)} role={tone === "danger" ? "alert" : "status"}>
    <Icon size={18} aria-hidden /><div>{children}</div>
  </div>;
}

export function Metric({ label, value, hint, icon }: { label: string; value: ReactNode; hint?: string; icon?: ReactNode }) {
  return <div className={cx("metric", Boolean(icon) && "metric--icon")}><span>{label}</span><strong>{value}</strong>{hint && <small>{hint}</small>}{icon && <i className="metric__icon" aria-hidden>{icon}</i>}</div>;
}

export function Modal({ title, description, children, onClose }: {
  title: string; description?: string; children: ReactNode; onClose: () => void;
}) {
  const closeButton = useRef<HTMLButtonElement>(null);
  const dialog = useRef<HTMLElement>(null);
  // Capture the trigger before a child's autoFocus runs during mounting.
  const [previousFocus] = useState(() => document.activeElement instanceof HTMLElement ? document.activeElement : null);
  const close = useEffectEvent(onClose);
  useEffect(() => {
    const closeOnEscape = (event: KeyboardEvent) => {
      if (event.key === "Escape") close();
      if (event.key !== "Tab" || !dialog.current) return;
      const focusable = Array.from(dialog.current.querySelectorAll<HTMLElement>(
        'a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])',
      ));
      if (!focusable.length) return event.preventDefault();
      const first = focusable[0];
      const last = focusable[focusable.length - 1];
      if (event.shiftKey && document.activeElement === first) {
        event.preventDefault(); last.focus();
      } else if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault(); first.focus();
      }
    };
    const previousOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    document.addEventListener("keydown", closeOnEscape);
    closeButton.current?.focus();
    return () => {
      document.removeEventListener("keydown", closeOnEscape);
      document.body.style.overflow = previousOverflow;
      previousFocus?.focus();
    };
  }, [previousFocus]);
  return <div className="modal-backdrop" onMouseDown={onClose}>
    <section ref={dialog} className="modal" role="dialog" aria-modal="true" aria-labelledby="modal-title"
      aria-describedby={description ? "modal-description" : undefined} onMouseDown={(e) => e.stopPropagation()}>
      <button ref={closeButton} type="button" className="modal__close" onClick={onClose} aria-label="Close">×</button>
      <h2 id="modal-title">{title}</h2>{description && <p id="modal-description" className="muted">{description}</p>}{children}
    </section>
  </div>;
}

export function ErrorState({ error, retry }: { error: Error; retry?: () => void }) {
  return <Notice tone="danger"><strong>Something went wrong</strong><p>{error.message}</p>
    {retry && <Button variant="secondary" onClick={retry}>Try again</Button>}</Notice>;
}

export function ConfirmModal({ title, description, confirmLabel, action, close, danger = false }: {
  title: string; description: string; confirmLabel: string; action: () => Promise<unknown>;
  close: () => void; danger?: boolean;
}) {
  const [pending, setPending] = useState(false);
  const [error, setError] = useState("");
  async function confirm() {
    setPending(true); setError("");
    try { await action(); } catch (cause) {
      setError(cause instanceof Error ? cause.message : "The action failed."); setPending(false);
    }
  }
  return <Modal title={title} description={description} onClose={close}>
    <div className="stack">{danger && <Notice tone="danger"><AlertTriangle size={16} />
      Review the impact before continuing.</Notice>}{error && <Notice tone="danger">{error}</Notice>}
      <div className="modal__actions"><Button variant="ghost" disabled={pending} onClick={close}>Cancel</Button>
        <Button variant={danger ? "danger" : "primary"} loading={pending} onClick={confirm}>{confirmLabel}</Button></div>
    </div>
  </Modal>;
}
