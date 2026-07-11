import { FormEvent, useState } from "react";
import { ArrowRight, BarChart3, Check, Database, ShieldCheck } from "lucide-react";
import { authenticate } from "./api";
import { Button, Notice } from "./components/ui";
import { useNavigate, useSearchParams } from "react-router-dom";

export function Auth() {
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const [mode, setMode] = useState<"login" | "register">(
    searchParams.get("mode") === "register" ? "register" : "login",
  );
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [success, setSuccess] = useState("");

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setError("");
    setLoading(true);
    const data = Object.fromEntries(new FormData(event.currentTarget));
    try {
      await authenticate(mode, data as Record<string, string>);
      if (mode === "register") {
        setMode("login");
        setSuccess("Account created successfully. Sign in to continue.");
        navigate("/auth", { replace: true });
      }
    }
    catch (cause) { setError(cause instanceof Error ? cause.message : "Unable to authenticate."); }
    finally { setLoading(false); }
  }

  return <main className="auth">
    <section className="auth__story">
      <div className="brand brand--light"><i className="brand-mark"><img src="/sceptre-icon-white.png" alt="" /></i><span>Sceptre <b>AI</b></span></div>
      <div className="auth__copy">
        <span className="eyebrow eyebrow--light">Governed AutoML, end to end</span>
        <h1>Turn your data into decisions you can defend.</h1>
        <p>Profile, train, explain, and deploy production-ready models from one focused workspace.</p>
        <ul>
          <li><Database /><span><b>Understand every dataset</b><small>Quality signals and preparation guidance before training.</small></span></li>
          <li><BarChart3 /><span><b>Compare models with evidence</b><small>Progressive leaderboards, validation, and explainability.</small></span></li>
          <li><ShieldCheck /><span><b>Operate with confidence</b><small>Governed promotion, drift checks, and deployment controls.</small></span></li>
        </ul>
      </div>
      <p className="auth__foot"><Check size={14} /> Your data stays on infrastructure you control.</p>
    </section>
    <section className="auth__panel">
      <form className="auth-form" onSubmit={submit}>
        <div className="auth-form__mobile-brand brand"><i className="brand-mark"><img src="/sceptre-icon.png" alt="" /></i><span>Sceptre <b>AI</b></span></div>
        <span className="eyebrow">{mode === "login" ? "Welcome back" : "Get started"}</span>
        <h2>{mode === "login" ? "Sign in to Sceptre" : "Create your account"}</h2>
        <p>{mode === "login" ? "Continue building reliable models." : "Set up your governed ML workspace in minutes."}</p>
        {success && <Notice tone="success">{success}</Notice>}
        {error && <Notice tone="danger">{error}</Notice>}
        {mode === "register" && <label>Full name<input name="full_name" autoComplete="name" placeholder="Ada Lovelace" /></label>}
        <label>Work email<input name="email" type="email" autoComplete="email" required placeholder="you@company.com" /></label>
        <label>Password<input name="password" type="password" minLength={mode === "register" ? 8 : 1} autoComplete={mode === "login" ? "current-password" : "new-password"} required placeholder="••••••••" /></label>
        <Button type="submit" loading={loading}>{mode === "login" ? "Sign in" : "Create account"}<ArrowRight size={16} /></Button>
        <p className="auth-form__switch">{mode === "login" ? "New to Sceptre?" : "Already have an account?"}
          <button type="button" onClick={() => { setMode(mode === "login" ? "register" : "login"); setError(""); setSuccess(""); }}>
            {mode === "login" ? "Create an account" : "Sign in"}
          </button>
        </p>
      </form>
    </section>
  </main>;
}
