import { FormEvent, useState } from "react";
import { ArrowLeft, ArrowRight, BarChart3, Check, Database, KeyRound, ShieldCheck } from "lucide-react";
import {
  authenticate, confirmPasswordReset, requestPasswordReset,
} from "./api";
import { Button, Notice } from "./components/ui";
import { useNavigate, useSearchParams } from "react-router-dom";

type AuthMode = "login" | "register" | "forgot" | "reset";

const content: Record<AuthMode, { eyebrow: string; title: string; description: string }> = {
  login: { eyebrow: "Welcome back", title: "Sign in to Sceptre", description: "Continue building reliable models." },
  register: { eyebrow: "Get started", title: "Create your account", description: "Set up your governed ML workspace in minutes." },
  forgot: { eyebrow: "Account recovery", title: "Reset your password", description: "We’ll prepare a secure, single-use reset link." },
  reset: { eyebrow: "Choose a new password", title: "Secure your account", description: "Use at least eight characters that you have not used here before." },
};

export function Auth() {
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const requestedMode = searchParams.get("mode");
  const initialMode: AuthMode = ["register", "forgot", "reset"].includes(requestedMode || "")
    ? requestedMode as AuthMode : "login";
  const [mode, setModeState] = useState<AuthMode>(initialMode);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [success, setSuccess] = useState("");
  const [devResetToken, setDevResetToken] = useState("");

  function setMode(next: AuthMode, token?: string) {
    setModeState(next);
    setError("");
    setSuccess("");
    const query = next === "login" ? "" : `?mode=${next}${token ? `&token=${encodeURIComponent(token)}` : ""}`;
    navigate(`/auth${query}`, { replace: true });
  }

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setError("");
    setSuccess("");
    const data = Object.fromEntries(new FormData(event.currentTarget)) as Record<string, string>;
    if ((mode === "register" || mode === "reset") && data.password !== data.confirm_password) {
      setError("The passwords do not match.");
      return;
    }
    setLoading(true);
    try {
      if (mode === "login") {
        await authenticate("login", { email: data.email, password: data.password });
        navigate("/projects", { replace: true });
      } else if (mode === "register") {
        await authenticate("register", {
          full_name: data.full_name, email: data.email, password: data.password,
        });
        setModeState("login");
        setSuccess("Account created successfully. Sign in to continue.");
        navigate("/auth", { replace: true });
      } else if (mode === "forgot") {
        const response = await requestPasswordReset(data.email);
        setSuccess(response.message);
        setDevResetToken(response.reset_token_for_dev || "");
      } else {
        const token = searchParams.get("token") || "";
        if (!token) throw new Error("This reset link is incomplete. Request a new one.");
        await confirmPasswordReset(token, data.password);
        setModeState("login");
        setSuccess("Password updated. Sign in with your new password.");
        navigate("/auth", { replace: true });
      }
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Unable to complete this request.");
    } finally {
      setLoading(false);
    }
  }

  const copy = content[mode];
  return <main className="auth" id="main-content">
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
      <form className="auth-form" key={mode} onSubmit={submit}>
        <div className="auth-form__mobile-brand brand"><i className="brand-mark"><img src="/sceptre-icon.png" alt="" /></i><span>Sceptre <b>AI</b></span></div>
        {mode !== "login" && <button type="button" className="auth-form__back" onClick={() => setMode("login")}><ArrowLeft size={15} /> Back to sign in</button>}
        <span className="eyebrow">{copy.eyebrow}</span>
        <h2>{copy.title}</h2>
        <p>{copy.description}</p>
        {success && <Notice tone="success">{success}</Notice>}
        {error && <Notice tone="danger">{error}</Notice>}
        {mode === "register" && <label>Full name<input name="full_name" autoComplete="name" placeholder="Ada Lovelace" /></label>}
        {(mode === "login" || mode === "register" || mode === "forgot") &&
          <label>Work email<input name="email" type="email" autoComplete="email" required placeholder="you@company.com" /></label>}
        {(mode === "login" || mode === "register" || mode === "reset") &&
          <label>{mode === "reset" ? "New password" : "Password"}<input name="password" type="password" minLength={mode === "login" ? 1 : 8} autoComplete={mode === "login" ? "current-password" : "new-password"} required placeholder="••••••••" /></label>}
        {(mode === "register" || mode === "reset") &&
          <label>Confirm password<input name="confirm_password" type="password" minLength={8} autoComplete="new-password" required placeholder="••••••••" /></label>}
        {mode === "login" && <button className="auth-form__forgot" type="button" onClick={() => setMode("forgot")}>Forgot password?</button>}
        <Button type="submit" loading={loading}>
          {mode === "login" ? "Sign in" : mode === "register" ? "Create account" : mode === "forgot" ? "Send reset instructions" : "Update password"}
          {mode === "forgot" || mode === "reset" ? <KeyRound size={16} /> : <ArrowRight size={16} />}
        </Button>
        {mode === "forgot" && devResetToken && <Button type="button" variant="secondary" onClick={() => setMode("reset", devResetToken)}>Continue to reset password</Button>}
        {(mode === "login" || mode === "register") && <p className="auth-form__switch">{mode === "login" ? "New to Sceptre?" : "Already have an account?"}
          <button type="button" onClick={() => setMode(mode === "login" ? "register" : "login")}>
            {mode === "login" ? "Create an account" : "Sign in"}
          </button>
        </p>}
      </form>
    </section>
  </main>;
}
