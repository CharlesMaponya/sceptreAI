import { FormEvent, useState } from "react";
import { CheckCircle2, KeyRound, LockKeyhole, Mail, ShieldCheck, UserRound } from "lucide-react";
import { changePassword, getSession, updateAccount } from "../api";
import { Badge, Button, Notice, PageHeader } from "../components/ui";
import { formatDate, initials, titleCase } from "../lib";

export function AccountPage() {
  const user = getSession()!.user;
  const [profileLoading, setProfileLoading] = useState(false);
  const [passwordLoading, setPasswordLoading] = useState(false);
  const [profileMessage, setProfileMessage] = useState("");
  const [passwordMessage, setPasswordMessage] = useState("");
  const [profileError, setProfileError] = useState("");
  const [passwordError, setPasswordError] = useState("");

  async function saveProfile(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setProfileError("");
    setProfileMessage("");
    setProfileLoading(true);
    const values = Object.fromEntries(new FormData(event.currentTarget)) as Record<string, string>;
    try {
      await updateAccount({ full_name: values.full_name, email: values.email });
      setProfileMessage("Your account details are up to date. Other signed-in sessions were closed.");
    } catch (cause) {
      setProfileError(cause instanceof Error ? cause.message : "Could not update your account.");
    } finally {
      setProfileLoading(false);
    }
  }

  async function savePassword(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setPasswordError("");
    setPasswordMessage("");
    const form = event.currentTarget;
    const values = Object.fromEntries(new FormData(form)) as Record<string, string>;
    if (values.new_password !== values.confirm_password) {
      setPasswordError("The new passwords do not match.");
      return;
    }
    setPasswordLoading(true);
    try {
      await changePassword({ current_password: values.current_password, new_password: values.new_password });
      form.reset();
      setPasswordMessage("Password changed. Other signed-in sessions were closed.");
    } catch (cause) {
      setPasswordError(cause instanceof Error ? cause.message : "Could not change your password.");
    } finally {
      setPasswordLoading(false);
    }
  }

  return <>
    <PageHeader eyebrow="Your account" title="Profile & security"
      description="Keep your identity current and control the credentials protecting this workspace." />

    <section className="account-layout" aria-label="Account settings">
      <div className="account-identity">
        <div className="account-identity__avatar" aria-hidden>{initials(user.full_name, user.email)}</div>
        <div>
          <span className="eyebrow">Signed-in identity</span>
          <h2>{user.full_name || "Sceptre user"}</h2>
          <p>{user.email}</p>
        </div>
        <dl className="account-facts">
          <div><dt>Workspace role</dt><dd>{titleCase(user.global_role)}</dd></div>
          <div><dt>Account status</dt><dd><Badge status={user.is_active ? "active" : "disabled"}>{user.is_active ? "Active" : "Disabled"}</Badge></dd></div>
          <div><dt>Email status</dt><dd>{user.is_verified ? <><CheckCircle2 size={14} /> Verified</> : "Not verified"}</dd></div>
          <div><dt>Member since</dt><dd>{formatDate(user.created_at)}</dd></div>
        </dl>
        <div className="account-identity__note"><ShieldCheck size={18} /><p><b>Local account</b><span>Your password and sessions are managed by this Sceptre installation.</span></p></div>
      </div>

      <div className="account-settings">
        <section className="account-section" aria-labelledby="profile-heading">
          <header><span><UserRound /></span><div><h2 id="profile-heading">Personal details</h2><p>Used in your workspace, reports, and audit history.</p></div></header>
          <form onSubmit={saveProfile}>
            {profileMessage && <Notice tone="success">{profileMessage}</Notice>}
            {profileError && <Notice tone="danger">{profileError}</Notice>}
            <div className="account-fields">
              <label>Full name<input name="full_name" autoComplete="name" defaultValue={user.full_name || ""} placeholder="Your name" /></label>
              <label>Email address<span className="field-with-icon"><Mail size={15} /><input name="email" type="email" autoComplete="email" defaultValue={user.email} required /></span></label>
            </div>
            <div className="account-form-actions"><small>Saving changes renews this session and closes other sessions.</small><Button type="submit" loading={profileLoading}>Save profile</Button></div>
          </form>
        </section>

        <section className="account-section" aria-labelledby="password-heading">
          <header><span><LockKeyhole /></span><div><h2 id="password-heading">Password</h2><p>Change your password after confirming the current one.</p></div></header>
          <form onSubmit={savePassword}>
            {passwordMessage && <Notice tone="success">{passwordMessage}</Notice>}
            {passwordError && <Notice tone="danger">{passwordError}</Notice>}
            <div className="account-fields account-fields--password">
              <label>Current password<input name="current_password" type="password" autoComplete="current-password" required /></label>
              <label>New password<input name="new_password" type="password" autoComplete="new-password" minLength={8} required /></label>
              <label>Confirm new password<input name="confirm_password" type="password" autoComplete="new-password" minLength={8} required /></label>
            </div>
            <div className="account-form-actions"><small>Use at least eight characters and do not reuse your current password.</small><Button type="submit" loading={passwordLoading}><KeyRound size={16} /> Change password</Button></div>
          </form>
        </section>
      </div>
    </section>
  </>;
}
