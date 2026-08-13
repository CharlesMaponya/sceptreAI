import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { axe } from "jest-axe";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { getSession, setSession } from "../api";
import { AccountPage } from "./AccountPage";

const session = {
  user: {
    id: "user-1", email: "ada@example.com", full_name: "Ada Lovelace", auth_provider: "simple",
    global_role: "member", is_active: true, is_verified: true, created_at: "2026-01-01T00:00:00Z",
  },
  tokens: { access_token: "access", refresh_token: "refresh-token-long-enough", token_type: "bearer", expires_in: 3600 },
};

describe("profile and security", () => {
  beforeEach(() => {
    setSession(session);
    vi.restoreAllMocks();
  });

  it("renders an accessible local-account control surface", async () => {
    const { container } = render(<AccountPage />);
    expect(screen.getByRole("heading", { name: "Profile & security" })).toBeInTheDocument();
    expect(screen.getByText("Local account")).toBeInTheDocument();
    expect(screen.getByLabelText("Current password")).toHaveAttribute("type", "password");
    expect(await axe(container)).toHaveNoViolations();
  });

  it("updates account details and keeps the renewed session", async () => {
    const user = userEvent.setup();
    const fetchMock = vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response(JSON.stringify({
      user: { ...session.user, full_name: "Ada Byron", email: "ada.byron@example.com" },
      tokens: { ...session.tokens, access_token: "renewed-access", refresh_token: "renewed-refresh-token" },
    }), { status: 200, headers: { "Content-Type": "application/json" } }));
    render(<AccountPage />);

    const name = screen.getByLabelText("Full name");
    const email = screen.getByLabelText("Email address");
    await user.clear(name);
    await user.type(name, "Ada Byron");
    await user.clear(email);
    await user.type(email, "ada.byron@example.com");
    await user.click(screen.getByRole("button", { name: "Save profile" }));

    expect(await screen.findByText(/account details are up to date/i)).toBeInTheDocument();
    expect(fetchMock.mock.calls[0][0]).toBe("/api/v1/auth/me");
    expect(fetchMock.mock.calls[0][1]?.method).toBe("PATCH");
    expect(getSession()?.user.full_name).toBe("Ada Byron");
    expect(getSession()?.tokens.access_token).toBe("renewed-access");
  });

  it("validates matching passwords before calling the API", async () => {
    const user = userEvent.setup();
    const fetchMock = vi.spyOn(globalThis, "fetch");
    render(<AccountPage />);

    await user.type(screen.getByLabelText("Current password"), "current-pass");
    await user.type(screen.getByLabelText("New password"), "new-password");
    await user.type(screen.getByLabelText("Confirm new password"), "different-password");
    await user.click(screen.getByRole("button", { name: "Change password" }));

    expect(await screen.findByText("The new passwords do not match.")).toBeInTheDocument();
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("changes the password, renews the session, and clears the form", async () => {
    const user = userEvent.setup();
    const fetchMock = vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response(JSON.stringify({
      user: session.user,
      tokens: { ...session.tokens, access_token: "password-renewed" },
    }), { status: 200, headers: { "Content-Type": "application/json" } }));
    render(<AccountPage />);

    const current = screen.getByLabelText("Current password");
    await user.type(current, "current-pass");
    await user.type(screen.getByLabelText("New password"), "new-password");
    await user.type(screen.getByLabelText("Confirm new password"), "new-password");
    await user.click(screen.getByRole("button", { name: "Change password" }));

    expect(await screen.findByText(/Password changed/)).toBeInTheDocument();
    expect(fetchMock.mock.calls[0][0]).toBe("/api/v1/auth/password/change");
    expect(fetchMock.mock.calls[0][1]?.method).toBe("POST");
    expect(current).toHaveValue("");
    expect(getSession()?.tokens.access_token).toBe("password-renewed");
  });

  it("reports profile and password API errors", async () => {
    const user = userEvent.setup();
    vi.spyOn(globalThis, "fetch")
      .mockResolvedValueOnce(new Response(JSON.stringify({ detail: "Email is already registered" }), {
        status: 409, headers: { "Content-Type": "application/json" },
      }))
      .mockResolvedValueOnce(new Response(JSON.stringify({ detail: "Current password is incorrect" }), {
        status: 400, headers: { "Content-Type": "application/json" },
      }));
    render(<AccountPage />);

    await user.click(screen.getByRole("button", { name: "Save profile" }));
    expect(await screen.findByText("Email is already registered")).toBeInTheDocument();

    await user.type(screen.getByLabelText("Current password"), "wrong-pass");
    await user.type(screen.getByLabelText("New password"), "new-password");
    await user.type(screen.getByLabelText("Confirm new password"), "new-password");
    await user.click(screen.getByRole("button", { name: "Change password" }));
    expect(await screen.findByText("Current password is incorrect")).toBeInTheDocument();
  });

  it("renders inactive, unverified, and unnamed account states", () => {
    setSession({
      ...session,
      user: { ...session.user, full_name: "", is_active: false, is_verified: false },
    });
    render(<AccountPage />);

    expect(screen.getByText("Disabled")).toBeInTheDocument();
    expect(screen.getByText("Not verified")).toBeInTheDocument();
    expect(screen.getByLabelText("Full name")).toHaveValue("");
  });
});
