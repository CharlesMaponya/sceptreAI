import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { axe } from "jest-axe";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { MemoryRouter } from "react-router";
import { Auth } from "./Auth";
import { getSession, setSession } from "./api";

describe("authentication experience", () => {
  const renderAuth = (entry = "/auth") => render(<MemoryRouter initialEntries={[entry]}><Auth /></MemoryRouter>);
  beforeEach(() => {
    setSession(null);
    vi.restoreAllMocks();
  });

  it("is accessible and exposes the sign-in fields", async () => {
    const { container } = renderAuth();
    expect(screen.getByRole("heading", { name: "Sign in to Sceptre" })).toBeInTheDocument();
    expect(screen.getByLabelText("Work email")).toHaveAttribute("type", "email");
    expect(screen.getByLabelText("Password")).toHaveAttribute("type", "password");
    expect(await axe(container)).toHaveNoViolations();
  });

  it("returns to sign in without creating a session after registration", async () => {
    const user = userEvent.setup();
    const fetchMock = vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response(JSON.stringify({
      user: {
        id: "u1", email: "ada@example.com", full_name: "Ada Lovelace", global_role: "member",
        auth_provider: "simple", is_active: true, is_verified: true, created_at: "2026-01-01T00:00:00Z",
      },
      message: "Account created successfully. Sign in to continue.",
    }), { status: 201, headers: { "Content-Type": "application/json" } }));
    renderAuth();
    await user.click(screen.getByRole("button", { name: "Create an account" }));
    await user.type(screen.getByLabelText("Full name"), "Ada Lovelace");
    await user.type(screen.getByLabelText("Work email"), "ada@example.com");
    await user.type(screen.getByLabelText("Password"), "correct-horse");
    await user.type(screen.getByLabelText("Confirm password"), "correct-horse");
    await user.click(screen.getByRole("button", { name: "Create account" }));

    expect(fetchMock).toHaveBeenCalledOnce();
    expect(fetchMock.mock.calls[0][0]).toBe("/api/v1/auth/register");
    expect(JSON.parse(String(fetchMock.mock.calls[0][1]?.body))).toEqual({
      full_name: "Ada Lovelace", email: "ada@example.com", password: "correct-horse",
    });
    expect(await screen.findByRole("heading", { name: "Sign in to Sceptre" })).toBeInTheDocument();
    expect(screen.getByText("Account created successfully. Sign in to continue.")).toBeInTheDocument();
    expect(getSession()).toBeNull();
  });

  it("supports the complete development password-reset flow", async () => {
    const user = userEvent.setup();
    const fetchMock = vi.spyOn(globalThis, "fetch")
      .mockResolvedValueOnce(new Response(JSON.stringify({
        message: "If the account exists, password reset instructions have been prepared.",
        reset_token_for_dev: "development-reset-token-long-enough",
      }), { status: 200, headers: { "Content-Type": "application/json" } }))
      .mockResolvedValueOnce(new Response(null, { status: 204 }));
    renderAuth();

    await user.click(screen.getByRole("button", { name: "Forgot password?" }));
    await user.type(screen.getByLabelText("Work email"), "ada@example.com");
    await user.click(screen.getByRole("button", { name: /Send reset instructions/i }));
    expect(await screen.findByText(/password reset instructions have been prepared/i)).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "Continue to reset password" }));
    await user.type(screen.getByLabelText("New password"), "new-correct-horse");
    await user.type(screen.getByLabelText("Confirm password"), "new-correct-horse");
    await user.click(screen.getByRole("button", { name: /Update password/i }));

    expect(await screen.findByText("Password updated. Sign in with your new password.")).toBeInTheDocument();
    expect(fetchMock.mock.calls[0][0]).toBe("/api/v1/auth/password-reset/request");
    expect(fetchMock.mock.calls[1][0]).toBe("/api/v1/auth/password-reset/confirm");
    expect(JSON.parse(String(fetchMock.mock.calls[1][1]?.body))).toEqual({
      token: "development-reset-token-long-enough", new_password: "new-correct-horse",
    });
  });

  it("rejects mismatched registration passwords without sending a request", async () => {
    const user = userEvent.setup();
    const fetchMock = vi.spyOn(globalThis, "fetch");
    renderAuth("/auth?mode=register");

    await user.type(screen.getByLabelText("Full name"), "Ada Lovelace");
    await user.type(screen.getByLabelText("Work email"), "ada@example.com");
    await user.type(screen.getByLabelText("Password"), "correct-horse");
    await user.type(screen.getByLabelText("Confirm password"), "different-horse");
    await user.click(screen.getByRole("button", { name: "Create account" }));

    expect(await screen.findByText("The passwords do not match.")).toBeInTheDocument();
    expect(fetchMock).not.toHaveBeenCalled();
    await user.click(screen.getByRole("button", { name: "Back to sign in" }));
    expect(screen.getByRole("heading", { name: "Sign in to Sceptre" })).toBeInTheDocument();
  });

  it("reports login failures and preserves the signed-out state", async () => {
    const user = userEvent.setup();
    vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response(JSON.stringify({
      detail: "Invalid email or password",
    }), { status: 401, headers: { "Content-Type": "application/json" } }));
    renderAuth();

    await user.type(screen.getByLabelText("Work email"), "ada@example.com");
    await user.type(screen.getByLabelText("Password"), "incorrect");
    await user.click(screen.getByRole("button", { name: "Sign in" }));

    expect(await screen.findByText("Invalid email or password")).toBeInTheDocument();
    expect(getSession()).toBeNull();
  });

  it("handles a reset response without a development token", async () => {
    const user = userEvent.setup();
    vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response(JSON.stringify({
      message: "If the account exists, instructions were sent.",
    }), { status: 200, headers: { "Content-Type": "application/json" } }));
    renderAuth("/auth?mode=forgot");

    await user.type(screen.getByLabelText("Work email"), "ada@example.com");
    await user.click(screen.getByRole("button", { name: /Send reset instructions/i }));

    expect(await screen.findByText("If the account exists, instructions were sent.")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Continue to reset password" })).not.toBeInTheDocument();
  });

  it("rejects incomplete and mismatched password-reset links", async () => {
    const user = userEvent.setup();
    const fetchMock = vi.spyOn(globalThis, "fetch");
    renderAuth("/auth?mode=reset");

    await user.type(screen.getByLabelText("New password"), "new-correct-horse");
    await user.type(screen.getByLabelText("Confirm password"), "different-horse");
    await user.click(screen.getByRole("button", { name: /Update password/i }));
    expect(await screen.findByText("The passwords do not match.")).toBeInTheDocument();
    expect(fetchMock).not.toHaveBeenCalled();

    await user.clear(screen.getByLabelText("Confirm password"));
    await user.type(screen.getByLabelText("Confirm password"), "new-correct-horse");
    await user.click(screen.getByRole("button", { name: /Update password/i }));
    expect(await screen.findByText("This reset link is incomplete. Request a new one.")).toBeInTheDocument();
    expect(fetchMock).not.toHaveBeenCalled();
  });
});
