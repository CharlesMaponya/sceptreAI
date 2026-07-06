import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { axe } from "jest-axe";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { MemoryRouter } from "react-router-dom";
import { Auth } from "./Auth";
import { setSession } from "./api";

describe("authentication experience", () => {
  const renderAuth = () => render(<MemoryRouter><Auth /></MemoryRouter>);
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

  it("switches to registration and submits the API contract", async () => {
    const user = userEvent.setup();
    const fetchMock = vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response(JSON.stringify({
      user: {
        id: "u1", email: "ada@example.com", full_name: "Ada Lovelace", global_role: "member",
        is_active: true, is_verified: true, created_at: "2026-01-01T00:00:00Z",
      },
      tokens: { access_token: "access", refresh_token: "refresh", token_type: "bearer", expires_in: 3600 },
    }), { status: 201, headers: { "Content-Type": "application/json" } }));
    renderAuth();
    await user.click(screen.getByRole("button", { name: "Create an account" }));
    await user.type(screen.getByLabelText("Full name"), "Ada Lovelace");
    await user.type(screen.getByLabelText("Work email"), "ada@example.com");
    await user.type(screen.getByLabelText("Password"), "correct-horse");
    await user.click(screen.getByRole("button", { name: "Create account" }));

    expect(fetchMock).toHaveBeenCalledOnce();
    expect(fetchMock.mock.calls[0][0]).toBe("/api/v1/auth/register");
    expect(JSON.parse(String(fetchMock.mock.calls[0][1]?.body))).toEqual({
      full_name: "Ada Lovelace", email: "ada@example.com", password: "correct-horse",
    });
  });
});
