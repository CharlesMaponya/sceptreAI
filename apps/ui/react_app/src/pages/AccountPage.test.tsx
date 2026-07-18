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
});
