import { render, screen } from "@testing-library/react";
import { axe } from "jest-axe";
import { MemoryRouter } from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { setSession } from "./api";
import { Landing } from "./Landing";

describe("public product landing page", () => {
  beforeEach(() => setSession(null));
  const renderLanding = () => {
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    return render(<QueryClientProvider client={client}><MemoryRouter>
      <Landing />
    </MemoryRouter></QueryClientProvider>);
  };

  it("communicates the business proposition and offers conversion paths", async () => {
    const { container } = renderLanding();
    expect(screen.getByRole("heading", {
      name: /Build models your business can trust/i,
    })).toBeInTheDocument();
    expect(screen.getByRole("heading", {
      name: /Production discipline without the platform tax/i,
    })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: /Build your first model/i }))
      .toHaveAttribute("href", "/auth?mode=register");
    expect(screen.getAllByRole("link", { name: "Sign in" })[0]).toHaveAttribute("href", "/auth");
    expect(await axe(container)).toHaveNoViolations();
  });

  it("verifies a stored session and shows the signed-in user", async () => {
    const verifiedUser = {
      id: "u1", email: "owner@example.com", full_name: "Owner Example", global_role: "member",
      is_active: true, is_verified: true, created_at: "2026-01-01T00:00:00Z",
    };
    setSession({
      user: verifiedUser,
      tokens: {
        access_token: "access", refresh_token: "refresh", token_type: "bearer", expires_in: 3600,
      },
    });
    vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response(JSON.stringify(verifiedUser), {
      status: 200, headers: { "Content-Type": "application/json" },
    }));
    renderLanding();
    expect(await screen.findByRole("link", { name: /Open Owner Example's workspace/i }))
      .toHaveAttribute("href", "/projects");
    expect(screen.getByText("OE")).toBeInTheDocument();
    expect(screen.getAllByRole("link", { name: /Open workspace/i })[0]).toHaveAttribute("href", "/projects");
  });
});
