import { beforeEach, describe, expect, it, vi } from "vitest";
import { api, getSession, setSession } from "./api";

const user = {
  id: "user-1", email: "ada@example.com", full_name: "Ada", global_role: "member",
  is_active: true, is_verified: true, created_at: "2026-01-01T00:00:00Z",
};

describe("API session handling", () => {
  beforeEach(() => {
    setSession(null);
    vi.restoreAllMocks();
  });

  it("rotates refresh tokens and retries the original request once", async () => {
    setSession({ user, tokens: {
      access_token: "expired", refresh_token: "refresh-1", token_type: "bearer", expires_in: 60,
    } });
    const fetchMock = vi.spyOn(globalThis, "fetch")
      .mockResolvedValueOnce(new Response(JSON.stringify({ detail: "Expired" }), {
        status: 401, headers: { "Content-Type": "application/json" },
      }))
      .mockResolvedValueOnce(new Response(JSON.stringify({
        access_token: "fresh", refresh_token: "refresh-2", token_type: "bearer", expires_in: 3600,
      }), { status: 200, headers: { "Content-Type": "application/json" } }))
      .mockResolvedValueOnce(new Response(JSON.stringify([{ id: "project-1" }]), {
        status: 200, headers: { "Content-Type": "application/json" },
      }));

    await expect(api<Array<{ id: string }>>("/projects")).resolves.toEqual([{ id: "project-1" }]);
    expect(fetchMock).toHaveBeenCalledTimes(3);
    expect(getSession()?.tokens.refresh_token).toBe("refresh-2");
    const retriedHeaders = new Headers(fetchMock.mock.calls[2][1]?.headers);
    expect(retriedHeaders.get("Authorization")).toBe("Bearer fresh");
  });

  it("clears the session when refresh fails", async () => {
    setSession({ user, tokens: {
      access_token: "expired", refresh_token: "bad", token_type: "bearer", expires_in: 60,
    } });
    vi.spyOn(globalThis, "fetch")
      .mockResolvedValueOnce(new Response(JSON.stringify({ detail: "Expired" }), { status: 401 }))
      .mockResolvedValueOnce(new Response(JSON.stringify({ detail: "Invalid refresh" }), { status: 401 }));

    await expect(api("/projects")).rejects.toThrow("session has expired");
    expect(getSession()).toBeNull();
  });
});
