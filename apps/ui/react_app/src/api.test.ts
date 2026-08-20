import { beforeEach, describe, expect, it, vi } from "vitest";
import { api, apiBlob, authenticate, getSession, setSession, signOut, uploadFormData } from "./api";

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

  it("adds one stable idempotency key to a mutating request and its retry", async () => {
    setSession({ user, tokens: {
      access_token: "expired", refresh_token: "refresh-1", token_type: "bearer", expires_in: 60,
    } });
    const fetchMock = vi.spyOn(globalThis, "fetch")
      .mockResolvedValueOnce(new Response(JSON.stringify({ detail: "Expired" }), { status: 401 }))
      .mockResolvedValueOnce(new Response(JSON.stringify({
        access_token: "fresh", refresh_token: "refresh-2", token_type: "bearer", expires_in: 3600,
      }), { status: 200 }))
      .mockResolvedValueOnce(new Response(JSON.stringify({ id: "created" }), { status: 201 }));

    await api("/resources", { method: "POST", body: "{}" });
    const first = new Headers(fetchMock.mock.calls[0][1]?.headers).get("Idempotency-Key");
    const retried = new Headers(fetchMock.mock.calls[2][1]?.headers).get("Idempotency-Key");
    expect(first).toBeTruthy();
    expect(retried).toBe(first);
  });

  it("preserves a caller-provided key and leaves reads without one", async () => {
    const fetchMock = vi.spyOn(globalThis, "fetch")
      .mockImplementation(async () => new Response("{}", { status: 200 }));
    await api("/resource", { method: "POST", headers: { "Idempotency-Key": "stable-key" } });
    await api("/resource");
    expect(new Headers(fetchMock.mock.calls[0][1]?.headers).get("Idempotency-Key"))
      .toBe("stable-key");
    expect(new Headers(fetchMock.mock.calls[1][1]?.headers).has("Idempotency-Key")).toBe(false);
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

describe("API response parsing", () => {
  beforeEach(() => {
    setSession(null);
    vi.restoreAllMocks();
  });

  it("preserves a plain-text error response", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(new Response("upstream unavailable", {
      status: 502, headers: { "Content-Type": "text/plain" },
    }));

    await expect(api("/training/progress")).rejects.toThrow("upstream unavailable");
  });

  it("uses the status fallback for an empty error response", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(new Response(null, { status: 503 }));

    await expect(api("/training/progress")).rejects.toThrow("Request failed (503)");
  });

  it("extracts detail from a JSON error response", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(new Response(JSON.stringify({
      detail: "Training pod failed.",
    }), { status: 500, headers: { "Content-Type": "application/json" } }));

    await expect(api("/training/progress")).rejects.toThrow("Training pod failed.");
  });

  it("combines structured validation errors", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(new Response(JSON.stringify({
      detail: [{ msg: "Target is required" }, { msg: "Choose at least one model" }],
    }), { status: 422, headers: { "Content-Type": "application/json" } }));

    await expect(api("/training/estimate")).rejects.toThrow(
      "Target is required, Choose at least one model",
    );
  });

  it("accepts empty and plain-text success responses", async () => {
    vi.spyOn(globalThis, "fetch")
      .mockResolvedValueOnce(new Response(null, { status: 204 }))
      .mockResolvedValueOnce(new Response("ready", { status: 200 }));

    await expect(api("/health/empty")).resolves.toBeNull();
    await expect(api("/health/text")).resolves.toBe("ready");
  });
});

describe("API authentication and multipart handling", () => {
  beforeEach(() => {
    setSession(null);
    vi.restoreAllMocks();
  });

  it("stores an authenticated session and tolerates logout transport failure", async () => {
    const authenticated = {
      user,
      tokens: { access_token: "access", refresh_token: "refresh", token_type: "bearer", expires_in: 3600 },
    };
    const fetchMock = vi.spyOn(globalThis, "fetch")
      .mockResolvedValueOnce(new Response(JSON.stringify(authenticated), {
        status: 200, headers: { "Content-Type": "application/json" },
      }))
      .mockRejectedValueOnce(new Error("network unavailable"));

    await expect(authenticate("login", { email: user.email, password: "secret" }))
      .resolves.toEqual(authenticated);
    expect(getSession()).toEqual(authenticated);
    await expect(signOut()).resolves.toBeUndefined();
    expect(getSession()).toBeNull();
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it("reports multipart progress and parses a successful upload", async () => {
    vi.spyOn(XMLHttpRequest.prototype, "open").mockImplementation(() => undefined);
    vi.spyOn(XMLHttpRequest.prototype, "setRequestHeader").mockImplementation(() => undefined);
    vi.spyOn(XMLHttpRequest.prototype, "send").mockImplementation(function (this: XMLHttpRequest) {
      this.upload.dispatchEvent(new ProgressEvent("progress", {
        lengthComputable: true, loaded: 5, total: 10,
      }));
      Object.defineProperty(this, "status", { configurable: true, value: 201 });
      Object.defineProperty(this, "responseText", {
        configurable: true, value: JSON.stringify({ id: "version-1" }),
      });
      this.dispatchEvent(new ProgressEvent("load"));
    });
    const progress: number[] = [];

    await expect(uploadFormData<{ id: string }>("/datasets/upload", new FormData(),
      (value) => progress.push(value))).resolves.toEqual({ id: "version-1" });
    expect(progress).toEqual([0, 50, 100]);
  });

  it("surfaces multipart validation, network, and cancellation failures", async () => {
    vi.spyOn(XMLHttpRequest.prototype, "open").mockImplementation(() => undefined);
    vi.spyOn(XMLHttpRequest.prototype, "setRequestHeader").mockImplementation(() => undefined);
    const send = vi.spyOn(XMLHttpRequest.prototype, "send");
    send.mockImplementationOnce(function (this: XMLHttpRequest) {
      Object.defineProperty(this, "status", { configurable: true, value: 422 });
      Object.defineProperty(this, "responseText", {
        configurable: true, value: JSON.stringify({ detail: [{ msg: "Unsupported format" }] }),
      });
      this.dispatchEvent(new ProgressEvent("load"));
    });
    await expect(uploadFormData("/datasets/upload", new FormData(), () => undefined))
      .rejects.toThrow("Unsupported format");

    send.mockImplementationOnce(function (this: XMLHttpRequest) {
      this.dispatchEvent(new ProgressEvent("error"));
    });
    await expect(uploadFormData("/datasets/upload", new FormData(), () => undefined))
      .rejects.toThrow("could not reach the API");

    send.mockImplementationOnce(function (this: XMLHttpRequest) {
      this.dispatchEvent(new ProgressEvent("abort"));
    });
    await expect(uploadFormData("/datasets/upload", new FormData(), () => undefined))
      .rejects.toThrow("upload was cancelled");
  });

  it("refreshes an expired multipart session and retries with the new access token", async () => {
    setSession({ user, tokens: {
      access_token: "expired", refresh_token: "refresh-1", token_type: "bearer", expires_in: 60,
    } });
    vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(new Response(JSON.stringify({
      access_token: "fresh", refresh_token: "refresh-2", token_type: "bearer", expires_in: 3600,
    }), { status: 200, headers: { "Content-Type": "application/json" } }));
    vi.spyOn(XMLHttpRequest.prototype, "open").mockImplementation(() => undefined);
    const headers = vi.spyOn(XMLHttpRequest.prototype, "setRequestHeader")
      .mockImplementation(() => undefined);
    let attempts = 0;
    vi.spyOn(XMLHttpRequest.prototype, "send").mockImplementation(function (this: XMLHttpRequest) {
      attempts += 1;
      Object.defineProperty(this, "status", { configurable: true, value: attempts === 1 ? 401 : 201 });
      Object.defineProperty(this, "responseText", {
        configurable: true,
        value: attempts === 1 ? JSON.stringify({ detail: "Expired" }) : JSON.stringify({ id: "version-2" }),
      });
      this.dispatchEvent(new ProgressEvent("load"));
    });

    await expect(uploadFormData<{ id: string }>("/datasets/upload", new FormData(), () => undefined))
      .resolves.toEqual({ id: "version-2" });
    expect(attempts).toBe(2);
    expect(headers.mock.calls).toContainEqual(["Authorization", "Bearer expired"]);
    expect(headers.mock.calls).toContainEqual(["Authorization", "Bearer fresh"]);
    const idempotencyKeys = headers.mock.calls
      .filter(([name]) => name === "Idempotency-Key")
      .map(([, value]) => value);
    expect(idempotencyKeys).toHaveLength(2);
    expect(idempotencyKeys[0]).toBe(idempotencyKeys[1]);
    expect(getSession()?.tokens.refresh_token).toBe("refresh-2");
  });
});

describe("authenticated binary downloads", () => {
  beforeEach(() => {
    setSession(null);
    vi.restoreAllMocks();
  });

  it("returns the prediction blob and provider filename", async () => {
    setSession({ user, tokens: {
      access_token: "access", refresh_token: "refresh", token_type: "bearer", expires_in: 3600,
    } });
    const fetchMock = vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(new Response("prediction\n1\n", {
      status: 200, headers: { "Content-Disposition": 'attachment; filename="scored.csv"' },
    }));
    const result = await apiBlob("/api/v1/offline", { method: "POST", body: "{}" });
    expect(result.filename).toBe("scored.csv");
    expect(result.blob.size).toBe(13);
    expect(new Headers(fetchMock.mock.calls[0][1]?.headers).get("Authorization"))
      .toBe("Bearer access");
  });

  it("uses a safe filename fallback and surfaces non-authentication errors", async () => {
    const fetchMock = vi.spyOn(globalThis, "fetch")
      .mockResolvedValueOnce(new Response("value", { status: 200 }))
      .mockResolvedValueOnce(new Response(JSON.stringify({ detail: "invalid scoring input" }), {
        status: 422,
      }));
    await expect(apiBlob("/api/v1/offline")).resolves.toMatchObject({ filename: "predictions.csv" });
    await expect(apiBlob("/api/v1/offline")).rejects.toThrow("invalid scoring input");
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it("refreshes once for an expired binary request", async () => {
    setSession({ user, tokens: {
      access_token: "expired", refresh_token: "refresh-1", token_type: "bearer", expires_in: 60,
    } });
    const fetchMock = vi.spyOn(globalThis, "fetch")
      .mockResolvedValueOnce(new Response(JSON.stringify({ detail: "Expired" }), { status: 401 }))
      .mockResolvedValueOnce(new Response(JSON.stringify({
        access_token: "fresh", refresh_token: "refresh-2", token_type: "bearer", expires_in: 3600,
      }), { status: 200 }))
      .mockResolvedValueOnce(new Response("prediction", { status: 200 }));
    await expect(apiBlob("/api/v1/offline", { method: "POST" }))
      .resolves.toMatchObject({ filename: "predictions.csv" });
    expect(new Headers(fetchMock.mock.calls[2][1]?.headers).get("Authorization"))
      .toBe("Bearer fresh");
  });

  it("clears the session when binary-request refresh fails", async () => {
    setSession({ user, tokens: {
      access_token: "expired", refresh_token: "bad", token_type: "bearer", expires_in: 60,
    } });
    vi.spyOn(globalThis, "fetch")
      .mockResolvedValueOnce(new Response("expired", { status: 401 }))
      .mockResolvedValueOnce(new Response("invalid", { status: 401 }));
    await expect(apiBlob("/api/v1/offline")).rejects.toThrow("session has expired");
    expect(getSession()).toBeNull();
  });
});
