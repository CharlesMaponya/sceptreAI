import type { AuthResponse, Tokens } from "./types";

const API_ROOT = (import.meta.env.VITE_API_URL as string | undefined) || "/api/v1";
const STORAGE_KEY = "sceptre.session";

class ApiError extends Error {
  constructor(message: string, readonly status: number) { super(message); }
}

export type Session = { user: AuthResponse["user"]; tokens: Tokens };
let session: Session | null = (() => {
  try { return JSON.parse(localStorage.getItem(STORAGE_KEY) || "null") as Session | null; }
  catch { return null; }
})();

export const getSession = () => session;
export const setSession = (next: Session | null) => {
  session = next;
  if (next) localStorage.setItem(STORAGE_KEY, JSON.stringify(next));
  else localStorage.removeItem(STORAGE_KEY);
  window.dispatchEvent(new Event("sceptre-session"));
};

const messageFrom = (data: unknown, status: number) => {
  if (typeof data === "string") return data;
  if (data && typeof data === "object" && "detail" in data) {
    const detail = (data as { detail: unknown }).detail;
    if (typeof detail === "string") return detail;
    if (Array.isArray(detail)) return detail.map((item) =>
      typeof item === "object" && item && "msg" in item ? String(item.msg) : String(item)
    ).join(", ");
  }
  return `Request failed (${status})`;
};

async function responseData(response: Response): Promise<unknown> {
  if (response.status === 204) return null;
  const body = await response.text();
  if (!body.trim()) return null;
  try { return JSON.parse(body) as unknown; }
  catch { return body; }
}

async function raw<T>(path: string, options: RequestInit = {}, token?: string): Promise<T> {
  const headers = new Headers(options.headers);
  if (!(options.body instanceof FormData)) headers.set("Content-Type", "application/json");
  if (token) headers.set("Authorization", `Bearer ${token}`);
  const response = await fetch(`${API_ROOT}${path}`, { ...options, headers });
  const data = await responseData(response);
  if (!response.ok) throw new ApiError(messageFrom(data, response.status), response.status);
  return data as T;
}

function multipartRequest<T>(
  path: string,
  body: FormData,
  token: string | undefined,
  onProgress: (percent: number) => void,
): Promise<T> {
  return new Promise((resolve, reject) => {
    const request = new XMLHttpRequest();
    request.open("POST", `${API_ROOT}${path}`);
    if (token) request.setRequestHeader("Authorization", `Bearer ${token}`);
    request.upload.onprogress = (event) => {
      if (event.lengthComputable && event.total > 0) {
        onProgress(Math.min(100, Math.round((event.loaded / event.total) * 100)));
      }
    };
    request.onload = () => {
      let data: unknown = null;
      try { data = request.responseText ? JSON.parse(request.responseText) : null; }
      catch { data = request.responseText; }
      if (request.status >= 200 && request.status < 300) {
        onProgress(100);
        resolve(data as T);
      } else {
        reject(new ApiError(messageFrom(data, request.status), request.status));
      }
    };
    request.onerror = () => reject(new Error("The upload could not reach the API."));
    request.onabort = () => reject(new Error("The upload was cancelled."));
    onProgress(0);
    request.send(body);
  });
}

export async function api<T>(path: string, options: RequestInit = {}): Promise<T> {
  try {
    return await raw<T>(path, options, session?.tokens.access_token);
  } catch (error) {
    if (!(error instanceof ApiError) || !session?.tokens.refresh_token || error.status !== 401) throw error;
    try {
      const tokens = await raw<Tokens>("/auth/refresh", {
        method: "POST", body: JSON.stringify({ refresh_token: session.tokens.refresh_token }),
      });
      setSession({ user: session.user, tokens });
      return raw<T>(path, options, tokens.access_token);
    } catch {
      setSession(null);
      throw new Error("Your session has expired. Please sign in again.");
    }
  }
}

export async function uploadFormData<T>(
  path: string,
  body: FormData,
  onProgress: (percent: number) => void,
): Promise<T> {
  try {
    return await multipartRequest<T>(path, body, session?.tokens.access_token, onProgress);
  } catch (error) {
    if (!(error instanceof ApiError) || !session?.tokens.refresh_token || error.status !== 401) {
      throw error;
    }
    try {
      const tokens = await raw<Tokens>("/auth/refresh", {
        method: "POST", body: JSON.stringify({ refresh_token: session.tokens.refresh_token }),
      });
      setSession({ user: session.user, tokens });
      return multipartRequest<T>(path, body, tokens.access_token, onProgress);
    } catch {
      setSession(null);
      throw new Error("Your session has expired. Please sign in again.");
    }
  }
}

export const json = (method: string, body?: unknown): RequestInit => ({
  method, body: body === undefined ? undefined : JSON.stringify(body),
});

export async function authenticate(mode: "login" | "register", values: Record<string, string>) {
  const result = await raw<AuthResponse>(`/auth/${mode}`, json("POST", values));
  if (mode === "login") setSession(result);
  return result;
}

export async function signOut() {
  const current = session;
  setSession(null);
  if (current) {
    await raw("/auth/logout", json("POST", { refresh_token: current.tokens.refresh_token }),
      current.tokens.access_token).catch(() => undefined);
  }
}
