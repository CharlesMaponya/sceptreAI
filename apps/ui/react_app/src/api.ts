import type { AuthResponse, Tokens } from "./types";

const API_ROOT = (import.meta.env.VITE_API_URL as string | undefined) || "/api/v1";
const STORAGE_KEY = "sceptre.session";

class ApiError extends Error {
  constructor(message: string, readonly status: number) { super(message); }
}

type Session = { user: AuthResponse["user"]; tokens: Tokens };
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

async function raw<T>(path: string, options: RequestInit = {}, token?: string): Promise<T> {
  const headers = new Headers(options.headers);
  if (!(options.body instanceof FormData)) headers.set("Content-Type", "application/json");
  if (token) headers.set("Authorization", `Bearer ${token}`);
  const response = await fetch(`${API_ROOT}${path}`, { ...options, headers });
  const data = response.status === 204 ? null : await response.json().catch(() => response.text());
  if (!response.ok) throw new ApiError(messageFrom(data, response.status), response.status);
  return data as T;
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

export const json = (method: string, body?: unknown): RequestInit => ({
  method, body: body === undefined ? undefined : JSON.stringify(body),
});

export async function authenticate(mode: "login" | "register", values: Record<string, string>) {
  const result = await raw<AuthResponse>(`/auth/${mode}`, json("POST", values));
  setSession(result);
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

export const fileToBase64 = (file: File) => new Promise<string>((resolve, reject) => {
  const reader = new FileReader();
  reader.onerror = () => reject(reader.error);
  reader.onload = () => resolve(String(reader.result).split(",")[1] || "");
  reader.readAsDataURL(file);
});
