import { useQuery } from "@tanstack/react-query";
import { useEffect, useState } from "react";
import { api, getSession, type Session } from "./api";
import type { User } from "./types";

export function useAuthState() {
  const [session, setLocalSession] = useState<Session | null>(() => getSession());

  useEffect(() => {
    const syncSession = () => setLocalSession(getSession());
    window.addEventListener("sceptre-session", syncSession);
    return () => window.removeEventListener("sceptre-session", syncSession);
  }, []);

  const verification = useQuery({
    queryKey: ["auth", "me", session?.tokens.access_token],
    queryFn: () => api<User>("/auth/me"),
    enabled: Boolean(session),
    retry: false,
    staleTime: 60_000,
  });

  return {
    session,
    user: session && verification.isSuccess ? verification.data : null,
    isChecking: Boolean(session) && verification.isPending,
    isAuthenticated: Boolean(session && verification.isSuccess),
    verificationError: verification.error,
  };
}
