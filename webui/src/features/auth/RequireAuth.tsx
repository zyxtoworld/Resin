import { useEffect, useMemo, useState, type ReactElement } from "react";
import { Navigate, useLocation } from "react-router-dom";
import { useAuthStore } from "./auth-store";

type RequireAuthProps = {
  children: ReactElement;
};

type AuthCheckState = {
  key: symbol;
  checked: boolean;
  anonymousAllowed: boolean;
};

export function RequireAuth({ children }: RequireAuthProps) {
  const token = useAuthStore((state) => state.token);
  const location = useLocation();
  const authCheckKey = useMemo(() => Symbol(token ? "authenticated" : "anonymous"), [token]);
  const [authCheck, setAuthCheck] = useState<AuthCheckState>(() => ({
    key: authCheckKey,
    checked: Boolean(token),
    anonymousAllowed: false,
  }));

  useEffect(() => {
    if (token) {
      return;
    }

    let active = true;
    const controller = new AbortController();

    const checkAuthMode = async () => {
      let anonymousAllowed = false;
      try {
        const response = await fetch("/api/v1/system/info", {
          method: "GET",
          signal: controller.signal,
        });
        // /api/v1/system/info returns 200 only when admin auth is disabled.
        anonymousAllowed = response.ok;
      } catch {
        anonymousAllowed = false;
      }

      if (active) {
        setAuthCheck({ key: authCheckKey, checked: true, anonymousAllowed });
      }
    };

    void checkAuthMode();

    return () => {
      active = false;
      controller.abort();
    };
  }, [authCheckKey, token]);

  const authCheckIsCurrent = authCheck.key === authCheckKey;
  const checked = Boolean(token) || (authCheckIsCurrent && authCheck.checked);
  const anonymousAllowed = !token && authCheckIsCurrent && authCheck.anonymousAllowed;

  if (token || anonymousAllowed) {
    return children;
  }

  if (!checked) {
    return null;
  }

  const next = `${location.pathname}${location.search}`;
  return <Navigate to={`/login?next=${encodeURIComponent(next)}`} replace />;
}
