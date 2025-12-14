/**
 * Context for managing SMART on FHIR authentication state.
 *
 * @author John Grimes
 */

import { createContext, useContext, useState, useCallback, type ReactNode } from "react";
import type Client from "fhirclient/lib/Client";

interface AuthState {
  isAuthenticated: boolean;
  isLoading: boolean;
  client: Client | null;
  error: string | null;
  authRequired: boolean | null; // null = unknown, true = required, false = not required
  sessionExpired: boolean;
}

interface AuthContextValue extends AuthState {
  setClient: (client: Client) => void;
  setError: (error: string) => void;
  setLoading: (loading: boolean) => void;
  setAuthRequired: (required: boolean) => void;
  setSessionExpired: (expired: boolean) => void;
  clearSessionAndPromptLogin: () => void;
  logout: () => void;
}

const AuthContext = createContext<AuthContextValue | null>(null);

export function AuthProvider({ children }: { children: ReactNode }) {
  const [state, setState] = useState<AuthState>({
    isAuthenticated: false,
    isLoading: false,
    client: null,
    error: null,
    authRequired: null,
    sessionExpired: false,
  });

  const setClient = useCallback((client: Client) => {
    setState((prev) => ({
      ...prev,
      isAuthenticated: true,
      isLoading: false,
      client,
      error: null,
    }));
  }, []);

  const setAuthRequired = useCallback((required: boolean) => {
    setState((prev) => ({
      ...prev,
      authRequired: required,
    }));
  }, []);

  const setError = useCallback((error: string) => {
    setState((prev) => ({
      ...prev,
      isLoading: false,
      error,
    }));
  }, []);

  const setLoading = useCallback((loading: boolean) => {
    setState((prev) => ({
      ...prev,
      isLoading: loading,
      error: loading ? null : prev.error,
    }));
  }, []);

  const setSessionExpired = useCallback((expired: boolean) => {
    setState((prev) => ({
      ...prev,
      sessionExpired: expired,
    }));
  }, []);

  const clearSessionAndPromptLogin = useCallback(() => {
    setState((prev) => ({
      ...prev,
      isAuthenticated: false,
      isLoading: false,
      client: null,
      error: null,
      sessionExpired: true,
    }));
    sessionStorage.removeItem("SMART_KEY");
  }, []);

  const logout = useCallback(() => {
    setState((prev) => ({
      ...prev,
      isAuthenticated: false,
      isLoading: false,
      client: null,
      error: null,
      sessionExpired: false,
    }));
    // Clear any stored session data.
    sessionStorage.removeItem("SMART_KEY");
  }, []);

  return (
    <AuthContext.Provider
      value={{
        ...state,
        setClient,
        setError,
        setLoading,
        setAuthRequired,
        setSessionExpired,
        clearSessionAndPromptLogin,
        logout,
      }}
    >
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth(): AuthContextValue {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error("useAuth must be used within an AuthProvider");
  }
  return context;
}
