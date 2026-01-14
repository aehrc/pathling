/**
 * Context for managing SMART on FHIR authentication state.
 *
 * @author John Grimes
 */

import { createContext, type ReactNode, use, useCallback, useEffect, useState } from "react";

import { registerClearSession } from "../main";

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

/**
 * Provider component for SMART on FHIR authentication state.
 *
 * @param root0 - The component props.
 * @param root0.children - The child components to render.
 * @returns The provider component wrapping children.
 */
export function AuthProvider({ children }: Readonly<{ children: ReactNode }>) {
  const [state, setState] = useState<AuthState>({
    isAuthenticated: false,
    isLoading: false,
    client: null,
    error: null,
    authRequired: null,
    sessionExpired: false,
  });

  const setClient = (client: Client) => {
    setState((prev) => ({
      ...prev,
      isAuthenticated: true,
      isLoading: false,
      client,
      error: null,
    }));
  };

  const setAuthRequired = (required: boolean) => {
    setState((prev) => ({
      ...prev,
      authRequired: required,
    }));
  };

  const setError = (error: string) => {
    setState((prev) => ({
      ...prev,
      isLoading: false,
      error,
    }));
  };

  const setLoading = (loading: boolean) => {
    setState((prev) => ({
      ...prev,
      isLoading: loading,
      error: loading ? null : prev.error,
    }));
  };

  const setSessionExpired = (expired: boolean) => {
    setState((prev) => ({
      ...prev,
      sessionExpired: expired,
    }));
  };

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

  const logout = () => {
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
    // Reload the page to reset the app state.
    window.location.reload();
  };

  // Register the session clearing function for global 401 handling.
  useEffect(() => {
    registerClearSession(clearSessionAndPromptLogin);
  }, [clearSessionAndPromptLogin]);

  return (
    <AuthContext
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
    </AuthContext>
  );
}

/**
 * Hook for accessing the authentication context.
 *
 * @returns The authentication context value.
 * @throws Error if used outside of an AuthProvider.
 */
export function useAuth(): AuthContextValue {
  const context = use(AuthContext);
  if (!context) {
    throw new Error("useAuth must be used within an AuthProvider");
  }
  return context;
}
