/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Context for managing SMART on FHIR authentication state.
 *
 * @author John Grimes
 */

import { createContext, type ReactNode, use, useCallback, useEffect, useState } from "react";

import { registerSessionExpiryHandler } from "../services/sessionExpiry";

import type Client from "fhirclient/lib/Client";

interface AuthState {
  isAuthenticated: boolean;
  client: Client | null;
  authRequired: boolean | null; // null = unknown, true = required, false = not required
  sessionExpired: boolean;
}

interface AuthContextValue extends AuthState {
  setClient: (client: Client) => void;
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
    client: null,
    authRequired: null,
    sessionExpired: false,
  });

  const setClient = (client: Client) => {
    setState((prev) => ({
      ...prev,
      isAuthenticated: true,
      client,
    }));
  };

  const setAuthRequired = (required: boolean) => {
    setState((prev) => ({
      ...prev,
      authRequired: required,
    }));
  };

  const setSessionExpired = (expired: boolean) => {
    setState((prev) => ({
      ...prev,
      sessionExpired: expired,
    }));
  };

  // An authorisation failure only means the session expired if one was held.
  // Deciding inside the updater keeps the check against the current state
  // without introducing any state of its own, and makes the prompt idempotent
  // so concurrent failures raise a single dialog.
  const clearSessionAndPromptLogin = useCallback(() => {
    setState((prev) =>
      prev.isAuthenticated
        ? { ...prev, isAuthenticated: false, client: null, sessionExpired: true }
        : prev,
    );
    // Clear the key unconditionally, so a stale one from a previous page load
    // does not survive.
    sessionStorage.removeItem("SMART_KEY");
  }, []);

  const logout = () => {
    setState((prev) => ({
      ...prev,
      isAuthenticated: false,
      client: null,
      sessionExpired: false,
    }));
    // Clear any stored session data.
    sessionStorage.removeItem("SMART_KEY");
    // Reload the page to reset the app state.
    window.location.reload();
  };

  // Register the session clearing function for global 401 handling.
  useEffect(() => {
    registerSessionExpiryHandler(clearSessionAndPromptLogin);
  }, [clearSessionAndPromptLogin]);

  return (
    <AuthContext
      value={{
        ...state,
        setClient,
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
