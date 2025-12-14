/**
 * Context for managing SMART on FHIR authentication state.
 *
 * @author John Grimes
 */

import {
  createContext,
  useContext,
  useState,
  useCallback,
  type ReactNode,
} from "react";
import type Client from "fhirclient/lib/Client";

interface AuthState {
  isAuthenticated: boolean;
  isLoading: boolean;
  client: Client | null;
  error: string | null;
}

interface AuthContextValue extends AuthState {
  setClient: (client: Client) => void;
  setError: (error: string) => void;
  setLoading: (loading: boolean) => void;
  logout: () => void;
}

const AuthContext = createContext<AuthContextValue | null>(null);

export function AuthProvider({ children }: { children: ReactNode }) {
  const [state, setState] = useState<AuthState>({
    isAuthenticated: false,
    isLoading: false,
    client: null,
    error: null,
  });

  const setClient = useCallback((client: Client) => {
    setState({
      isAuthenticated: true,
      isLoading: false,
      client,
      error: null,
    });
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

  const logout = useCallback(() => {
    setState({
      isAuthenticated: false,
      isLoading: false,
      client: null,
      error: null,
    });
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
