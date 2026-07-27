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
 * Entry point for the Pathling Export UI application.
 *
 * @author John Grimes
 */

import { Theme } from "@radix-ui/themes";
import { MutationCache, QueryCache, QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { StrictMode, useSyncExternalStore } from "react";
import { createRoot } from "react-dom/client";
import { BrowserRouter } from "react-router";

import "@radix-ui/themes/styles.css";
import "./index.css";
import App from "./App";
import { ErrorBoundary } from "./components/ErrorBoundary";
import { AuthProvider } from "./contexts/AuthContext";
import { ToastProvider } from "./contexts/ToastContext";
import { notifyUnauthorized } from "./services/sessionExpiry";
import { UnauthorizedError } from "./types/errors";
import { setupGlobalErrorHandlers } from "./utils/errorHandler";

setupGlobalErrorHandlers();

/**
 * Global error handler for TanStack Query. Reports an authorisation failure to
 * the authentication state, which decides whether it means the session expired
 * or the user was never logged in.
 *
 * @param error - The error from a failed query or mutation.
 */
function handleGlobalError(error: Error): void {
  if (error instanceof UnauthorizedError) {
    notifyUnauthorized();
  }
}

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: false,
      refetchOnWindowFocus: false,
      staleTime: 0,
      gcTime: 0,
    },
    mutations: {
      retry: false,
    },
  },
  queryCache: new QueryCache({
    onError: handleGlobalError,
  }),
  mutationCache: new MutationCache({
    onError: handleGlobalError,
  }),
});

const darkModeQuery = window.matchMedia("(prefers-color-scheme: dark)");

function subscribeToColorScheme(callback: () => void): () => void {
  darkModeQuery.addEventListener("change", callback);
  return () => darkModeQuery.removeEventListener("change", callback);
}

function getColorScheme(): "light" | "dark" {
  return darkModeQuery.matches ? "dark" : "light";
}

function Root() {
  const appearance = useSyncExternalStore(subscribeToColorScheme, getColorScheme);

  return (
    <Theme accentColor="teal" grayColor="slate" radius="medium" appearance={appearance}>
      <ToastProvider>
        <QueryClientProvider client={queryClient}>
          <BrowserRouter basename="/admin">
            <AuthProvider>
              <ErrorBoundary>
                <App />
              </ErrorBoundary>
            </AuthProvider>
          </BrowserRouter>
        </QueryClientProvider>
      </ToastProvider>
    </Theme>
  );
}

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <Root />
  </StrictMode>,
);
