/**
 * Entry point for the Pathling Export UI application.
 *
 * @author John Grimes
 */

import { StrictMode, useSyncExternalStore } from "react";
import { createRoot } from "react-dom/client";
import { BrowserRouter } from "react-router";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { Theme } from "@radix-ui/themes";
import "@radix-ui/themes/styles.css";
import "./index.css";
import { AuthProvider } from "./contexts/AuthContext";
import { JobProvider } from "./contexts/JobContext";
import { ToastProvider } from "./contexts/ToastContext";
import { ErrorBoundary } from "./components/ErrorBoundary";
import { setupGlobalErrorHandlers } from "./utils/errorHandler";
import App from "./App";

setupGlobalErrorHandlers();

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 1,
      refetchOnWindowFocus: false,
    },
  },
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
    <Theme accentColor="blue" grayColor="slate" radius="medium" appearance={appearance}>
      <ToastProvider>
        <QueryClientProvider client={queryClient}>
          <BrowserRouter basename="/admin">
            <AuthProvider>
              <JobProvider>
                <ErrorBoundary>
                  <App />
                </ErrorBoundary>
              </JobProvider>
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
