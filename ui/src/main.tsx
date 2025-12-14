/**
 * Entry point for the Pathling Export UI application.
 *
 * @author John Grimes
 */

import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { BrowserRouter } from "react-router";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { Theme } from "@radix-ui/themes";
import "@radix-ui/themes/styles.css";
import "./index.css";
import { SettingsProvider } from "./contexts/SettingsContext";
import { AuthProvider } from "./contexts/AuthContext";
import { JobProvider } from "./contexts/JobContext";
import App from "./App";

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 1,
      refetchOnWindowFocus: false,
    },
  },
});

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <Theme accentColor="blue" grayColor="slate" radius="medium">
      <QueryClientProvider client={queryClient}>
        <BrowserRouter>
          <SettingsProvider>
            <AuthProvider>
              <JobProvider>
                <App />
              </JobProvider>
            </AuthProvider>
          </SettingsProvider>
        </BrowserRouter>
      </QueryClientProvider>
    </Theme>
  </StrictMode>
);
