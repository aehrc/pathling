/**
 * Hook for polling export job status using TanStack Query.
 *
 * @author John Grimes
 */

import { useQuery } from "@tanstack/react-query";
import { useRef, useEffect } from "react";
import { useAuth } from "../contexts/AuthContext";
import { useSettings } from "../contexts/SettingsContext";
import { pollJobStatus } from "../services/export";
import { UnauthorizedError } from "../types/errors";
import type { ExportManifest } from "../types/export";
import type { JobStatus } from "../types/job";

interface UseExportJobPollingOptions {
  jobId: string;
  pollUrl: string;
  status: JobStatus;
  onProgress: (id: string, progress: number) => void;
  onStatusChange: (id: string, status: JobStatus) => void;
  onComplete: (id: string, manifest: ExportManifest) => void;
  onError: (id: string, error: string) => void;
}

/**
 * Polls an export job's status at 3-second intervals until completion or error.
 * Handles 401 errors by triggering re-authentication.
 */
export function useExportJobPolling({
  jobId,
  pollUrl,
  status,
  onProgress,
  onStatusChange,
  onComplete,
  onError,
}: UseExportJobPollingOptions) {
  const { fhirBaseUrl } = useSettings();
  const { client, clearSessionAndPromptLogin } = useAuth();
  const unauthorizedHandledRef = useRef(false);

  const isActive = status === "pending" || status === "in_progress";

  const query = useQuery({
    queryKey: ["exportJob", jobId],
    queryFn: async () => {
      const accessToken = client?.state.tokenResponse?.access_token;
      return pollJobStatus(fhirBaseUrl!, accessToken, pollUrl);
    },
    enabled: !!fhirBaseUrl && isActive,
    refetchInterval: isActive ? 3000 : false,
    retry: (failureCount, error) => {
      if (error instanceof UnauthorizedError) {
        return false;
      }
      return failureCount < 2;
    },
  });

  // Handle query results and errors via callbacks.
  useEffect(() => {
    if (query.data) {
      const result = query.data;
      if (result.status === "completed" && result.manifest) {
        onComplete(jobId, result.manifest);
      } else if (result.status === "in_progress") {
        if (result.progress !== undefined) {
          onProgress(jobId, result.progress);
        }
        onStatusChange(jobId, "in_progress");
      }
    }
  }, [query.data, jobId, onComplete, onProgress, onStatusChange]);

  useEffect(() => {
    if (query.error) {
      if (
        query.error instanceof UnauthorizedError &&
        !unauthorizedHandledRef.current
      ) {
        unauthorizedHandledRef.current = true;
        clearSessionAndPromptLogin();
      } else if (!(query.error instanceof UnauthorizedError)) {
        onError(
          jobId,
          query.error instanceof Error ? query.error.message : "Unknown error",
        );
      }
    }
  }, [query.error, jobId, onError, clearSessionAndPromptLogin]);

  return query;
}
