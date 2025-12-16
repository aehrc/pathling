/**
 * Hook for polling bulk submit job status using TanStack Query.
 *
 * @author John Grimes
 */

import { useQuery } from "@tanstack/react-query";
import { useRef, useEffect } from "react";
import { config } from "../config";
import { useAuth } from "../contexts/AuthContext";
import { pollBulkSubmitJobStatus } from "../services/bulkSubmit";
import { UnauthorizedError } from "../types/errors";
import type { StatusManifest } from "../types/bulkSubmit";
import type { JobStatus } from "../types/job";

interface UseBulkSubmitJobPollingOptions {
  jobId: string;
  pollUrl: string;
  status: JobStatus;
  onProgress: (id: string, progress: number) => void;
  onStatusChange: (id: string, status: JobStatus) => void;
  onComplete: (id: string, manifest: StatusManifest) => void;
  onError: (id: string, error: string) => void;
}

/**
 * Polls a bulk submit job's status at 3-second intervals until completion or error.
 * Uses the poll URL from the Content-Location header, matching the export/import pattern.
 * Handles 401 errors by triggering re-authentication.
 */
export function useBulkSubmitJobPolling({
  jobId,
  pollUrl,
  status,
  onProgress,
  onStatusChange,
  onComplete,
  onError,
}: UseBulkSubmitJobPollingOptions) {
  const { fhirBaseUrl } = config;
  const { client, clearSessionAndPromptLogin } = useAuth();
  const unauthorizedHandledRef = useRef(false);

  const isActive = status === "pending" || status === "in_progress";

  const query = useQuery({
    queryKey: ["bulkSubmitJob", jobId],
    queryFn: async () => {
      const accessToken = client?.state.tokenResponse?.access_token;
      return pollBulkSubmitJobStatus(fhirBaseUrl!, accessToken, pollUrl);
    },
    enabled: !!fhirBaseUrl && !!pollUrl && isActive,
    refetchInterval: isActive ? 3000 : false,
    staleTime: Infinity,
    retry: (failureCount, error) => {
      if (error instanceof UnauthorizedError) {
        return false;
      }
      return failureCount < 2;
    },
  });

  // Handle query results via callbacks.
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

  // Handle errors via callbacks.
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
