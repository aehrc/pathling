/**
 * Generic hook for polling job status using TanStack Query.
 *
 * @author John Grimes
 */

import { useQuery } from "@tanstack/react-query";
import { useRef, useEffect } from "react";
import { config } from "../config";
import { useAuth } from "../contexts/AuthContext";
import { UnauthorizedError } from "../types/errors";
import type { JobStatus } from "../types/job";
import type { PollResult } from "../types/polling";

export interface UseJobPollingOptions<TManifest> {
  jobId: string;
  pollUrl: string;
  status: JobStatus;
  queryKey: string;
  pollFn: (
    fhirBaseUrl: string,
    accessToken: string | undefined,
    pollUrl: string,
  ) => Promise<PollResult<TManifest>>;
  onProgress: (id: string, progress: number) => void;
  onStatusChange: (id: string, status: JobStatus) => void;
  onComplete: (id: string, manifest: TManifest) => void;
  onError: (id: string, error: string) => void;
}

/**
 * Polls a job's status at 3-second intervals until completion or error.
 * Handles 401 errors by triggering re-authentication.
 */
export function useJobPolling<TManifest>({
  jobId,
  pollUrl,
  status,
  queryKey,
  pollFn,
  onProgress,
  onStatusChange,
  onComplete,
  onError,
}: UseJobPollingOptions<TManifest>) {
  const { fhirBaseUrl } = config;
  const { client, clearSessionAndPromptLogin } = useAuth();
  const unauthorizedHandledRef = useRef(false);

  const isActive = status === "pending" || status === "in_progress";

  const query = useQuery({
    queryKey: [queryKey, jobId],
    queryFn: async () => {
      const accessToken = client?.state.tokenResponse?.access_token;
      return pollFn(fhirBaseUrl!, accessToken, pollUrl);
    },
    enabled: !!fhirBaseUrl && isActive,
    refetchInterval: isActive ? 3000 : false,
    staleTime: Infinity,
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
