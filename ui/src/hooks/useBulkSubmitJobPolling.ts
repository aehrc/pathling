/**
 * Hook for polling bulk submit job status using TanStack Query.
 *
 * @author John Grimes
 */

import { useQuery } from "@tanstack/react-query";
import { useEffect, useRef } from "react";
import { config } from "../config";
import { useAuth } from "../contexts/AuthContext";
import {
  checkBulkSubmitStatus,
  pollBulkSubmitJobStatus,
} from "../services/bulkSubmit";
import type { StatusManifest, SubmitterIdentifier } from "../types/bulkSubmit";
import { UnauthorizedError } from "../types/errors";
import type { JobStatus } from "../types/job";

interface UseBulkSubmitJobPollingOptions {
  jobId: string;
  pollUrl: string | null;
  submissionId: string;
  submitter: SubmitterIdentifier;
  status: JobStatus;
  onProgress: (id: string, progress: number) => void;
  onStatusChange: (id: string, status: JobStatus) => void;
  onPollUrlObtained: (id: string, pollUrl: string) => void;
  onComplete: (id: string, manifest: StatusManifest) => void;
  onError: (id: string, error: string) => void;
}

/**
 * Polls a bulk submit job's status at 3-second intervals until completion or error. When pollUrl
 * is null (job hasn't started yet), this hook will poll checkBulkSubmitStatus to wait for the job
 * to be created. Once a poll URL is obtained, it switches to polling pollBulkSubmitJobStatus for
 * progress updates.
 */
export function useBulkSubmitJobPolling({
  jobId,
  pollUrl,
  submissionId,
  submitter,
  status,
  onProgress,
  onStatusChange,
  onPollUrlObtained,
  onComplete,
  onError,
}: UseBulkSubmitJobPollingOptions) {
  const { fhirBaseUrl } = config;
  const { client, clearSessionAndPromptLogin } = useAuth();
  const unauthorizedHandledRef = useRef(false);

  const isActive = status === "pending" || status === "in_progress";
  const isWaitingForJob = pollUrl === null;

  // Query for when we're waiting for the job to start (no poll URL yet).
  const statusQuery = useQuery({
    queryKey: ["bulkSubmitStatus", jobId],
    queryFn: async () => {
      const accessToken = client?.state.tokenResponse?.access_token;
      return checkBulkSubmitStatus(fhirBaseUrl!, accessToken, {
        submissionId,
        submitter,
      });
    },
    enabled: !!fhirBaseUrl && isActive && isWaitingForJob,
    refetchInterval: isActive && isWaitingForJob ? 5000 : false,
    staleTime: Infinity,
    retry: (failureCount, error) => {
      if (error instanceof UnauthorizedError) {
        return false;
      }
      return failureCount < 2;
    },
  });

  // Query for when we have a poll URL and need to poll for progress.
  const jobQuery = useQuery({
    queryKey: ["bulkSubmitJob", jobId],
    queryFn: async () => {
      const accessToken = client?.state.tokenResponse?.access_token;
      return pollBulkSubmitJobStatus(fhirBaseUrl!, accessToken, pollUrl!);
    },
    enabled: !!fhirBaseUrl && !!pollUrl && isActive,
    refetchInterval: isActive && !isWaitingForJob ? 3000 : false,
    staleTime: Infinity,
    retry: (failureCount, error) => {
      if (error instanceof UnauthorizedError) {
        return false;
      }
      return failureCount < 2;
    },
  });

  // Handle status query results (waiting for job to start).
  useEffect(() => {
    if (statusQuery.data) {
      const result = statusQuery.data;
      if (result.status === "completed" && result.manifest) {
        onComplete(jobId, result.manifest);
      } else if (result.status === "aborted") {
        onStatusChange(jobId, "cancelled");
      } else if (result.status === "pending") {
        onPollUrlObtained(jobId, result.pollUrl);
        onStatusChange(jobId, "in_progress");
      }
      // If status is "retry", we just keep polling.
    }
  }, [statusQuery.data, jobId, onComplete, onPollUrlObtained, onStatusChange]);

  // Handle job query results (polling for progress).
  useEffect(() => {
    if (jobQuery.data) {
      const result = jobQuery.data;
      if (result.status === "completed" && result.manifest) {
        onComplete(jobId, result.manifest);
      } else if (result.status === "in_progress") {
        if (result.progress !== undefined) {
          onProgress(jobId, result.progress);
        }
        onStatusChange(jobId, "in_progress");
      }
    }
  }, [jobQuery.data, jobId, onComplete, onProgress, onStatusChange]);

  // Handle errors from either query.
  useEffect(() => {
    const error = statusQuery.error || jobQuery.error;
    if (error) {
      if (
        error instanceof UnauthorizedError &&
        !unauthorizedHandledRef.current
      ) {
        unauthorizedHandledRef.current = true;
        clearSessionAndPromptLogin();
      } else if (!(error instanceof UnauthorizedError)) {
        onError(
          jobId,
          error instanceof Error ? error.message : "Unknown error",
        );
      }
    }
  }, [
    statusQuery.error,
    jobQuery.error,
    jobId,
    onError,
    clearSessionAndPromptLogin,
  ]);

  return isWaitingForJob ? statusQuery : jobQuery;
}
