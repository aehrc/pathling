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

import { useState, useCallback, useRef } from "react";

import { executeAsyncJob, parseProgressHeader } from "../api";

import type { AsyncJobExecutorOptions } from "../api";

/**
 * Status of an async job operation.
 */
export type AsyncJobStatus =
  | "idle"
  | "pending"
  | "in-progress"
  | "complete"
  | "error"
  | "cancelled";

/**
 * Options for starting an async job.
 */
export interface AsyncJobOptions {
  /** Callback when progress updates. */
  onProgress?: (progress: number) => void;
  /** Callback when job completes. */
  onComplete?: () => void;
  /** Callback when job fails. */
  onError?: (error: Error) => void;
}

/**
 * Result of an async job hook.
 */
export interface UseAsyncJobResult<TRequest, TResult> {
  /** Current status of the job. */
  status: AsyncJobStatus;
  /** Progress percentage (0-100) when available. */
  progress?: number;
  /** The final result when status is "complete". */
  result?: TResult;
  /** Error object when status is "error". */
  error?: Error;
  /** The request that produced the current result/error. */
  request?: TRequest;
  /** Start execution with the given request. If already running, cancels and restarts. */
  startWith: (request: TRequest) => void;
  /** Reset all state back to idle. */
  reset: () => void;
  /** Function to cancel the job. */
  cancel: () => Promise<void>;
  /** Delete job files on server without changing status. Call after job completes. */
  deleteJob: () => Promise<void>;
}

/**
 * Base hook for async job operations. Wraps executeAsyncJob with React state.
 *
 * @param buildOptions - Function that builds executor options from a request object.
 * @param callbacks - Optional callbacks for progress, completion, and error events.
 * @param callbacks.onProgress - Called when progress is updated.
 * @param callbacks.onComplete - Called when the job completes successfully.
 * @param callbacks.onError - Called when the job fails with an error.
 * @returns Hook result with status, result, and control functions.
 */
export function useAsyncJob<TRequest, TKickOffResult, TStatusResult, TResult>(
  buildOptions: (
    request: TRequest,
  ) => Omit<
    AsyncJobExecutorOptions<TKickOffResult, TStatusResult, TResult>,
    "onProgress"
  >,
  callbacks?: {
    onProgress?: (progress: number) => void;
    onComplete?: () => void;
    onError?: (error: Error) => void;
  },
): UseAsyncJobResult<TRequest, TResult> {
  const [status, setStatus] = useState<AsyncJobStatus>("idle");
  const [progress, setProgress] = useState<number | undefined>(undefined);
  const [result, setResult] = useState<TResult | undefined>(undefined);
  const [error, setError] = useState<Error | undefined>(undefined);
  const [request, setRequest] = useState<TRequest | undefined>(undefined);

  const cancelRef = useRef<(() => Promise<void>) | null>(null);

  const startWith = useCallback(
    (newRequest: TRequest) => {
      // If already running, cancel the current job first.
      if (cancelRef.current) {
        cancelRef.current();
      }

      setRequest(newRequest);
      setStatus("pending");
      setProgress(undefined);
      setResult(undefined);
      setError(undefined);

      const options = buildOptions(newRequest);

      const handle = executeAsyncJob<TKickOffResult, TStatusResult, TResult>({
        ...options,
        onProgress: (statusResult) => {
          setStatus("in-progress");
          // Try to extract progress from the status result if it has a progress field.
          const statusWithProgress = statusResult as { progress?: string };
          if (statusWithProgress.progress) {
            const progressValue = parseProgressHeader(
              statusWithProgress.progress,
            );
            if (progressValue !== undefined) {
              setProgress(progressValue);
              callbacks?.onProgress?.(progressValue);
            }
          }
        },
      });

      cancelRef.current = handle.cancel;

      handle.result
        .then((res) => {
          setResult(res);
          setStatus("complete");
          callbacks?.onComplete?.();
        })
        .catch((err) => {
          const error = err instanceof Error ? err : new Error("Unknown error");
          setError(error);
          setStatus("error");
          callbacks?.onError?.(error);
        });
    },
    [buildOptions, callbacks],
  );

  const reset = useCallback(() => {
    // Cancel any running job.
    if (cancelRef.current) {
      cancelRef.current();
      cancelRef.current = null;
    }
    setStatus("idle");
    setProgress(undefined);
    setResult(undefined);
    setError(undefined);
    setRequest(undefined);
  }, []);

  const cancel = useCallback(async () => {
    if (cancelRef.current) {
      await cancelRef.current();
      setStatus("cancelled");
    }
  }, []);

  const deleteJob = useCallback(async () => {
    // Delete job files on server without changing status.
    // This allows cleanup of completed exports.
    if (cancelRef.current) {
      await cancelRef.current();
    }
  }, []);

  return {
    status,
    progress,
    result,
    error,
    request,
    startWith,
    reset,
    cancel,
    deleteJob,
  };
}
