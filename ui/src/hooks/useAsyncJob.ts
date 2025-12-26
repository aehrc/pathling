/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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
 *
 * Author: John Grimes
 */

import { useState, useCallback, useRef } from "react";
import { executeAsyncJob, parseProgressHeader } from "../api";
import type { AsyncJobStatus } from "../types/hooks";
import type { AsyncJobExecutorOptions } from "../types/api";

export interface UseAsyncJobResult<TRequest, TResult> {
  status: AsyncJobStatus;
  progress?: number;
  result?: TResult;
  error?: Error;
  /** The request that produced the current result/error. */
  request?: TRequest;
  /** Start execution with the given request. If already running, cancels and restarts. */
  startWith: (request: TRequest) => void;
  /** Reset all state back to idle. */
  reset: () => void;
  cancel: () => Promise<void>;
}

/**
 * Base hook for async job operations. Wraps executeAsyncJob with React state.
 *
 * @param buildOptions - Function that builds executor options from a request object.
 * @param callbacks - Optional callbacks for progress, completion, and error events.
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

  return { status, progress, result, error, request, startWith, reset, cancel };
}
