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

// =============================================================================
// Async Job Executor Types
// =============================================================================

/**
 * Options for executing an async job with polling.
 */
export interface AsyncJobExecutorOptions<
  TKickOffResult,
  TStatusResult,
  TFinalResult,
> {
  kickOff: () => Promise<TKickOffResult>;
  getJobId: (kickOffResult: TKickOffResult) => string;
  checkStatus: (jobId: string) => Promise<TStatusResult>;
  isComplete: (statusResult: TStatusResult) => boolean;
  getResult: (statusResult: TStatusResult) => TFinalResult;
  cancel: (jobId: string) => Promise<void>;
  pollingInterval?: number;
  onProgress?: (statusResult: TStatusResult) => void;
}

/**
 * A handle for controlling a running async job.
 */
export interface AsyncJobHandle<TFinalResult> {
  result: Promise<TFinalResult>;
  cancel: () => Promise<void>;
}

/**
 * Executes an async job with polling, handling the kick-off, status checking,
 * and cancellation. Returns a handle with the result promise and a cancel function.
 */
export type AsyncJobExecutorFn = <TKickOffResult, TStatusResult, TFinalResult>(
  options: AsyncJobExecutorOptions<TKickOffResult, TStatusResult, TFinalResult>,
) => AsyncJobHandle<TFinalResult>;

/**
 * Default polling interval in milliseconds.
 */
const DEFAULT_POLLING_INTERVAL = 3000;

/**
 * Executes an async job with polling, handling the kick-off, status checking,
 * and cancellation.
 *
 * @param options - Configuration for the async job execution.
 * @returns A handle with the result promise and a cancel function.
 *
 * @example
 * const handle = executeAsyncJob({
 *   kickOff: () => importKickOff(baseUrl, { ... }),
 *   getJobId: (result) => result.jobId,
 *   checkStatus: (jobId) => jobStatus(baseUrl, { jobId }),
 *   isComplete: (status) => status.status === "complete",
 *   getResult: (status) => status.result,
 *   cancel: (jobId) => jobCancel(baseUrl, { jobId }),
 *   pollingInterval: 3000,
 *   onProgress: (status) => console.log(status.progress),
 * });
 *
 * // Wait for result.
 * const result = await handle.result;
 *
 * // Or cancel.
 * await handle.cancel();
 */
export function executeAsyncJob<TKickOffResult, TStatusResult, TFinalResult>(
  options: AsyncJobExecutorOptions<TKickOffResult, TStatusResult, TFinalResult>,
): AsyncJobHandle<TFinalResult> {
  const {
    kickOff,
    getJobId,
    checkStatus,
    isComplete,
    getResult,
    cancel,
    pollingInterval = DEFAULT_POLLING_INTERVAL,
    onProgress,
  } = options;

  let jobId: string | undefined;
  let cancelled = false;
  let timeoutId: ReturnType<typeof setTimeout> | undefined;

  const resultPromise = new Promise<TFinalResult>((resolve, reject) => {
    const poll = async () => {
      if (cancelled) {
        return;
      }

      try {
        const statusResult = await checkStatus(jobId!);

        if (cancelled) {
          return;
        }

        if (isComplete(statusResult)) {
          resolve(getResult(statusResult));
          return;
        }

        if (onProgress) {
          onProgress(statusResult);
        }

        // Schedule next poll.
        timeoutId = setTimeout(poll, pollingInterval);
      } catch (error) {
        reject(error);
      }
    };

    // Start the job.
    kickOff()
      .then((kickOffResult) => {
        if (cancelled) {
          return;
        }

        jobId = getJobId(kickOffResult);

        // Start polling after the initial interval.
        timeoutId = setTimeout(poll, pollingInterval);
      })
      .catch(reject);
  });

  const cancelFn = async () => {
    cancelled = true;

    if (timeoutId) {
      clearTimeout(timeoutId);
    }

    if (jobId) {
      await cancel(jobId);
    }
  };

  return {
    result: resultPromise,
    cancel: cancelFn,
  };
}
