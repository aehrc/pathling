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
 */

// noinspection JSUnusedGlobalSymbols

import pRetry, { AbortError, FailedAttemptError } from "p-retry";
import {
  PathlingClientOptionsResolved,
  QueryOptions,
  RetryConfig,
} from "./index.js";
import { JobClient, JobInProgressError } from "./job.js";
import { OpOutcomeError } from "./OperationOutcome.js";

/**
 * A set of options that can be passed to the `retry` function.
 */
export interface RetryOptions {
  retry: RetryConfig;
  verboseLogging?: boolean;
  message?: string;
}

/**
 * Retry a function according to the supplied options.
 *
 * @param promise The promise to retry.
 * @param options The retry options.
 * @returns A promise that resolves to the result of the function.
 */
export function retry(promise: () => Promise<any>, options: RetryOptions) {
  return pRetry(
    (attemptCount) => {
      if (options.verboseLogging && options.message) {
        console.info("%s, attempt %d", options.message, attemptCount);
      }
      return promise();
    },
    {
      retries: options.retry.times,
      minTimeout: options.retry.wait * 1000,
      factor: options.retry.backOff,
      onFailedAttempt: (error: FailedAttemptError) => {
        if (options?.verboseLogging) {
          console.info(
            "Attempt #%d not complete, %d retries left - %s",
            error.attemptNumber,
            error.retriesLeft,
            error.message
          );
        }
      },
    }
  );
}

/**
 * Extract a status URL from the headers of a response.
 */
export function getStatusUrl(response: Response): string {
  const statusUrl = response.headers.get("content-location");
  if (!statusUrl) {
    throw new Error("No Content-Location header found");
  }
  return statusUrl;
}

/**
 * Wait for the eventual response provided by an async job status URL.
 */
export function waitForAsyncResult<ResponseType>(
  url: string,
  message: string,
  clientOptions: PathlingClientOptionsResolved,
  requestOptions?: QueryOptions
): Promise<ResponseType> {
  const jobClient = new JobClient();
  return retry(async () => retryHandler(jobClient, url, requestOptions), {
    retry: clientOptions.asyncRetry,
    verboseLogging: clientOptions.verboseLogging,
    message,
  });
}

async function retryHandler(
  jobClient: JobClient,
  url: string,
  requestOptions?: QueryOptions
): Promise<any> {
  try {
    return await jobClient.request(url, requestOptions);
  } catch (e: any) {
    if (e instanceof OpOutcomeError && e.message === "Job ID not found") {
      // If the error is an OpOutcomeError with a message of "Job ID not found",
      // retry the request with cache-busting headers.
      // This is to allow for situations where the server has been restarted but
      // responses that refer to old job IDs still exist in caches.
      return jobClient.request(url, {
        ...requestOptions,
        bustCache: true,
      });
    } else if (!(e instanceof JobInProgressError)) {
      // If the error is not a JobInProgressError, abort the retry.
      throw new AbortError(e);
    } else {
      // Otherwise, report progress and throw the JobInProgressError to trigger
      // a retry.
      reportProgress(e as Error, requestOptions);
      throw e;
    }
  }
}

function reportProgress(e: Error, requestOptions?: QueryOptions) {
  const progress = (e as JobInProgressError).progress;
  if (
    (e as Error).name === "JobInProgressError" &&
    progress &&
    requestOptions?.onProgress
  ) {
    requestOptions.onProgress(progress);
  }
}
