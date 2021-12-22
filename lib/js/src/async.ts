/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

// noinspection JSUnusedGlobalSymbols

import pRetry, { AbortError, FailedAttemptError } from "p-retry";
import {
  PathlingClientOptionsResolved,
  QueryOptions,
  RetryConfig,
} from "./index";
import { AxiosResponse } from "axios";
import { JobClient, JobInProgressError } from "./job";
import { buildResponseError } from "./OperationOutcome";

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
export function getStatusUrl<T>(response: AxiosResponse<T>) {
  const statusUrl = response.headers["content-location"];
  if (!statusUrl) {
    throw new Error("No Content-Location header found");
  }
  return statusUrl;
}

/**
 * Wait for the eventual response provided by an async job status URL.
 */
export function waitForAsyncResult(
  url: string,
  message: string,
  clientOptions: PathlingClientOptionsResolved,
  requestOptions?: QueryOptions
): Promise<any> {
  const jobClient = new JobClient();
  return retry(
    async () => {
      try {
        return await jobClient.request(url, requestOptions);
      } catch (e: any) {
        if (e.response && (e.response as AxiosResponse).status !== 202) {
          throw new AbortError(buildResponseError(e));
        }
        reportProgress(e as Error, requestOptions);
        throw e;
      }
    },
    {
      retry: clientOptions.asyncRetry,
      verboseLogging: clientOptions.verboseLogging,
      message,
    }
  );
}

function reportProgress(e: Error, requestOptions?: QueryOptions) {
  if (
    (e as Error).name === "JobInProgressError" &&
    (e as JobInProgressError).progress &&
    requestOptions?.onProgress
  ) {
    requestOptions.onProgress((e as JobInProgressError).progress);
  }
}
