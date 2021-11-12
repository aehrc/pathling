import pRetry, { FailedAttemptError } from "p-retry";
import { PathlingClientOptionsResolved, RetryConfig } from "./index";
import axios, { AxiosResponse } from "axios";

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
    throw "No Content-Location header found";
  }
  return statusUrl;
}

/**
 * An error that is raised when we check on the status of a job that is not yet
 * finished.
 */
export class JobInProgressError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "JobInProgressError";
  }
}

/**
 * Wait for the eventual response provided by an async job status URL.
 */
export function waitForAsyncResult(
  url: string,
  message: string,
  options: PathlingClientOptionsResolved
): Promise<any> {
  return retry(
    async () => {
      const response = await axios.get<any>(url);

      if (response.status === 200) {
        return response.data;
      } else if (response.status === 202) {
        const progress = response.headers["x-progress"];
        const message = progress ? progress : "(no progress message)";
        throw new JobInProgressError(`Job in progress: ${message}`);
      } else {
        throw `Unexpected status: ${response.status} ${response.statusText}`;
      }
    },
    {
      retry: options.asyncRetry,
      verboseLogging: options.verboseLogging,
      message,
    }
  );
}
