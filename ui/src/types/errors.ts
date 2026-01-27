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

import type { OperationOutcome } from "fhir/r4";

/**
 * Custom error types for API error handling.
 *
 * @author John Grimes
 */

/**
 * Error thrown when an API request receives a 401 Unauthorized response.
 * Indicates that the user's session has expired or is invalid.
 */
export class UnauthorizedError extends Error {
  /**
   * Creates a new UnauthorizedError instance.
   *
   * @param message - The error message.
   */
  constructor(message: string = "Session expired or unauthorized") {
    super(message);
    this.name = "UnauthorizedError";
  }
}

/**
 * Error thrown when an API request receives a 404 Not Found response.
 * Indicates that the requested resource does not exist.
 */
export class NotFoundError extends Error {
  /**
   * Creates a new NotFoundError instance.
   *
   * @param message - The error message.
   */
  constructor(message: string = "Resource not found") {
    super(message);
    this.name = "NotFoundError";
  }
}

/**
 * Error thrown when an API request returns a FHIR OperationOutcome.
 * Preserves the full OperationOutcome structure for detailed error display.
 */
export class OperationOutcomeError extends Error {
  /** The FHIR OperationOutcome resource from the response. */
  readonly operationOutcome: OperationOutcome;

  /** The HTTP status code from the response. */
  readonly status: number;

  /**
   * Creates a new OperationOutcomeError instance.
   *
   * @param operationOutcome - The FHIR OperationOutcome resource.
   * @param status - The HTTP status code from the response.
   * @param context - Optional context string for error messages (e.g., "Import kick-off").
   */
  constructor(
    operationOutcome: OperationOutcome,
    status: number,
    context?: string,
  ) {
    const prefix = context ? `${context} failed` : "Request failed";
    const diagnostics =
      operationOutcome.issue?.[0]?.diagnostics ?? "Unknown error";
    super(`${prefix}: ${status} - ${diagnostics}`);
    this.name = "OperationOutcomeError";
    this.operationOutcome = operationOutcome;
    this.status = status;
  }
}
