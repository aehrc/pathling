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
