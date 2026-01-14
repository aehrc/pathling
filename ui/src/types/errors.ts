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
   *
   * @param message
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
   *
   * @param message
   */
  constructor(message: string = "Resource not found") {
    super(message);
    this.name = "NotFoundError";
  }
}
