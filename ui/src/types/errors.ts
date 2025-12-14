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
  constructor(message: string = "Session expired or unauthorized") {
    super(message);
    this.name = "UnauthorizedError";
  }
}
