package au.csiro.pathling.fhirpath.function.resolver;

import java.io.Serial;

/**
 * Exception thrown when there is an error during the invocation of a function.
 *
 * @author John Grimes
 */
public class FunctionInvocationError extends RuntimeException {

  @Serial
  private static final long serialVersionUID = -3228718152389969726L;

  /**
   * Creates a new FunctionInvocationError.
   *
   * @param message the error message
   * @param cause the underlying cause
   */
  public FunctionInvocationError(final String message, final Throwable cause) {
    super(message, cause);
  }
 
}
