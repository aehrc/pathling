package au.csiro.pathling.fhirpath.operator;

import java.io.Serial;

/**
 * Represents an error that occurs during the invocation of a method-defined operator.
 *
 * @author John Grimes
 */
public class MethodInvocationError extends RuntimeException {

  @Serial
  private static final long serialVersionUID = -1161837506294342552L;

  public MethodInvocationError(final String message, final Throwable cause) {
    super(message, cause);
  }
}
