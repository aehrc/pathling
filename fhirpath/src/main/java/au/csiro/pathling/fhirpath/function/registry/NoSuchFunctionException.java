package au.csiro.pathling.fhirpath.function.registry;

import java.io.Serial;

/**
 * Thrown when a function is requested which is not present in the registry.
 */
public class NoSuchFunctionException extends Exception {

  @Serial
  private static final long serialVersionUID = 1L;

  /**
   * @param message The message to include in the exception
   */
  public NoSuchFunctionException(final String message) {
    super(message);
  }
 
}
