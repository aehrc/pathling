package au.csiro.pathling.views;

import java.io.Serial;

/**
 * Exception thrown when an error occurs during the construction of a constant in a view.
 *
 * @author John Grimes
 */
public class ConstantConstructionException extends RuntimeException {

  @Serial
  private static final long serialVersionUID = -6668286802657345673L;

  /**
   * Creates a new ConstantConstructionException.
   *
   * @param cause the underlying cause
   */
  public ConstantConstructionException(final Throwable cause) {
    super(cause);
  }
}
