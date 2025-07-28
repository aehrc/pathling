package au.csiro.pathling.io;

import java.io.Serial;

/**
 * Represents an error that occurs during data persistence operations.
 *
 * @author John Grimes
 */
public class PersistenceError extends RuntimeException {

  @Serial
  private static final long serialVersionUID = -757932366975899363L;

  public PersistenceError(final String message, final Throwable cause) {
    super(message, cause);
  }
}
