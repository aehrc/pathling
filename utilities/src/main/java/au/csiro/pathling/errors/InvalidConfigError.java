package au.csiro.pathling.errors;


/**
 * Thrown when some condiguration settings are missing or invalid.
 *
 * @author Piotr Szul
 */
public class InvalidConfigError extends RuntimeException {

  private static final long serialVersionUID = 5953491164133388069L;

  /**
   * @param message The detailed message for the exception
   */
  public InvalidConfigError(final String message) {
    super(message);
  }
}
