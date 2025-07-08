package au.csiro.pathling.fhirpath.collection;

import java.io.Serial;

/**
 * Represents an error that occurs during the construction of a {@link Collection}.
 *
 * @author John Grimes
 */
public class CollectionConstructionError extends RuntimeException {

  @Serial
  private static final long serialVersionUID = -1560019566754143491L;

  public CollectionConstructionError(final String message, final Throwable cause) {
    super(message, cause);
  }

}
