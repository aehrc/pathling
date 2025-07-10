package au.csiro.pathling.errors;

import java.io.Serial;

/**
 * Thrown when the request references a FHIRPath feature that is not supported. This is specifically
 * reserved for things that are defined within FHIRPath but not supported by Pathling.
 *
 * @author John Grimes
 */
public class UnsupportedFhirPathFeatureError extends InvalidUserInputError {

  @Serial
  private static final long serialVersionUID = 3463869194525010650L;

  public UnsupportedFhirPathFeatureError(final String message) {
    super(message);
  }

}
