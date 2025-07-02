package au.csiro.pathling.errors;

/**
 * Thrown when the request references a FHIRPath feature that is not supported. This is specifically
 * reserved for things that are defined within FHIRPath but not supported by Pathling.
 *
 * @author John Grimes
 */
public class UnsupportedFhirPathFeatureError extends InvalidUserInputError {

  public UnsupportedFhirPathFeatureError(final String message) {
    super(message);
  }

  public UnsupportedFhirPathFeatureError(final String message, final Throwable cause) {
    super(message, cause);
  }

  public UnsupportedFhirPathFeatureError(final Throwable cause) {
    super(cause);
  }

}
