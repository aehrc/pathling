package au.csiro.pathling.fhirpath.variable;

import au.csiro.pathling.errors.UnsupportedFhirPathFeatureError;
import au.csiro.pathling.fhirpath.collection.Collection;
import jakarta.annotation.Nonnull;
import java.util.Optional;

/**
 * This resolver throws errors for known unsupported environment variables in FHIRPath.
 *
 * @author John Grimes
 */
public class UnsupportedVariableResolver implements EnvironmentVariableResolver {

  @Override
  public Optional<Collection> get(@Nonnull final String name) {
    return switch (name) {
      case "factory" -> throw new UnsupportedFhirPathFeatureError("Type factory is not supported");
      case "terminologies" ->
          throw new UnsupportedOperationException("Terminology service is not supported");
      case "server" ->
          throw new UnsupportedFhirPathFeatureError("General Service API is not supported");
      default -> Optional.empty();
    };
  }

}
