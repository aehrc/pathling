package au.csiro.pathling.fhirpath.variable;

import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.ResourceCollection;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import lombok.Value;

/**
 * A resolver that provides access to the input context and resource.
 *
 * @author John Grimes
 * @see <a href="https://hl7.org/fhirpath/#environment-variables">FHIRPath environment variables</a>
 * @see <a href="https://hl7.org/fhir/R4/fhirpath.html#variables">FHIR-specific environment
 * variables</a>
 */
@Value
public class ContextVariableResolver implements EnvironmentVariableResolver {

  @Nonnull
  ResourceCollection resource;

  @Nonnull
  Collection inputContext;

  @Override
  public Optional<Collection> get(@Nonnull final String name) {
    if (name.equals("%context")) {
      return Optional.of(inputContext);
    } else if (name.equals("%resource") || name.equals("%rootResource")) {
      return Optional.of(resource);
    } else {
      return Optional.empty();
    }
  }

}
