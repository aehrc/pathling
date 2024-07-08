package au.csiro.pathling.fhirpath.variable;

import au.csiro.pathling.fhirpath.collection.Collection;
import jakarta.annotation.Nonnull;
import java.util.Map;
import java.util.Optional;
import lombok.Value;

/**
 * A resolver that is based upon a supplied map of variables and their values.
 *
 * @author John Grimes
 */
@Value
public class SuppliedVariableResolver implements EnvironmentVariableResolver {

  @Nonnull
  Map<String, Collection> variables;

  @Override
  public Optional<Collection> get(@Nonnull final String name) {
    return Optional.ofNullable(variables.get(name));
  }

}
