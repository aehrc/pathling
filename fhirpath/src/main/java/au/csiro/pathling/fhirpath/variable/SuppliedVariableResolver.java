package au.csiro.pathling.fhirpath.variable;

import au.csiro.pathling.fhirpath.collection.Collection;
import jakarta.annotation.Nonnull;
import java.util.Map;
import java.util.Optional;

/**
 * A resolver that is based upon a supplied map of variables and their values.
 *
 * @param variables a map of variable names to collections of values
 * @author John Grimes
 */
public record SuppliedVariableResolver(@Nonnull Map<String, Collection> variables) implements
    EnvironmentVariableResolver {

  @Nonnull
  @Override
  public Optional<Collection> get(@Nonnull final String name) {
    return Optional.ofNullable(variables.get(name));
  }

}
