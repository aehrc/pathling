package au.csiro.pathling.fhirpath.variable;

import au.csiro.pathling.fhirpath.collection.Collection;
import jakarta.annotation.Nonnull;
import java.util.Optional;

/**
 * Something that can resolve a variable name into a substitute value within a FHIRPath expression.
 *
 * @author John Grimes
 */
public interface EnvironmentVariableResolver {

  /**
   * Get a variable by name.
   *
   * @param name The name of the variable
   * @return The variable, if it exists
   */
  Optional<Collection> get(@Nonnull final String name);

}
