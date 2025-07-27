package au.csiro.pathling.fhirpath.definition;

import jakarta.annotation.Nonnull;
import java.util.Optional;

/**
 * Base class for FHIR-like schema definitions. The schema is capable or representing a subset all
 * FHIR schema concepts such as elements, choices, and resources.
 */
public interface NodeDefinition {

  /**
   * Returns the child element of this definition with the specified name.
   *
   * @param name the name of the child element
   * @return a new {@link NodeDefinition} describing the child
   */
  @Nonnull
  Optional<ChildDefinition> getChildElement(@Nonnull String name);

}
