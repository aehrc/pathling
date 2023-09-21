package au.csiro.pathling.fhirpath.definition;

import java.util.Optional;
import javax.annotation.Nonnull;

/**
 * Base abstractions for FHIR defintions.
 */
public interface NodeDefinition {

  /**
   * Returns the child element of this definition with the specified name.
   *
   * @param name the name of the child element
   * @return a new {@link NodeDefinition} describing the child
   */
  @Nonnull
  Optional<? extends ChildDefinition> getChildElement(@Nonnull String name);

}
