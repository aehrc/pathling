package au.csiro.pathling.fhirpath.definition;

import jakarta.annotation.Nonnull;
import java.util.Optional;


/**
 * Encapsulates the FHIR definitions for a child.
 *
 * @author Piotr Szul
 */
public interface ChildDefinition extends NodeDefinition {
  
  /**
   * @return the name of this child
   */
  @Nonnull
  String getName();

  /**
   * @return The maximum cardinality for this child
   */
  @Nonnull
  Optional<Integer> getMaxCardinality();

}
