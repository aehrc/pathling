package au.csiro.pathling.fhirpath.definition;

import jakarta.annotation.Nonnull;
import java.util.Optional;


/**
 * Represents of a named child in a FHIR-like schema.
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
