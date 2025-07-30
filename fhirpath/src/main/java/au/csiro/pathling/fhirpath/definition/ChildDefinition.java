package au.csiro.pathling.fhirpath.definition;

import jakarta.annotation.Nonnull;


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

}
