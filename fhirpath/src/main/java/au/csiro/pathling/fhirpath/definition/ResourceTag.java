package au.csiro.pathling.fhirpath.definition;

import jakarta.annotation.Nonnull;

/**
 * Represents a resource type for FHIR-like schema definitions.
 */
public interface ResourceTag {

  /**
   * Returns the string code representation of this resource tag.
   *
   * @return the resource code as a string
   */
  @Nonnull
  String toCode();

}
