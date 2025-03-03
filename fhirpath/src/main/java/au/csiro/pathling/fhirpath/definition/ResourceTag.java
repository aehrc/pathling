package au.csiro.pathling.fhirpath.definition;

import jakarta.annotation.Nonnull;

/**
 * Represents a resource type for FHIR-like schema definitions.
 */
public interface ResourceTag {

  @Nonnull
  String toCode();

}
