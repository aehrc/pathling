package au.csiro.pathling.fhirpath.definition;

import jakarta.annotation.Nonnull;

/**
 * Represents a definition of a FHIR-like resource.
 */
public interface ResourceDefinition extends NodeDefinition {

  @Nonnull
  ResourceTag getResourceTag();

  @Nonnull
  default String getResourceCode() {
    return getResourceTag().toCode();
  }
}
