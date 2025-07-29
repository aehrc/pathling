package au.csiro.pathling.fhirpath.definition;

import jakarta.annotation.Nonnull;

/**
 * Represents a definition of a FHIR-like resource.
 */
public interface ResourceDefinition extends NodeDefinition {

  /**
   * Gets the resource tag for this resource definition.
   *
   * @return the resource tag
   */
  @Nonnull
  ResourceTag getResourceTag();

  /**
   * Gets the resource code for this resource definition.
   *
   * @return the resource code as a string
   */
  @Nonnull
  default String getResourceCode() {
    return getResourceTag().toCode();
  }
}
