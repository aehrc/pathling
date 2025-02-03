package au.csiro.pathling.fhirpath.definition;

import jakarta.annotation.Nonnull;

public interface DefinitionContext {

  /**
   * Finds the definition of a resource type.
   *
   * @param resourceType the type of the resource to find
   * @return the definition of the resource type Throws IllegalArgumentException if the resource
   * type is not found.
   */
  @Nonnull
  ResourceDefinition findResourceDefinition(@Nonnull final String resourceType);

}
