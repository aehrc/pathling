/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath;

import au.csiro.pathling.fhirpath.element.ElementDefinition;
import ca.uhn.fhir.context.BaseRuntimeChildDefinition;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.Getter;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Encapsulates the FHIR definitions for a resource.
 *
 * @author John Grimes
 */
public class ResourceDefinition {

  /**
   * The HAPI FHIR resource type.
   */
  @Nonnull
  @Getter
  private final ResourceType resourceType;

  @Nonnull
  private final RuntimeResourceDefinition definition;

  /**
   * @param resourceType The {@link ResourceType} that describes this resource
   * @param definition The HAPI {@link RuntimeResourceDefinition} for this resource
   */
  public ResourceDefinition(@Nonnull final ResourceType resourceType,
      @Nonnull final RuntimeResourceDefinition definition) {
    this.resourceType = resourceType;
    this.definition = definition;
  }

  /**
   * Returns the child element of this resource with the specified name.
   *
   * @param name The name of the child element
   * @return A new ElementDefinition describing the child
   */
  @Nonnull
  public Optional<ElementDefinition> getChildElement(@Nonnull final String name) {
    final Optional<BaseRuntimeChildDefinition> childDefinition = Optional
        .ofNullable(definition.getChildByName(name));
    return childDefinition.map(definition -> ElementDefinition.build(definition, name));
  }

}
