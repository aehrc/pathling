/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package au.csiro.pathling.fhirpath.definition;

import ca.uhn.fhir.context.RuntimeResourceDefinition;
import javax.annotation.Nonnull;
import lombok.Getter;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.ListResource;

/**
 * Encapsulates the FHIR definitions for a resource.
 *
 * @author John Grimes
 */
@Getter
public class ResourceDefinition extends BaseNodeDefinition<RuntimeResourceDefinition> {

  /**
   * The HAPI FHIR resource type.
   */
  @Nonnull
  private final ResourceType resourceType;

  /**
   * @param resourceType The {@link ResourceType} that describes this resource
   * @param definition The HAPI {@link RuntimeResourceDefinition} for this resource
   */
  public ResourceDefinition(@Nonnull final ResourceType resourceType,
      @Nonnull final RuntimeResourceDefinition definition) {
    super(definition);
    this.resourceType = resourceType;
  }

  /**
   * Returns the {@link ResourceType} for a HAPI resource class.
   *
   * @param resourceClass the resource class, extending {@link IBaseResource}
   * @return a {@link ResourceType} value
   * @throws IllegalArgumentException if the resource type was not found
   */
  @Nonnull
  public static ResourceType getResourceTypeFromClass(
      @Nonnull final Class<? extends IBaseResource> resourceClass) {
    // List is the only resource for which the HAPI resource class name does not match the resource
    // code.
    final String resourceName = resourceClass == ListResource.class
                                ? "List"
                                : resourceClass.getSimpleName();
    return ResourceType.fromCode(resourceName);
  }

}