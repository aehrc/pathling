/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.fhirpath.definition.fhir;

import au.csiro.pathling.fhirpath.definition.ResourceDefinition;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import lombok.Getter;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Encapsulates the FHIR definitions for a resource. Supports both standard FHIR resource types and
 * custom resource types (like ViewDefinition) that are registered with HAPI but not part of the
 * standard FHIR specification.
 *
 * @author John Grimes
 */
@Getter
class FhirResourceDefinition extends BaseFhirNodeDefinition<RuntimeResourceDefinition>
    implements ResourceDefinition {

  /** The resource tag containing the resource code and optional ResourceType enum. */
  @Nonnull private final FhirResourceTag resourceTag;

  @Override
  @Nonnull
  public FhirResourceTag getResourceTag() {
    return resourceTag;
  }

  /**
   * Creates a FhirResourceDefinition from a resource code and HAPI definition.
   *
   * @param resourceCode the resource code (e.g., "Patient", "ViewDefinition")
   * @param resourceType the optional ResourceType enum (empty for custom types)
   * @param definition the HAPI RuntimeResourceDefinition
   */
  public FhirResourceDefinition(
      @Nonnull final String resourceCode,
      @Nonnull final Optional<ResourceType> resourceType,
      @Nonnull final RuntimeResourceDefinition definition) {
    super(definition);
    this.resourceTag = FhirResourceTag.of(resourceCode, resourceType);
  }

  /**
   * Creates a FhirResourceDefinition from a standard FHIR ResourceType.
   *
   * @param resourceType the ResourceType that describes this resource
   * @param definition the HAPI RuntimeResourceDefinition for this resource
   */
  public FhirResourceDefinition(
      @Nonnull final ResourceType resourceType,
      @Nonnull final RuntimeResourceDefinition definition) {
    super(definition);
    this.resourceTag = FhirResourceTag.of(resourceType);
  }
}
