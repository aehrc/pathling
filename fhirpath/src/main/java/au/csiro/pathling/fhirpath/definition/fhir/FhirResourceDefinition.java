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
import lombok.Getter;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Encapsulates the FHIR definitions for a resource.
 *
 * @author John Grimes
 */
@Getter
class FhirResourceDefinition extends
    BaseFhirNodeDefinition<RuntimeResourceDefinition> implements
    ResourceDefinition {

  /**
   * The HAPI FHIR resource type.
   */
  @Nonnull
  private final ResourceType resourceType;

  @Override
  @Nonnull
  public FhirResourceTag getResourceTag() {
    return FhirResourceTag.of(resourceType);
  }

  /**
   * @param resourceType The {@link ResourceType} that describes this resource
   * @param definition The HAPI {@link RuntimeResourceDefinition} for this resource
   */
  public FhirResourceDefinition(@Nonnull final ResourceType resourceType,
      @Nonnull final RuntimeResourceDefinition definition) {
    super(definition);
    this.resourceType = resourceType;
  }
}
