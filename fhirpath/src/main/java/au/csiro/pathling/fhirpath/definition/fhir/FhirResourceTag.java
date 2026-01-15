/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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

import au.csiro.pathling.fhirpath.definition.ResourceTag;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import lombok.Value;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Represents a FHIR resource tag. Supports both standard FHIR resource types (which have a
 * corresponding {@link ResourceType} enum value) and custom resource types (like ViewDefinition)
 * that are not part of the standard FHIR specification.
 */
@Value
public class FhirResourceTag implements ResourceTag {

  /** The resource code (e.g., "Patient", "Observation", "ViewDefinition"). */
  @Nonnull String resourceCode;

  /**
   * The FHIR resource type enum, if this is a standard FHIR resource type. Empty for custom
   * resource types like ViewDefinition.
   */
  @Nonnull Optional<ResourceType> resourceType;

  /**
   * Creates a FhirResourceTag from a standard FHIR ResourceType.
   *
   * @param resourceType the FHIR resource type
   * @return a new FhirResourceTag
   */
  @Nonnull
  public static FhirResourceTag of(@Nonnull final ResourceType resourceType) {
    return new FhirResourceTag(resourceType.toCode(), Optional.of(resourceType));
  }

  /**
   * Creates a FhirResourceTag from a resource code string. This method attempts to resolve the code
   * to a standard ResourceType, but allows custom types that are not in the FHIR specification.
   *
   * @param resourceCode the resource code (e.g., "Patient", "ViewDefinition")
   * @return a new FhirResourceTag
   */
  @Nonnull
  public static FhirResourceTag of(@Nonnull final String resourceCode) {
    try {
      final ResourceType resourceType = ResourceType.fromCode(resourceCode);
      return new FhirResourceTag(resourceCode, Optional.of(resourceType));
    } catch (final org.hl7.fhir.exceptions.FHIRException e) {
      // Custom resource type not in the standard FHIR specification.
      return new FhirResourceTag(resourceCode, Optional.empty());
    }
  }

  /**
   * Creates a FhirResourceTag from a resource code and optional ResourceType.
   *
   * @param resourceCode the resource code
   * @param resourceType the optional resource type
   * @return a new FhirResourceTag
   */
  @Nonnull
  public static FhirResourceTag of(
      @Nonnull final String resourceCode, @Nonnull final Optional<ResourceType> resourceType) {
    return new FhirResourceTag(resourceCode, resourceType);
  }

  @Override
  @Nonnull
  public String toString() {
    return resourceCode;
  }

  @Override
  @Nonnull
  public String toCode() {
    return resourceCode;
  }
}
