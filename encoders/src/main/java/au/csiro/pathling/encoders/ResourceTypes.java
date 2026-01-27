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
 *
 * @author John Grimes
 */

package au.csiro.pathling.encoders;

import jakarta.annotation.Nonnull;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.hl7.fhir.exceptions.FHIRException;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Centralised utility class for resource type validation and management. This class provides a
 * single source of truth for:
 *
 * <ul>
 *   <li>Custom resource types that are not part of the standard FHIR specification but are
 *       supported by Pathling (e.g., ViewDefinition from SQL on FHIR)
 *   <li>Unsupported resource types that cannot be encoded due to their complex structure
 *   <li>Validation methods for checking resource type support
 * </ul>
 */
public final class ResourceTypes {

  /** The resource type code for ViewDefinition resources from the SQL on FHIR specification. */
  public static final String VIEW_DEFINITION = "ViewDefinition";

  /**
   * Custom resource types that are not part of the standard FHIR specification but are supported by
   * Pathling. These are resource types defined in other specifications (e.g., SQL on FHIR) that
   * Pathling can process.
   */
  public static final Set<String> CUSTOM_RESOURCE_TYPES = Set.of(VIEW_DEFINITION);

  /**
   * Mapping from custom resource type names to their implementing classes. This is used to register
   * custom types with the FhirContext so that HAPI can recognise and parse them.
   */
  public static final Map<String, Class<? extends IBaseResource>> CUSTOM_RESOURCE_TYPE_CLASSES =
      Map.of(VIEW_DEFINITION, ViewDefinitionResource.class);

  /**
   * Resource types that are not supported for encoding. These resources have complex structures
   * that cannot be represented in the Spark schema format used by Pathling encoders.
   */
  public static final Set<String> UNSUPPORTED_RESOURCES =
      Set.of("Parameters", "StructureDefinition", "StructureMap", "Bundle");

  private ResourceTypes() {
    // Utility class, prevent instantiation.
  }

  /**
   * Checks if the given resource type code represents a custom resource type that is not part of
   * the standard FHIR specification but is supported by Pathling.
   *
   * @param code the resource type code to check
   * @return true if the code represents a supported custom resource type, false otherwise
   */
  public static boolean isCustomResourceType(@Nonnull final String code) {
    return CUSTOM_RESOURCE_TYPES.contains(code);
  }

  /**
   * Checks if the given resource type code is supported by Pathling. A resource type is supported
   * if it is either:
   *
   * <ul>
   *   <li>A custom resource type (e.g., ViewDefinition)
   *   <li>A standard FHIR resource type that is not in the unsupported list
   * </ul>
   *
   * @param code the resource type code to check
   * @return true if the resource type is supported, false otherwise
   */
  public static boolean isSupported(@Nonnull final String code) {
    // Custom resource types are always supported.
    if (isCustomResourceType(code)) {
      return true;
    }

    // Unsupported resources are not supported.
    if (UNSUPPORTED_RESOURCES.contains(code)) {
      return false;
    }

    // Check if it's a valid standard FHIR resource type.
    try {
      final ResourceType resourceType = ResourceType.fromCode(code);
      return resourceType != null;
    } catch (final FHIRException e) {
      return false;
    }
  }

  /**
   * Matches the given string against supported resource types in a case-insensitive fashion and
   * returns the canonical form of the resource type if found.
   *
   * @param resourceTypeString the string to match against resource types
   * @return an Optional containing the canonical resource type code if the string matches a
   *     supported resource type, empty otherwise
   */
  @Nonnull
  public static Optional<String> matchSupportedResourceType(
      @Nonnull final String resourceTypeString) {
    // Check custom resource types first (case-insensitive).
    for (final String customType : CUSTOM_RESOURCE_TYPES) {
      if (customType.equalsIgnoreCase(resourceTypeString)) {
        return Optional.of(customType);
      }
    }

    // Check if it's an unsupported type (exact match for unsupported list).
    if (UNSUPPORTED_RESOURCES.contains(resourceTypeString)) {
      return Optional.empty();
    }

    // Try exact match for standard FHIR types.
    try {
      final ResourceType exactMatch = ResourceType.fromCode(resourceTypeString);
      if (exactMatch != null && !UNSUPPORTED_RESOURCES.contains(exactMatch.toCode())) {
        return Optional.of(exactMatch.toCode());
      }
    } catch (final FHIRException ignored) {
      // Continue to case-insensitive search.
    }

    // Try case-insensitive match for standard FHIR types.
    for (final ResourceType resourceType : ResourceType.values()) {
      if (resourceTypeString.equalsIgnoreCase(resourceType.toCode())
          && !UNSUPPORTED_RESOURCES.contains(resourceType.toCode())) {
        return Optional.of(resourceType.toCode());
      }
    }

    return Optional.empty();
  }
}
