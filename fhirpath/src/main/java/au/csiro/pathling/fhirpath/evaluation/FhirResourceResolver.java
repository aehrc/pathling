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

package au.csiro.pathling.fhirpath.evaluation;

import au.csiro.pathling.fhirpath.collection.ResourceCollection;
import au.csiro.pathling.fhirpath.column.EmptyRepresentation;
import au.csiro.pathling.fhirpath.column.ResourceRepresentation;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import org.hl7.fhir.exceptions.FHIRException;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * A {@link ResourceResolver} implementation for standard FHIR resources.
 * <p>
 * This resolver provides Column references for FHIRPath evaluation using flat schema
 * representation, where columns are accessed directly (e.g., {@code col("name")}).
 * <p>
 * Cross-resource references are handled according to the configured {@link CrossResourceStrategy}:
 * <ul>
 *   <li>{@link CrossResourceStrategy#FAIL} - throws an exception for cross-resource references</li>
 *   <li>{@link CrossResourceStrategy#EMPTY} - returns an empty ResourceCollection with correct
 *       type information but {@code lit(null)} column value</li>
 * </ul>
 *
 * @param subjectResource the subject resource type for this resolver
 * @param fhirContext the FHIR context for resource definitions
 * @param crossResourceStrategy the strategy for handling cross-resource references
 * @see ResourceResolver
 * @see CrossResourceStrategy
 */
public record FhirResourceResolver(
    @Nonnull ResourceType subjectResource,
    @Nonnull FhirContext fhirContext,
    @Nonnull CrossResourceStrategy crossResourceStrategy
) implements ResourceResolver {

  @Override
  @Nonnull
  public ResourceType getSubjectResource() {
    return subjectResource;
  }

  @Override
  @Nonnull
  public ResourceCollection resolveSubjectResource() {
    return createResourceCollection(subjectResource);
  }

  @Override
  @Nonnull
  public Optional<ResourceCollection> resolveResource(@Nonnull final String resourceCode) {
    // If it's the subject resource, return it directly
    if (resourceCode.equals(subjectResource.toCode())) {
      return Optional.of(resolveSubjectResource());
    }

    // Check if this is a valid FHIR resource type
    final Optional<ResourceType> resourceType = parseResourceTypeSafe(resourceCode);
    if (resourceType.isEmpty()) {
      // Not a valid FHIR resource type - this indicates the expression refers to
      // a field in the subject resource (e.g., "CustomField"), not a different resource
      return Optional.empty();
    }

    // Valid FHIR resource type but different from subject - apply cross-resource strategy
    return switch (crossResourceStrategy) {
      case FAIL -> throw new UnsupportedOperationException(
          "Cross-resource reference to '" + resourceCode + "' is not supported in single-resource "
              + "evaluation. Subject resource is: " + subjectResource.toCode());
      case EMPTY -> Optional.of(createEmptyResourceCollection(resourceType.get()));
    };
  }

  /**
   * Creates a ResourceCollection using flat schema representation.
   * <p>
   * Uses {@link ResourceRepresentation#alwaysPresent()} because in single-resource evaluation,
   * each row represents exactly one resource. This ensures field access works correctly even
   * for resources that don't have an {@code id} element defined.
   *
   * @param resourceType the resource type
   * @return a new ResourceCollection
   */
  @Nonnull
  private ResourceCollection createResourceCollection(@Nonnull final ResourceType resourceType) {
    return ResourceCollection.build(
        ResourceRepresentation.alwaysPresent(),
        fhirContext,
        resourceType);
  }

  /**
   * Creates an empty ResourceCollection with correct type information but {@code lit(null)} column.
   * <p>
   * This is used for cross-resource references with the EMPTY strategy. The collection has the
   * correct type information (allowing type checking to work correctly) but its column value
   * is null, causing any traversals or operations to return null/empty results.
   * <p>
   * Uses {@link EmptyRepresentation} which handles all traversal operations gracefully without
   * attempting to access Spark columns that would cause errors.
   *
   * @param resourceType the resource type
   * @return a ResourceCollection with correct type but null column
   */
  @Nonnull
  private ResourceCollection createEmptyResourceCollection(@Nonnull final ResourceType resourceType) {
    return ResourceCollection.build(
        EmptyRepresentation.getInstance(),
        fhirContext,
        resourceType);
  }

  /**
   * Parses a resource type code to a ResourceType enum value, returning empty if invalid.
   * <p>
   * This is used to distinguish between:
   * <ul>
   *   <li>Valid FHIR resource types (e.g., "Patient", "Encounter") - returns the ResourceType</li>
   *   <li>Invalid codes that represent fields (e.g., "CustomField") - returns empty</li>
   * </ul>
   *
   * @param resourceCode the resource type code
   * @return Optional containing the ResourceType if valid, empty otherwise
   */
  @Nonnull
  private static Optional<ResourceType> parseResourceTypeSafe(@Nonnull final String resourceCode) {
    try {
      return Optional.of(ResourceType.fromCode(resourceCode));
    } catch (final FHIRException e) {
      return Optional.empty();
    }
  }
}
