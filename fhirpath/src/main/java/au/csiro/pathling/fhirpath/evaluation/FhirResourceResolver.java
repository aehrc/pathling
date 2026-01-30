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

/**
 * A {@link ResourceResolver} implementation for standard FHIR resources.
 *
 * <p>This resolver provides Column references for FHIRPath evaluation using flat schema
 * representation, where columns are accessed directly (e.g., {@code col("name")}).
 *
 * <p>Cross-resource references are handled according to the configured {@link
 * CrossResourceStrategy}:
 *
 * <ul>
 *   <li>{@link CrossResourceStrategy#FAIL} - throws an exception for cross-resource references
 *   <li>{@link CrossResourceStrategy#EMPTY} - returns an empty ResourceCollection with correct type
 *       information but {@code lit(null)} column value
 * </ul>
 *
 * @param subjectResourceCode the subject resource type code for this resolver (e.g., "Patient",
 *     "ViewDefinition")
 * @param fhirContext the FHIR context for resource definitions
 * @param crossResourceStrategy the strategy for handling cross-resource references
 * @see ResourceResolver
 * @see CrossResourceStrategy
 */
public record FhirResourceResolver(
    @Nonnull String subjectResourceCode,
    @Nonnull FhirContext fhirContext,
    @Nonnull CrossResourceStrategy crossResourceStrategy)
    implements ResourceResolver {

  @Override
  @Nonnull
  public String getSubjectResourceCode() {
    return subjectResourceCode;
  }

  @Override
  @Nonnull
  public ResourceCollection resolveSubjectResource() {
    return createResourceCollection(subjectResourceCode);
  }

  @Override
  @Nonnull
  public Optional<ResourceCollection> resolveResource(@Nonnull final String resourceCode) {
    // If it's the subject resource, return it directly
    if (resourceCode.equals(subjectResourceCode)) {
      return Optional.of(resolveSubjectResource());
    }

    // Check if this is a valid FHIR resource type by attempting to look it up in the FhirContext.
    // This handles both standard FHIR types and custom registered types.
    if (!isKnownResourceType(resourceCode)) {
      // Not a known resource type - this indicates the expression refers to
      // a field in the subject resource (e.g., "CustomField"), not a different resource
      return Optional.empty();
    }

    // Known resource type but different from subject - apply cross-resource strategy
    return switch (crossResourceStrategy) {
      case FAIL ->
          throw new UnsupportedOperationException(
              "Cross-resource reference to '"
                  + resourceCode
                  + "' is not supported in single-resource "
                  + "evaluation. Subject resource is: "
                  + subjectResourceCode);
      case EMPTY -> Optional.of(createEmptyResourceCollection(resourceCode));
    };
  }

  /**
   * Creates a ResourceCollection using flat schema representation.
   *
   * <p>Uses {@link ResourceRepresentation#alwaysPresent()} because in single-resource evaluation,
   * each row represents exactly one resource. This ensures field access works correctly even for
   * resources that don't have an {@code id} element defined.
   *
   * @param resourceCode the resource type code
   * @return a new ResourceCollection
   */
  @Nonnull
  private ResourceCollection createResourceCollection(@Nonnull final String resourceCode) {
    return ResourceCollection.build(
        ResourceRepresentation.alwaysPresent(), fhirContext, resourceCode);
  }

  /**
   * Creates an empty ResourceCollection with correct type information but {@code lit(null)} column.
   *
   * <p>This is used for cross-resource references with the EMPTY strategy. The collection has the
   * correct type information (allowing type checking to work correctly) but its column value is
   * null, causing any traversals or operations to return null/empty results.
   *
   * <p>Uses {@link EmptyRepresentation} which handles all traversal operations gracefully without
   * attempting to access Spark columns that would cause errors.
   *
   * @param resourceCode the resource type code
   * @return a ResourceCollection with correct type but null column
   */
  @Nonnull
  private ResourceCollection createEmptyResourceCollection(@Nonnull final String resourceCode) {
    return ResourceCollection.build(EmptyRepresentation.getInstance(), fhirContext, resourceCode);
  }

  /**
   * Checks if a resource code represents a known resource type in the FHIR context.
   *
   * <p>This method attempts to look up the resource definition in the FHIR context, which handles
   * both standard FHIR types (e.g., "Patient", "Observation") and custom registered types (e.g.,
   * "ViewDefinition").
   *
   * @param resourceCode the resource type code
   * @return true if the resource type is known, false otherwise
   */
  private boolean isKnownResourceType(@Nonnull final String resourceCode) {
    try {
      fhirContext.getResourceDefinition(resourceCode);
      return true;
    } catch (final Exception e) {
      return false;
    }
  }
}
