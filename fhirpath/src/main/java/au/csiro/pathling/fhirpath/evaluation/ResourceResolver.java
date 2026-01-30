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
import jakarta.annotation.Nonnull;
import java.util.Optional;

/**
 * Interface for resolving FHIR resources during single-resource FHIRPath evaluation.
 *
 * <p>This interface is designed for scenarios where only column expressions are needed without
 * access to actual dataset views. It does not require the {@code createView()} method, making it
 * suitable for:
 *
 * <ul>
 *   <li>Building filter expressions where only Column references are needed
 *   <li>Single-resource evaluation without DataSource dependencies
 *   <li>Search column building and similar use cases
 * </ul>
 *
 * @see SingleResourceEvaluator
 */
public interface ResourceResolver {

  /**
   * Returns the subject resource code for this resolver.
   *
   * <p>The subject resource is the primary resource type being queried, such as "Patient" in a
   * query starting with {@code Patient.name.given}. This method supports both standard FHIR
   * resource types (e.g., "Patient", "Observation") and custom resource types (e.g.,
   * "ViewDefinition").
   *
   * @return the subject resource type code
   */
  @Nonnull
  String getSubjectResourceCode();

  /**
   * Resolves the subject resource for this context.
   *
   * <p>The subject resource is the primary resource type being queried and evaluated against.
   *
   * @return a ResourceCollection representing the subject resource
   */
  @Nonnull
  ResourceCollection resolveSubjectResource();

  /**
   * Resolves a resource by its type code.
   *
   * <p>This method returns an Optional containing a ResourceCollection. The behavior depends on the
   * implementation:
   *
   * <ul>
   *   <li>For the subject resource, returns its ResourceCollection
   *   <li>For foreign resources with EMPTY strategy, returns a ResourceCollection with correct type
   *       information but {@code lit(null)} column value
   *   <li>For foreign resources with FAIL strategy, throws an exception
   *   <li>Returns {@code Optional.empty()} only if the resource type is truly unknown
   * </ul>
   *
   * <p><strong>Important:</strong> Returning {@code Optional.empty()} will cause the FHIRPath
   * evaluation to fall back to field traversal, which may not be the desired behavior for known
   * resource types. Implementations should return an empty ResourceCollection (with correct type
   * but null column) rather than {@code Optional.empty()} for known but unavailable resources.
   *
   * @param resourceCode the FHIR resource type code (e.g., "Patient", "Observation")
   * @return an Optional containing the ResourceCollection if available
   * @throws UnsupportedOperationException if cross-resource references are not supported and the
   *     FAIL strategy is in use
   */
  @Nonnull
  Optional<ResourceCollection> resolveResource(@Nonnull String resourceCode);
}
