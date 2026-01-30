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
import au.csiro.pathling.fhirpath.column.ResourceRepresentation;
import au.csiro.pathling.fhirpath.definition.DefinitionContext;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import lombok.Value;

/**
 * A {@link ResourceResolver} implementation that works with custom definitions.
 *
 * <p>This resolver is designed for scenarios where resources are defined through a {@link
 * DefinitionContext} rather than FHIR's standard resource definitions. It's particularly useful for
 * testing FHIRPath expressions on arbitrary data structures defined in YAML test specifications.
 *
 * <p>Key characteristics:
 *
 * <ul>
 *   <li>Uses a {@link DefinitionContext} to look up resource definitions
 *   <li>The subject resource is identified by a resource code string
 *   <li>Uses {@link ResourceRepresentation#alwaysPresent()} for flat schema support
 *   <li>Only resolves the subject resource; other resources return empty Optional
 * </ul>
 *
 * @see ResourceResolver
 * @see DefinitionContext
 */
@Value(staticConstructor = "of")
public class DefinitionResourceResolver implements ResourceResolver {

  /** The resource code identifying the subject resource (e.g., "Patient", "Test"). */
  @Nonnull String subjectResourceCode;

  /** The definition context providing resource definitions. */
  @Nonnull DefinitionContext definitionContext;

  /**
   * {@inheritDoc}
   *
   * <p>Returns the subject resource code. Supports both standard FHIR resource types (e.g.,
   * "Patient") and custom types defined in the definition context.
   */
  @Override
  @Nonnull
  public String getSubjectResourceCode() {
    return subjectResourceCode;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Creates a ResourceCollection for the subject resource using flat schema representation. Uses
   * {@link ResourceRepresentation#alwaysPresent()} because in single-resource evaluation, each row
   * represents exactly one resource.
   */
  @Override
  @Nonnull
  public ResourceCollection resolveSubjectResource() {
    return ResourceCollection.build(
        ResourceRepresentation.alwaysPresent(),
        definitionContext.findResourceDefinition(subjectResourceCode));
  }

  /**
   * {@inheritDoc}
   *
   * <p>Only resolves the subject resource. Cross-resource references return empty Optional, which
   * will cause the FHIRPath evaluation to fall back to field traversal.
   */
  @Override
  @Nonnull
  public Optional<ResourceCollection> resolveResource(@Nonnull final String resourceCode) {
    if (subjectResourceCode.equals(resourceCode)) {
      return Optional.of(resolveSubjectResource());
    }
    return Optional.empty();
  }
}
