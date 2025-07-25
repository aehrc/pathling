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

package au.csiro.pathling.fhirpath.execution;

import au.csiro.pathling.fhirpath.collection.ResourceCollection;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import au.csiro.pathling.fhirpath.context.ResourceResolver;
import au.csiro.pathling.fhirpath.definition.DefinitionContext;
import au.csiro.pathling.fhirpath.definition.ResourceTag;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import lombok.Value;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * A specialized implementation of {@link ResourceResolver} that works with definition-based
 * resources rather than FHIR resources from a data source.
 * <p>
 * This resolver is designed for scenarios where:
 * <ul>
 *   <li>Resources are defined through a {@link DefinitionContext} rather than loaded from a data source</li>
 *   <li>The subject resource is identified by a {@link ResourceTag} rather than a {@link ResourceType}</li>
 *   <li>The dataset structure is provided directly rather than being created from a data source</li>
 * </ul>
 * <p>
 * This implementation is particularly useful for testing, validation, and working with
 * custom resource definitions that may not directly correspond to standard FHIR resources.
 */
@Value(staticConstructor = "of")
public class DefResourceResolver implements ResourceResolver {

  /**
   * The resource tag identifying the subject resource.
   * <p>
   * Unlike other resolvers that use {@link ResourceType}, this resolver uses a {@link ResourceTag}
   * which provides more flexibility in identifying resources in definition-based contexts.
   */
  @Nonnull
  ResourceTag subjectResource;

  /**
   * The definition context providing resource definitions.
   * <p>
   * This context contains the definitions for resources that can be resolved, including their
   * structure, elements, and relationships.
   */
  @Nonnull
  DefinitionContext definitionContext;

  /**
   * The dataset containing the subject resource data.
   * <p>
   * This dataset is provided directly rather than being created from a data source, allowing for
   * more flexibility in how the data is structured and sourced.
   */
  @Nonnull
  Dataset<Row> subjectDataset;

  /**
   * {@inheritDoc}
   * <p>
   * This implementation only supports resolving the subject resource. If the requested resource
   * code matches the subject resource code, it returns the subject resource. Otherwise, it returns
   * an empty Optional.
   */
  @Override
  public @Nonnull Optional<ResourceCollection> resolveResource(
      @Nonnull final String resourceCode) {
    if (subjectResource.toCode().equals(resourceCode)) {
      return Optional.of(resolveSubjectResource());
    } else {
      return Optional.empty();
    }
  }

  /**
   * {@inheritDoc}
   * <p>
   * This implementation creates a resource collection for the subject resource using the
   * {@link #createResource} method.
   */
  @Override
  @Nonnull
  public ResourceCollection resolveSubjectResource() {
    return createResource(getSubjectResource());
  }

  /**
   * Creates a ResourceCollection for the specified resource tag.
   * <p>
   * This method creates a column representation for the resource and builds a ResourceCollection
   * using the resource definition from the definition context. The column representation uses the
   * resource tag code as the column name.
   *
   * @param resourceType The resource tag to create a collection for
   * @return A ResourceCollection for the specified resource tag
   */
  @Nonnull
  ResourceCollection createResource(@Nonnull final ResourceTag resourceType) {
    return ResourceCollection.build(
        new DefaultRepresentation(functions.col(resourceType.toCode())),
        definitionContext.findResourceDefinition(resourceType.toCode()));
  }

  /**
   * {@inheritDoc}
   * <p>
   * This implementation creates a view from the subject dataset in the standardized structure.
   */
  @Nonnull
  public Dataset<Row> createView() {
    return BaseResourceResolver.toResourceRepresentation(subjectResource.toCode(), subjectDataset);
  }

}
