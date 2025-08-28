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

package au.csiro.pathling.fhirpath.execution;

import static au.csiro.pathling.utilities.Preconditions.checkArgument;

import au.csiro.pathling.fhirpath.collection.ResourceCollection;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import au.csiro.pathling.fhirpath.context.ResourceResolver;
import au.csiro.pathling.io.source.DataSource;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Base implementation of the {@link ResourceResolver} interface that provides common functionality
 * for resolving FHIR resources during FHIRPath evaluation.
 * <p>
 * This abstract class implements core resource resolution methods and provides a foundation for
 * more specialized resource resolvers. It handles:
 * <ul>
 *   <li>Basic resource resolution by type code</li>
 *   <li>Subject resource resolution</li>
 *   <li>Resource creation with appropriate column representations</li>
 * </ul>
 * <p>
 * Subclasses must implement:
 * <ul>
 *   <li>{@link #getSubjectResource()} - to define the primary resource type</li>
 *   <li>{@link #getFhirContext()} - to provide the FHIR context for resource definitions</li>
 * </ul>
 * <p>
 * By default, this implementation does not support joins or reverse joins, and subclasses
 * must override the relevant methods to provide that functionality.
 */
public abstract class BaseResourceResolver implements ResourceResolver {

  /**
   * Returns the subject resource type for this resolver.
   * <p>
   * The subject resource is the primary resource type being queried, such as Patient in a query
   * starting with Patient.name.given.
   *
   * @return The subject resource type
   */
  @Nonnull
  public abstract ResourceType getSubjectResource();

  /**
   * Returns the FHIR context used by this resolver.
   * <p>
   * The FHIR context provides access to resource definitions and other FHIR-specific information
   * needed for resource resolution.
   *
   * @return The FHIR context
   */
  @Nonnull
  public abstract FhirContext getFhirContext();

  /**
   * {@inheritDoc}
   * <p>
   * This implementation first checks if the requested resource code matches the subject resource.
   * If it does, it returns the subject resource. We don't support resolving resources other than
   * the subject resource in this implementation.
   */
  @Override
  public @Nonnull Optional<ResourceCollection> resolveResource(
      @Nonnull final String resourceCode) {
    checkArgument(resourceCode.equals(getSubjectResource().toCode()),
        "Resource code must match the subject resource code: " + getSubjectResource().toCode());
    return Optional.of(resolveSubjectResource());
  }

  /**
   * {@inheritDoc}
   * <p>
   * This implementation creates a resource collection for the subject resource type.
   */
  @Override
  @Nonnull
  public ResourceCollection resolveSubjectResource() {
    return createResource(getSubjectResource());
  }

  /**
   * Creates a ResourceCollection for the specified resource type.
   * <p>
   * This method creates a column representation for the resource and builds a ResourceCollection
   * using the FHIR context and resource type. The column representation uses the resource type code
   * as the column name.
   *
   * @param resourceType The resource type to create a collection for
   * @return A ResourceCollection for the specified resource type
   */
  @Nonnull
  protected ResourceCollection createResource(@Nonnull final ResourceType resourceType) {
    return ResourceCollection.build(
        new DefaultRepresentation(functions.col(resourceType.toCode())),
        getFhirContext(), resourceType);
  }

  /**
   * {@inheritDoc}
   * <p>
   * The base implementation throws an UnsupportedOperationException. Subclasses that support view
   * creation must override this method.
   */
  @Nonnull
  public Dataset<Row> createView() {
    throw new UnsupportedOperationException("createView() is not supported");
  }

  /**
   * Creates a dataset for a specific resource type with a standardized structure.
   * <p>
   * This method:
   * <ol>
   *   <li>Reads the resource data from the data source</li>
   *   <li>Checks if the resource type exists (throws an exception if not)</li>
   *   <li>Restructures the dataset in a standardized structure.
   * </ol>
   * <p>
   * This standardized structure is used throughout the resource resolution process.
   *
   * @param dataSource The data source to read from
   * @param resourceType The resource type to read
   * @return A dataset containing the resource data in a standardized structure
   * @throws IllegalArgumentException if the resource type is not found in the data source
   */
  @Nonnull
  protected static Dataset<Row> getResourceDataset(@Nonnull final DataSource dataSource,
      @Nonnull final ResourceType resourceType) {
    final Dataset<Row> dataset = dataSource.read(resourceType.toCode());
    return toResourceRepresentation(resourceType.toCode(), dataset);
  }

  /**
   * Creates a dataset for a specific resource type with a standardized structure.
   * <p>
   * This method restructures the dataset to have:
   *     <ul>
   *       <li>An "id" column for the resource ID</li>
   *       <li>A "key" column containing the versioned ID for joining</li>
   *       <li>A column named after the resource type containing all resource data as a struct</li>
   *     </ul>
   * <p>
   * This standardized structure is used throughout the resource resolution process.
   *
   * @param resourceCode The resource type to read
   * @param resourceDataset The dataset containing the flat resource data
   * @return A dataset containing the resource data in a standardized structure
   */
  protected static Dataset<Row> toResourceRepresentation(@Nonnull final String resourceCode,
      @Nonnull final Dataset<Row> resourceDataset) {
    return resourceDataset.select(
        resourceDataset.col("id"),
        resourceDataset.col("id_versioned").alias("key"),
        functions.struct(
            Stream.of(resourceDataset.columns())
                .map(resourceDataset::col).toArray(Column[]::new)
        ).alias(resourceCode));
  }
}
