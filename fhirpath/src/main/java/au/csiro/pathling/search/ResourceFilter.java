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

package au.csiro.pathling.search;

import static au.csiro.pathling.utilities.Preconditions.checkArgument;

import au.csiro.pathling.fhirpath.execution.ResourceSchemaTransformation;
import jakarta.annotation.Nonnull;
import lombok.Value;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import java.util.function.BinaryOperator;
import java.util.function.UnaryOperator;

/**
 * An immutable filter that can be applied to Pathling-encoded FHIR resource datasets.
 * <p>
 * ResourceFilter holds a pre-computed SparkSQL Column expression that represents filter criteria.
 * Filters can be composed using logical operators ({@link #and}, {@link #or}, {@link #not}) and
 * applied to datasets using {@link #apply}.
 * <p>
 * The filter expression is evaluated at creation time (eager evaluation), meaning that the Column
 * expression is built when the filter is created, not when it is applied. This allows filters to
 * be created without requiring a SparkSession or actual data.
 * <p>
 * Usage example:
 * <pre>{@code
 * // Create filters using ResourceFilterFactory
 * ResourceFilter maleFilter = factory.fromQueryString(ResourceType.PATIENT, "gender=male");
 * ResourceFilter activeFilter = factory.fromExpression(ResourceType.PATIENT, "active = true");
 *
 * // Compose filters
 * ResourceFilter combined = maleFilter.and(activeFilter);
 *
 * // Apply to a Pathling-encoded Patient dataset
 * Dataset<Row> patients = dataSource.read("Patient");
 * Dataset<Row> result = combined.apply(patients);
 * }</pre>
 *
 * @see ResourceFilterFactory
 */
@Value
public class ResourceFilter {

  /**
   * The resource type this filter applies to.
   */
  @Nonnull
  ResourceType resourceType;

  /**
   * The pre-computed SparkSQL Column expression representing the filter criteria.
   * <p>
   * This column expression is designed to be applied to a dataset with the evaluation schema
   * (id, key, ResourceType{...}).
   */
  @Nonnull
  Column filterExpression;

  /**
   * Transforms this filter by applying a unary operator to its filter expression.
   * <p>
   * This is a generic transformation method that can be used to implement various single-filter
   * operations.
   *
   * @param operator the unary operator to apply to the filter expression
   * @return a new filter with the transformed expression
   */
  @Nonnull
  public ResourceFilter map(@Nonnull final UnaryOperator<Column> operator) {
    return new ResourceFilter(resourceType, operator.apply(filterExpression));
  }

  /**
   * Combines this filter with another by applying a binary operator to their filter expressions.
   * <p>
   * This is a generic transformation method that can be used to implement various two-filter
   * combining operations. Both filters must be for the same resource type.
   *
   * @param other the other filter to combine with
   * @param operator the binary operator to apply to the filter expressions
   * @return a new filter with the combined expression
   * @throws IllegalArgumentException if the resource types don't match
   */
  @Nonnull
  public ResourceFilter biMap(
      @Nonnull final ResourceFilter other,
      @Nonnull final BinaryOperator<Column> operator) {
    checkArgument(resourceType.equals(other.resourceType),
        String.format("Cannot combine filters for different resource types: %s and %s",
            resourceType.toCode(), other.resourceType.toCode()));
    return new ResourceFilter(resourceType, operator.apply(filterExpression, other.filterExpression));
  }

  /**
   * Creates a new filter by combining this filter with another using AND logic.
   * <p>
   * Both filters must be for the same resource type.
   *
   * @param other the other filter to combine with
   * @return a new filter representing (this AND other)
   * @throws IllegalArgumentException if the resource types don't match
   */
  @Nonnull
  public ResourceFilter and(@Nonnull final ResourceFilter other) {
    return biMap(other, Column::and);
  }

  /**
   * Creates a new filter by combining this filter with another using OR logic.
   * <p>
   * Both filters must be for the same resource type.
   *
   * @param other the other filter to combine with
   * @return a new filter representing (this OR other)
   * @throws IllegalArgumentException if the resource types don't match
   */
  @Nonnull
  public ResourceFilter or(@Nonnull final ResourceFilter other) {
    return biMap(other, Column::or);
  }

  /**
   * Creates a new filter that negates this filter.
   *
   * @return a new filter representing NOT(this)
   */
  @Nonnull
  public ResourceFilter not() {
    return map(functions::not);
  }

  /**
   * Applies this filter to a flat Pathling-encoded resource dataset.
   * <p>
   * This method:
   * <ol>
   *   <li>Wraps the flat dataset into the evaluation schema (id, key, ResourceType{...})</li>
   *   <li>Applies the filter expression</li>
   *   <li>Unwraps the result back to the flat schema</li>
   * </ol>
   * <p>
   * The input dataset should have the standard Pathling flat schema with columns like
   * id, id_versioned, meta, name, etc.
   *
   * @param flatResourceDataset the flat Pathling-encoded resource dataset to filter
   * @return a filtered dataset with the same schema as the input
   */
  @Nonnull
  public Dataset<Row> apply(@Nonnull final Dataset<Row> flatResourceDataset) {
    final String resourceCode = resourceType.toCode();

    // Wrap to evaluation schema
    final Dataset<Row> wrapped = ResourceSchemaTransformation.wrapToEvaluationSchema(
        resourceCode, flatResourceDataset);

    // Apply filter
    final Dataset<Row> filtered = wrapped.filter(filterExpression);

    // Unwrap back to flat schema
    return ResourceSchemaTransformation.unwrapToFlatSchema(resourceCode, filtered);
  }
}
