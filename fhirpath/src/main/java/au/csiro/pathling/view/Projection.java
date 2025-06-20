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

package au.csiro.pathling.view;

import static au.csiro.pathling.utilities.Functions.maybeCast;
import static au.csiro.pathling.view.Projection.ProjectionConstraint.FLAT;
import static org.apache.spark.sql.functions.inline;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.StringCoercible;
import au.csiro.pathling.fhirpath.collection.BooleanCollection;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import au.csiro.pathling.views.ConstantDeclaration;
import jakarta.annotation.Nonnull;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.AllArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * An abstract representation of a projection of FHIR data, with the ability to select columns,
 * filter the result and make use of constants.
 */
@Value
@AllArgsConstructor
@Slf4j
public class Projection {


  public enum ProjectionConstraint {
    /**
     * Indicates that the result will be returned in a form that supports the representation of
     * complex types.
     */
    UNCONSTRAINED,

    /**
     * Indicates that the result will be returned in a form that will be constrained to a flat,
     * unstructured representation.
     */
    FLAT
  }

  /**
   * The resource type that the projection is based upon.
   */
  @Nonnull
  ResourceType subjectResource;

  /**
   * The constants that are available to the expressions within the projection.
   */
  @Nonnull
  List<ConstantDeclaration> constants;

  /**
   * The clause that defines the columns to be included in the projection.
   */
  @Nonnull
  ProjectionClause selection;

  /**
   * The clause that defines the rows to be included in the projection.
   */
  @Nonnull
  Optional<ProjectionClause> where;

  /**
   * A constraint on the result of the projection, such as whether it must be flat.
   */
  @Nonnull
  ProjectionConstraint constraint;

  public Projection(@Nonnull final ResourceType subjectResource,
      @Nonnull final List<ConstantDeclaration> constants,
      @Nonnull final ProjectionClause selection,
      @Nonnull final Optional<ProjectionClause> where) {
    this(subjectResource, constants, selection, where, ProjectionConstraint.UNCONSTRAINED);
  }

  /**
   * Executes the projection, returning a dataset that can be used to retrieve the result.
   *
   * @param context The execution context
   * @return The dataset that represents the result of the projection
   */
  public Dataset<Row> execute(@Nonnull final ExecutionContext context) {
    // Prepare dependencies for evaluation.
    final ProjectionContext projectionContext = ProjectionContext.of(context,
        subjectResource, constants);

    // Evaluate the selection clause.
    final ProjectionResult projectionResult = selection.evaluate(projectionContext);
    final Dataset<Row> unfiltered = projectionContext.getDataset();

    // Evaluate the where clause and build a filter column.
    final Optional<Column> filterColumn = evaluateFilters(projectionContext);

    // Apply the filter column to the unfiltered dataset.
    final Dataset<Row> filteredResult = filterColumn
        .map(unfiltered::filter)
        .orElse(unfiltered);

    // Convert the intermediate struct representation in the result column to a regular row, using 
    // the inline function.
    final Dataset<Row> inlinedResult = filteredResult
        .select(inline(projectionResult.getResultColumn()));

    // Get the list of column names from the data type of the column that we just inlined.
    final StructType schema = filteredResult.select(projectionResult.getResultColumn()).schema();
    final ArrayType arrayType = (ArrayType) schema.fields()[0].dataType();
    final StructType structType = (StructType) arrayType.elementType();
    final List<String> columnNames = Arrays.stream(structType.fields())
        .map(StructField::name)
        .toList();
    // Bind the result cllections to the columns in the projection result.
    final List<Collection> boundResults = IntStream.range(0, columnNames.size())
        .mapToObj(i -> projectionResult.getResults().get(i).getCollection()
            .copyWith(new
                DefaultRepresentation(inlinedResult.col(columnNames.get(i)))))
        .toList();

    Column[] renderedColumns = IntStream.range(0, columnNames.size())
        .mapToObj(i -> renderColumn(boundResults.get(i), columnNames.get(i)))
        .toArray(Column[]::new);
    // Select the columns from the inlined result.
    return inlinedResult.select(renderedColumns);
  }

  /**
   * Converts a {@link Collection} to a {@link Column} based upon the context of the requested
   * operation.
   *
   * @param collection The result to convert
   * @return The converted column
   */
  @Nonnull
  private Column renderColumn(@Nonnull final Collection collection, @Nonnull final String name) {
    final Collection finalResult;
    if (FLAT.equals(constraint)) {
      // If we are constrained to a flat result, we need to coerce the collection to a string.
      finalResult = Optional.of(collection)
          .flatMap(maybeCast(StringCoercible.class))
          .map(StringCoercible::asStringPath)
          .orElseThrow(
              () -> new InvalidUserInputError(
                  "Cannot render one of the columns in flat mode"));
    } else {
      // Otherwise, we can use the collection as-is.
      finalResult = collection;
    }
    // re-alias the column
    return finalResult.getColumn().getValue().alias(name);
  }


  /**
   * Evaluates the where clause and returns a column that can be used to filter the result.
   *
   * @param context The execution context
   * @return A column that can be used to filter the result
   */
  @Nonnull
  private Optional<Column> evaluateFilters(@Nonnull final ProjectionContext context) {
    return where.flatMap(whereSelection -> {
      final List<ProjectedColumn> whereResult = whereSelection.evaluate(context).getResults();
      final boolean isValidFilter = whereResult.stream()
          .allMatch(cr -> cr.getCollection() instanceof BooleanCollection);
      if (!isValidFilter) {
        throw new IllegalArgumentException("Filter must be a boolean expression");
      }
      return whereResult.stream()
          .map(cr -> cr.getCollection().asSingular().getColumn().getValue())
          .reduce(Column::and);
    });
  }

  @Override
  public String toString() {
    return "Projection{" +
        "subjectResource=" + subjectResource +
        ", constants=[" + constants.stream()
        .map(ConstantDeclaration::toString)
        .collect(Collectors.joining(", ")) +
        "], selection=" + selection +
        where.map(w -> ", where=" + w).orElse("") +
        ", constraint=" + constraint +
        '}';
  }
}
