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

package au.csiro.pathling.fhirpath.function;

import static au.csiro.pathling.utilities.Preconditions.checkArgument;
import static au.csiro.pathling.utilities.Preconditions.checkPresent;
import static au.csiro.pathling.utilities.Strings.randomAlias;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.first;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.NonLiteralPath;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Represents a function intended to reduce a set of values to a single value.
 *
 * @author John Grimes
 */
public abstract class AggregateFunction {

  /**
   * Builds a result for an aggregation operation, with a single {@link FhirPath} object as input
   * that will be copied and used as a template for the new result.
   *
   * @param dataset the {@link Dataset} that will be used in the result
   * @param parserContext the current {@link ParserContext}
   * @param input the {@link FhirPath} objects being aggregated
   * @param aggregateColumn a {@link Column} describing the resulting value
   * @param expression the FHIRPath expression for the result
   * @return a new {@link ElementPath} representing the result
   */
  @Nonnull
  protected NonLiteralPath buildAggregateResult(@Nonnull final Dataset<Row> dataset,
      @Nonnull final ParserContext parserContext, @Nonnull final NonLiteralPath input,
      @Nonnull final Column aggregateColumn, @Nonnull final String expression) {

    return buildAggregateResult(dataset, parserContext, Collections.singletonList(input),
        aggregateColumn, expression, input::copy);
  }

  /**
   * Builds a result for an aggregation operation, with a single {@link FhirPath} object as input.
   *
   * @param dataset the {@link Dataset} that will be used in the result
   * @param parserContext the current {@link ParserContext}
   * @param input the {@link FhirPath} objects being aggregated
   * @param aggregateColumn a {@link Column} describing the resulting value
   * @param expression the FHIRPath expression for the result
   * @param fhirType the {@link FHIRDefinedType} of the result
   * @return a new {@link ElementPath} representing the result
   */
  @SuppressWarnings("SameParameterValue")
  @Nonnull
  protected ElementPath buildAggregateResult(@Nonnull final Dataset<Row> dataset,
      @Nonnull final ParserContext parserContext, @Nonnull final FhirPath input,
      @Nonnull final Column aggregateColumn, @Nonnull final String expression,
      @Nonnull final FHIRDefinedType fhirType) {

    return buildAggregateResult(dataset, parserContext, Collections.singletonList(input),
        aggregateColumn, expression, fhirType);
  }

  /**
   * Builds a result for an aggregation operation, with possibly multiple {@link FhirPath} objects
   * as input (e.g. in the case of a binary operator that performs aggregation).
   *
   * @param dataset the {@link Dataset} that will be used in the result
   * @param parserContext the current {@link ParserContext}
   * @param inputs the {@link FhirPath} objects being aggregated
   * @param aggregateColumn a {@link Column} describing the aggregation operation
   * the final value
   * @param expression the FHIRPath expression for the result
   * @param fhirType the {@link FHIRDefinedType} of the result
   * @return a new {@link ElementPath} representing the result
   */
  @Nonnull
  protected ElementPath buildAggregateResult(@Nonnull final Dataset<Row> dataset,
      @Nonnull final ParserContext parserContext, @Nonnull final Collection<FhirPath> inputs,
      @Nonnull final Column aggregateColumn, @Nonnull final String expression,
      @Nonnull final FHIRDefinedType fhirType) {

    return buildAggregateResult(dataset, parserContext, inputs, aggregateColumn, expression,
        // Create the result as an ElementPath of the given FHIR type.
        (exp, ds, id, value, ordering, singular, thisColumn) -> ElementPath.build(exp, ds, id,
            value, ordering, true, Optional.empty(), thisColumn, fhirType));
  }

  @Nonnull
  private <T extends FhirPath> T buildAggregateResult(@Nonnull final Dataset<Row> dataset,
      @Nonnull final ParserContext parserContext, @Nonnull final Collection<FhirPath> inputs,
      @Nonnull final Column aggregateColumn, @Nonnull final String expression,
      @Nonnull final ResultPathFactory<T> resultPathFactory) {

    checkArgument(!inputs.isEmpty(), "Collection of inputs cannot be empty");

    // Use an ID column from any of the inputs.
    final Column idColumn = inputs.stream().findFirst().get().getIdColumn();

    // Get the grouping columns from the parser context.
    final List<Column> groupingColumns = parserContext.getGroupingColumns();
    final Column[] groupBy = groupingColumns.toArray(new Column[0]);

    // Clear the nesting present within the context.
    parserContext.getNesting().clear();

    // Perform the aggregation, while also preserving all columns that are not being aggregated 
    // using the "first()" function.
    // First, collect the grouping and non-grouping columns into separate collections so that we can 
    // feed them to the "agg" method.
    final Set<Column> nonGroupingColumns = Stream.of(dataset.columns())
        .map(functions::col)
        .collect(toSet());
    groupingColumns.forEach(nonGroupingColumns::remove);

    // Wrap the non-grouping columns in a "first" aggregation.
    final List<Column> selection = nonGroupingColumns.stream()
        .map(column -> first(column, true).alias(column.toString()))
        .collect(toList());
    final String valueColumnName = randomAlias();
    selection.add(aggregateColumn.alias(valueColumnName));

    // Separate the first column from the rest, so that we can pass all the columns to the "agg" 
    // method.
    final Column firstSelection = checkPresent(selection.stream().limit(1).findFirst());
    final Column[] remainingSelection = selection.stream().skip(1).toArray(Column[]::new);

    // Perform the final aggregation, the first row of each non-grouping column grouped by the 
    // grouping columns.
    final Dataset<Row> result = dataset
        .groupBy(groupBy)
        .agg(firstSelection, remainingSelection);
    final Column valueColumn = col(valueColumnName);

    // Get any "this" columns that may be present in the inputs.
    final Optional<Column> thisColumn = NonLiteralPath
        .findThisColumn((Object[]) inputs.toArray(new FhirPath[0]));

    return resultPathFactory.create(expression, result, idColumn, valueColumn, Optional.empty(),
        true, thisColumn);
  }

  /**
   * A factory that encapsulates creation of the aggregation result path.
   *
   * @param <T> subtype of FhirPath to create
   */
  private interface ResultPathFactory<T extends FhirPath> {

    /**
     * Creates a subtype T of FhirPath
     *
     * @param expression an updated expression to describe the new FhirPath
     * @param dataset the new Dataset that can be used to evaluate this FhirPath against data
     * @param idColumn the new resource identity column
     * @param valueColumn the new expression value column
     * @param orderingColumn the new ordering column
     * @param singular the new singular value
     * @param thisColumn a column containing the collection being iterated, for cases where a path
     * is being created to represent the {@code $this} keyword
     * @return a new instance of T
     */
    T create(@Nonnull String expression, @Nonnull Dataset<Row> dataset, @Nonnull Column idColumn,
        @Nonnull Column valueColumn, @Nonnull Optional<Column> orderingColumn, boolean singular,
        @Nonnull Optional<Column> thisColumn);
  }

}
