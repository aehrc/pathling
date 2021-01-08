/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function;

import static au.csiro.pathling.utilities.Preconditions.checkArgument;
import static au.csiro.pathling.utilities.Preconditions.checkPresent;
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
import java.util.function.Predicate;
import java.util.stream.Collectors;
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
   * @param input the {@link FhirPath} objects being aggregated
   * @param valueColumn a {@link Column} describing the resulting value
   * @param expression the FHIRPath expression for the result
   * @return a new {@link ElementPath} representing the result
   */
  @Nonnull
  protected NonLiteralPath buildAggregateResult(@Nonnull final Dataset<Row> dataset,
      @Nonnull final ParserContext parserContext, @Nonnull final NonLiteralPath input,
      @Nonnull final Column valueColumn, @Nonnull final String expression) {

    return buildAggregateResult(dataset, parserContext, Collections.singletonList(input),
        valueColumn,
        expression, input::copy);
  }

  /**
   * Builds a result for an aggregation operation, with a single {@link FhirPath} object as input.
   *
   * @param dataset the {@link Dataset} that will be used in the result
   * @param input the {@link FhirPath} objects being aggregated
   * @param valueColumn a {@link Column} describing the resulting value
   * @param expression the FHIRPath expression for the result
   * @param fhirType the {@link FHIRDefinedType} of the result
   * @return a new {@link ElementPath} representing the result
   */
  @SuppressWarnings("SameParameterValue")
  @Nonnull
  protected ElementPath buildAggregateResult(@Nonnull final Dataset<Row> dataset,
      @Nonnull final ParserContext parserContext, @Nonnull final FhirPath input,
      @Nonnull final Column valueColumn, @Nonnull final String expression,
      @Nonnull final FHIRDefinedType fhirType) {

    return buildAggregateResult(dataset, parserContext, Collections.singletonList(input),
        valueColumn,
        expression, fhirType);
  }

  /**
   * Builds a result for an aggregation operation, with possibly multiple {@link FhirPath} objects
   * as input (e.g. in the case of a binary operator that performs aggregation).
   *
   * @param dataset the {@link Dataset} that will be used in the result
   * @param inputs the {@link FhirPath} objects being aggregated
   * @param valueColumn a {@link Column} describing the resulting value
   * @param expression the FHIRPath expression for the result
   * @param fhirType the {@link FHIRDefinedType} of the result
   * @return a new {@link ElementPath} representing the result
   */
  @Nonnull
  protected ElementPath buildAggregateResult(@Nonnull final Dataset<Row> dataset,
      @Nonnull final ParserContext parserContext, @Nonnull final Collection<FhirPath> inputs,
      @Nonnull final Column valueColumn, @Nonnull final String expression,
      @Nonnull final FHIRDefinedType fhirType) {

    return buildAggregateResult(dataset, parserContext, inputs, valueColumn, expression,
        // Create the result as an ElementPath of the given FHIR type.
        (exp, ds, id, eid, value, singular, thisColumn) -> ElementPath
            .build(exp, ds, id, eid, value, true, Optional.empty(), thisColumn, fhirType));
  }

  @Nonnull
  private <T extends FhirPath> T buildAggregateResult(@Nonnull final Dataset<Row> dataset,
      @Nonnull final ParserContext parserContext, @Nonnull final Collection<FhirPath> inputs,
      @Nonnull final Column valueColumn, @Nonnull final String expression,
      @Nonnull final ResultPathFactory<T> resultPathFactory) {

    checkArgument(!inputs.isEmpty(), "Collection of inputs cannot be empty");

    // Use an ID column from any of the inputs.
    final Column idColumn = inputs.stream().findFirst().get().getIdColumn();

    // Determine the group by columns based on whether we are in the context of a grouping, or an
    // individual resource.
    final List<Column> groupByList = parserContext.getGroupingColumns()
        .orElse(Collections.singletonList(idColumn));
    final Column[] groupBy = groupByList
        .toArray(Column[]::new);

    // The selection will be either:
    // (1) the first function applied to each column except the resource ID, plus the value column
    //     (in the case of individual resource context), or;
    // (2) the first function applied to each column except the grouping columns, plus the value
    //     column (in the case of a grouping context).
    final Predicate<Column> resourceFilter = column -> !column.equals(idColumn);
    final Predicate<Column> groupingFilter = column -> !groupByList.contains(column);
    final List<Column> selection = Stream.of(dataset.columns())
        .map(functions::col)
        .filter(parserContext.getGroupingColumns().isEmpty()
                ? resourceFilter
                : groupingFilter)
        .map(column -> first(column, true).alias(column.toString()))
        .collect(Collectors.toList());
    selection.add(valueColumn.alias("value"));

    final Column firstSelection = checkPresent(selection.stream().limit(1).findFirst());
    final Column[] remainingSelection = selection.stream().skip(1).toArray(Column[]::new);

    // Get any this columns that may be present in the inputs.
    // TODO: This is very error prone as a collection can be passed here instead of an array.
    //  How can we make it more stringent?
    @SuppressWarnings("ConfusingArgumentToVarargsMethod")
    final Optional<Column> thisColumn = NonLiteralPath
        .findThisColumn(inputs.toArray(new FhirPath[0]));

    final Dataset<Row> finalDataset = dataset
        .groupBy(groupBy)
        .agg(firstSelection, remainingSelection);
    final Column finalValueColumn = col("value");

    // empty eid column as the result is singular
    return resultPathFactory
        .create(expression, finalDataset, idColumn, Optional.empty(), finalValueColumn, true,
            thisColumn);
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
     * @param eidColumn the new element identity column
     * @param valueColumn the new expression value column
     * @param singular the new singular value
     * @param thisColumn a column containing the collection being iterated, for cases where a path
     * is being created to represent the {@code $this} keyword
     * @return a new instance of T
     */
    T create(@Nonnull String expression, @Nonnull Dataset<Row> dataset,
        @Nonnull Column idColumn, @Nonnull Optional<Column> eidColumn, @Nonnull Column valueColumn,
        boolean singular, @Nonnull Optional<Column> thisColumn);
  }

}
