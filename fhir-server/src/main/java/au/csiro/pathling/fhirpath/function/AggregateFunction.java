/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function;

import static au.csiro.pathling.QueryHelpers.firstNColumns;
import static au.csiro.pathling.utilities.Preconditions.checkArgument;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.NonLiteralPath;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import java.util.*;
import java.util.function.Function;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Represents a function intended to reduce a set of values to a single value.
 *
 * @author John Grimes
 */
public abstract class AggregateFunction {

  /**
   * A factory that encapsulates creation of the aggregation result path
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
     * @param singular the new singular value
     * @param thisColumn a column containing the collection being iterated, for cases where a path
     * is being created to represent the {@code $this} keyword
     * @return a new instance of T
     */
    T create(@Nonnull String expression, @Nonnull Dataset<Row> dataset,
        @Nonnull Optional<Column> idColumn, @Nonnull Column valueColumn, boolean singular,
        @Nonnull Optional<Column> thisColumn);
  }

  /**
   * Applies a function-based aggregation, with a single {@link FhirPath} as an input.
   *
   * @param context the current {@link ParserContext}
   * @param input the {@link FhirPath} being aggregated
   * @param function the {@link Function} that will take a {@link Column}, and return another
   * Column
   * @param expression the FHIRPath expression for the result
   * @param fhirType the {@link FHIRDefinedType} of the result
   * @return a new {@link ElementPath} representing the result
   */
  @Nonnull
  @SuppressWarnings("SameParameterValue")
  protected ElementPath applyAggregationFunction(@Nonnull final ParserContext context,
      @Nonnull final FhirPath input, @Nonnull final Function<Column, Column> function,
      @Nonnull final String expression, @Nonnull final FHIRDefinedType fhirType) {
    return applyAggregation(context, input.getDataset(), Collections.singletonList(input),
        function.apply(input.getValueColumn()), expression, fhirType);
  }

  /**
   * Applies a {@link Column}-based aggregation, with possibly multiple {@link FhirPath} objects as
   * input (e.g. in the case of a binary operator that performs aggregation).
   *
   * @param context the current {@link ParserContext}
   * @param dataset the {@link Dataset} that will be aggregated
   * @param inputs the {@link FhirPath} objects being aggregated
   * @param aggregationColumn a {@link Column} describing the aggregation
   * @param expression the FHIRPath expression for the result
   * @param fhirType the {@link FHIRDefinedType} of the result
   * @return a new {@link ElementPath} representing the result
   */
  @Nonnull
  protected ElementPath applyAggregation(@Nonnull final ParserContext context,
      @Nonnull final Dataset<Row> dataset, @Nonnull final Collection<FhirPath> inputs,
      @Nonnull final Column aggregationColumn, @Nonnull final String expression,
      @Nonnull final FHIRDefinedType fhirType) {

    return applyAggregation(context, dataset, inputs, aggregationColumn, expression,
        // create the result as an ElementPath of given FhirType
        new ResultPathFactory<ElementPath>() {
          @Override
          public ElementPath create(@Nonnull String expression, @Nonnull Dataset<Row> dataset,
              @Nonnull Optional<Column> idColumn, @Nonnull Column valueColumn, boolean singular,
              @Nonnull Optional<Column> thisColumn) {
            return ElementPath
                .build(expression, dataset, idColumn, valueColumn, true, Optional.empty(),
                    thisColumn,
                    fhirType);
          }
        });
  }

  /**
   * Applies a function-based aggregation, with a single {@link FhirPath} as an input.
   *
   * @param context the current {@link ParserContext}
   * @param input the {@link NonLiteralPath} being aggregated (also prototype for the result path)
   * @param function the {@link Function} that will take a {@link Column}, and return another
   * Column
   * @param expression the FHIRPath expression for the result
   * @return a new {@link NonLiteralPath} representing the result of the same type of input
   */
  @Nonnull
  @SuppressWarnings("SameParameterValue")
  protected NonLiteralPath applyAggregationFunction(@Nonnull final ParserContext context,
      @Nonnull final NonLiteralPath input, @Nonnull final Function<Column, Column> function,
      @Nonnull final String expression) {
    return applyAggregation(context, input.getDataset(), Collections.singletonList(input),
        function.apply(input.getValueColumn()), expression, input);
  }

  /**
   * Applies a {@link Column}-based aggregation, with possibly multiple {@link FhirPath} objects as
   * input (e.g. in the case of a binary operator that performs aggregation).
   *
   * @param context the current {@link ParserContext}
   * @param dataset the {@link Dataset} that will be aggregated
   * @param inputs the {@link FhirPath} objects being aggregated
   * @param aggregationColumn a {@link Column} describing the aggregation
   * @param expression the FHIRPath expression for the result
   * @param contextPath the {@link NonLiteralPath} to use a prototype for result path
   * @return a new {@link FhirPath} representing the result
   */
  @Nonnull
  protected NonLiteralPath applyAggregation(@Nonnull final ParserContext context,
      @Nonnull final Dataset<Row> dataset, @Nonnull final Collection<FhirPath> inputs,
      @Nonnull final Column aggregationColumn, @Nonnull final String expression,
      @Nonnull final NonLiteralPath contextPath) {

    return applyAggregation(context, dataset, inputs, aggregationColumn, expression,
        // create the result as a copy of input path
        new ResultPathFactory<NonLiteralPath>() {
          @Override
          public NonLiteralPath create(@Nonnull String expression, @Nonnull Dataset<Row> dataset,
              @Nonnull Optional<Column> idColumn, @Nonnull Column valueColumn, boolean singular,
              @Nonnull Optional<Column> thisColumn) {
            return contextPath
                .copy(expression, dataset, idColumn, valueColumn, singular, thisColumn);
          }
        });
  }

  /**
   * Applies a {@link Column}-based aggregation, with possibly multiple {@link FhirPath} objects as
   * input (e.g. in the case of a binary operator that performs aggregation).
   *
   * @param context the current {@link ParserContext}
   * @param dataset the {@link Dataset} that will be aggregated
   * @param inputs the {@link FhirPath} objects being aggregated
   * @param aggregationColumn a {@link Column} describing the aggregation
   * @param expression the FHIRPath expression for the result
   * @param resultPathFactory the {@link ResultPathFactory} factory to use to create the result
   * path
   * @return a new {@link T} representing the result
   */
  @Nonnull
  private <T extends FhirPath> T applyAggregation(@Nonnull final ParserContext context,
      @Nonnull final Dataset<Row> dataset, @Nonnull final Collection<FhirPath> inputs,
      @Nonnull final Column aggregationColumn, @Nonnull final String expression,
      @Nonnull final ResultPathFactory<T> resultPathFactory) {
    final List<Column> groupingColumns = context.getGroupingColumns();

    // Use an ID column from any of the inputs.
    final Optional<Column> idColumn = FhirPath.findIdColumn(inputs.toArray());

    // There should be either an ID column or at least one grouping column.
    checkArgument(idColumn.isPresent() || groupingColumns.size() > 0,
        "ID column should be present within inputs, or groupings should be present in context");

    // Check for a $this column in any of the inputs - if its present, it will need to be preserved.
    final Optional<Column> thisColumn = NonLiteralPath.findThisColumn(inputs.toArray());

    // Calculate the set of grouping columns based on the grouping columns in the context, plus any
    // columns within the input paths that need to be preserved.
    final Column[] groupByArray = getGroupBy(groupingColumns, idColumn, thisColumn);

    // Apply the aggregation.
    final Dataset<Row> result = dataset.groupBy(groupByArray).agg(aggregationColumn);

    int cursor = 0;
    final Optional<Column> newIdColumn;
    if (idColumn.isPresent() && groupingColumns.isEmpty()) {
      // If there are no grouping columns, we just need to get the updated ID column.
      newIdColumn = Optional.of(result.col(result.columns()[0]));
      cursor += 1;
    } else {
      // If there are grouping columns, there will no longer be an ID column. The new grouping
      // columns will be the first columns in the table.
      newIdColumn = Optional.empty();
      final List<Column> newGroupingColumns = firstNColumns(result, groupingColumns.size());
      context.setGroupingColumns(newGroupingColumns);
      cursor += groupingColumns.size();
    }

    // Update the function input value column, if present.
    Optional<Column> newThisColumn = Optional.empty();
    if (thisColumn.isPresent()) {
      final String thisValueColName = result.columns()[cursor];
      newThisColumn = Optional.of(result.col(thisValueColName));
      cursor += 1;
    }

    // The value column will be the final column, after all the other columns.
    final String valueColName = result.columns()[cursor];
    final Column valueColumn = result.col(valueColName);

    return resultPathFactory.create(expression, result, newIdColumn, valueColumn,
        true, newThisColumn);
  }

  @Nonnull
  private static Column[] getGroupBy(@Nonnull final Collection<Column> groupingColumns,
      @Nonnull final Optional<Column> idColumn, @Nonnull final Optional<Column> thisColumn) {
    final List<Column> groupBy = new ArrayList<>();
    if (idColumn.isPresent() && groupingColumns.isEmpty()) {
      groupBy.add(idColumn.get());
    } else {
      groupBy.addAll(groupingColumns);
    }

    thisColumn.ifPresent(groupBy::add);

    return groupBy.toArray(new Column[]{});
  }

}
