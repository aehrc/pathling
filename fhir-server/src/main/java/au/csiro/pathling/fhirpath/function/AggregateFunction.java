/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function;

import static au.csiro.pathling.QueryHelpers.updateGroupingColumns;
import static au.csiro.pathling.utilities.Preconditions.check;

import au.csiro.pathling.QueryHelpers.IdAndValue;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import java.util.Arrays;
import java.util.Optional;
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

  @Nonnull
  @SuppressWarnings("SameParameterValue")
  protected ElementPath applyAggregationFunction(@Nonnull final ParserContext context,
      @Nonnull final FhirPath input, @Nonnull final Function<Column, Column> function,
      @Nonnull final String expression, @Nonnull final FHIRDefinedType fhirType) {
    return applyAggregation(context, input.getDataset(), input.getIdColumn(),
        function.apply(input.getValueColumn()), expression, fhirType);
  }

  @Nonnull
  protected ElementPath applyAggregation(@Nonnull final ParserContext context,
      @Nonnull final Dataset<Row> dataset, @Nonnull final Optional<Column> idColumn,
      @Nonnull final Column aggregationColumn, @Nonnull final String expression,
      @Nonnull final FHIRDefinedType fhirType) {
    check(context.getGroupBy().isPresent() || idColumn.isPresent());

    // Group by the grouping columns if present, or the ID column from the input.
    @SuppressWarnings("OptionalGetWithoutIsPresent") Column[] groupingColumns = context.getGroupBy()
        .orElse(new Column[]{idColumn.get()});

    // Add in the value column from the $this context if it is present, so as to preserve it.
    if (context.getThisContext().isPresent()) {
      groupingColumns = Arrays.copyOf(groupingColumns, groupingColumns.length + 1);
      groupingColumns[groupingColumns.length - 1] = context.getThisContext().get().getValueColumn();
    }

    // Apply the aggregation.
    final Dataset<Row> result = dataset.groupBy(groupingColumns).agg(aggregationColumn);

    // If there were grouping columns, there will no longer be an ID column.
    final Optional<Column> updatedIdColumn = context.getGroupBy().isPresent()
                                             ? Optional.empty()
                                             : idColumn;

    // The grouping columns are updated to the new columns within the aggregation result.
    final IdAndValue idAndValue = updateGroupingColumns(context, result, updatedIdColumn);

    return ElementPath
        .build(expression, result, idAndValue.getIdColumn(), idAndValue.getValueColumn(), true,
            fhirType);
  }

}
