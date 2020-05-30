/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function;

import static au.csiro.pathling.QueryHelpers.firstNColumns;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import java.util.List;
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
  protected ElementPath applyAggregation(@Nonnull final ParserContext context,
      @Nonnull final FhirPath input, @Nonnull final Function<Column, Column> function,
      @Nonnull final String expression, @Nonnull final FHIRDefinedType fhirType) {
    final Dataset<Row> result = input.getDataset().groupBy(context.getGroupBy())
        .agg(function.apply(input.getValueColumn()));

    // The value column will be the column following each of the grouping columns.
    final int numberOfGroupings = context.getGroupBy().length;
    final Column valueColumn = result.col(result.columns()[numberOfGroupings]);

    // We need to update the grouping columns, as the aggregation erases any columns that were 
    // previously in the Dataset.
    final List<Column> newGroupingColumns = firstNColumns(result, numberOfGroupings);
    context.setGroupingColumns(newGroupingColumns);

    return ElementPath.build(expression, result, input.getIdColumn(), valueColumn, true, fhirType);
  }

}
