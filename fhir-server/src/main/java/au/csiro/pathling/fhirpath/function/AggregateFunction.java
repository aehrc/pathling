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
  protected ElementPath applyAggregation(@Nonnull final ParserContext context,
      @Nonnull final FhirPath input, @Nonnull final Function<Column, Column> function,
      @Nonnull final String expression, @Nonnull final FHIRDefinedType fhirType) {
    check(context.getGroupBy().isPresent() || input.getIdColumn().isPresent());

    // Group by the grouping columns if present, or the ID column from the input.
    final Dataset<Row> result = input.getDataset()
        .groupBy(context.getGroupBy()
            .orElse(new Column[]{input.getIdColumn().get()})
        )
        .agg(function.apply(input.getValueColumn()));

    // If there were grouping columns, there will no longer be an ID column.
    final Optional<Column> updatedIdColumn = context.getGroupBy().isPresent()
                                             ? Optional.empty()
                                             : input.getIdColumn();

    // The grouping columns are updated to the new columns within the aggregation result.
    final IdAndValue idAndValue = updateGroupingColumns(context, result, updatedIdColumn);

    return ElementPath
        .build(expression, result, idAndValue.getIdColumn(), idAndValue.getValueColumn(), true,
            fhirType);
  }

}
