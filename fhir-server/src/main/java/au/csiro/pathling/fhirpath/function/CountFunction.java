/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function;

import static au.csiro.pathling.fhirpath.function.NamedFunction.checkNoArguments;
import static au.csiro.pathling.fhirpath.function.NamedFunction.expressionFromInput;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.when;

import au.csiro.pathling.fhirpath.FhirPath;
import java.util.function.Function;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * A function for aggregating data based on counting the number of rows within the result.
 *
 * @author John Grimes
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#count">count</a>
 */
public class CountFunction extends AggregateFunction implements NamedFunction {

  private static final String NAME = "count";

  protected CountFunction() {
  }

  @Nonnull
  @Override
  public FhirPath invoke(@Nonnull final NamedFunctionInput input) {
    checkNoArguments("count", input);
    final FhirPath inputPath = input.getInput();
    final String expression = expressionFromInput(input, NAME);
    // According to the FHIRPath specification, the count function must return 0 when invoked on an 
    // empty collection.
    final Function<Column, Column> count = (column) -> when(count(column).isNull(), 0L)
        .otherwise(count(column));
    return applyAggregation(input.getContext(), inputPath, count, expression,
        FHIRDefinedType.UNSIGNEDINT);
  }

}
