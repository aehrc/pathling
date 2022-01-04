/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function;

import static au.csiro.pathling.fhirpath.function.NamedFunction.checkNoArguments;
import static au.csiro.pathling.fhirpath.function.NamedFunction.expressionFromInput;
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;
import static org.apache.spark.sql.functions.sum;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.NonLiteralPath;
import au.csiro.pathling.fhirpath.Numeric;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * A function for computing the sum of a collection of numeric values.
 *
 * @author John Grimes
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#sum">sum</a>
 */
public class SumFunction extends AggregateFunction implements NamedFunction {

  private static final String NAME = "sum";

  protected SumFunction() {
  }

  @Nonnull
  @Override
  public FhirPath invoke(@Nonnull final NamedFunctionInput input) {
    checkNoArguments("sum", input);
    checkUserInput(input.getInput() instanceof Numeric,
        "Input to sum function must be numeric: " + input.getInput().getExpression());

    final NonLiteralPath inputPath = input.getInput();
    final Dataset<Row> dataset = inputPath.getDataset();
    final String expression = expressionFromInput(input, NAME);
    final Column finalValueColumn = sum(inputPath.getValueColumn());

    return buildAggregateResult(dataset, input.getContext(), inputPath, finalValueColumn,
        expression);
  }

}
