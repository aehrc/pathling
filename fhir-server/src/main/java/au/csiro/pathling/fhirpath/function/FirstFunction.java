/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function;

import static au.csiro.pathling.fhirpath.function.NamedFunction.checkNoArguments;
import static au.csiro.pathling.fhirpath.function.NamedFunction.expressionFromInput;
import static org.apache.spark.sql.functions.first;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.element.ElementPath;
import java.util.function.Function;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;

/**
 * This function allows the selection of only the first element of a collection.
 *
 * @author John Grimes
 * @author Piotr Szul
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#first">first</a>
 */
public class FirstFunction extends AggregateFunction implements NamedFunction {

  private static final String NAME = "first";

  protected FirstFunction() {
  }

  @Nonnull
  @Override
  public FhirPath invoke(@Nonnull NamedFunctionInput input) {
    checkNoArguments("first", input);

    final ElementPath inputPath = (ElementPath) input.getInput();
    final String expression = expressionFromInput(input, NAME);

    final Function<Column, Column> firstIgnoreNull = col -> first(col, true);

    return applyAggregationFunction(input.getContext(), inputPath, firstIgnoreNull, expression,
        inputPath.getFhirType());
  }
}
