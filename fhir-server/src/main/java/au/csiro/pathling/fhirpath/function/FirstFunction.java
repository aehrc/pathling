/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function;

import static au.csiro.pathling.fhirpath.function.NamedFunction.checkNoArguments;
import static au.csiro.pathling.fhirpath.function.NamedFunction.expressionFromInput;
import static org.apache.spark.sql.functions.first;
import static org.apache.spark.sql.functions.min;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.NonLiteralPath;
import java.util.function.Function;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

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
  public FhirPath invoke(@Nonnull final NamedFunctionInput input) {
    checkNoArguments("first", input);

    final NonLiteralPath inputPath = input.getInput();
    final String expression = expressionFromInput(input, NAME);

    final Function<Column, Column> firstIgnoreNull = col -> first(col, true);
    final Function<Column, Column> eidAggreation = col -> min(col);

    // need to somwhow pass extra final Function<Dataset<Row>, Dataset<Row>> datasetTransform = ds -> ds.sort()
    final Function<FhirPath, Dataset<Row>> getDataset = fp -> fp.getDataset().sort(((NonLiteralPath)fp).getEidColumn().get());

    return applyAggregationFunction(input.getContext(), inputPath, firstIgnoreNull, expression, true);
  }
}
