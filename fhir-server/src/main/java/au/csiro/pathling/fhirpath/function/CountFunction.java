/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function;

import static au.csiro.pathling.fhirpath.function.NamedFunction.checkNoArguments;
import static au.csiro.pathling.fhirpath.function.NamedFunction.expressionFromInput;
import static org.apache.spark.sql.functions.when;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.NonLiteralPath;
import au.csiro.pathling.fhirpath.ResourcePath;
import java.util.function.Function;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
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
    final NonLiteralPath inputPath = input.getInput();
    final String expression = expressionFromInput(input, NAME);
    final Column subjectColumn = inputPath.getValueColumn();

    // When we are counting resources, we use the distinct count to account for the fact that
    // there may be duplicate IDs in the dataset.
    // When we are counting elements, we use a non-distinct count, to account for the fact that it
    // is valid to have multiple elements with the same value.
    final Function<Column, Column> countFunction = inputPath instanceof ResourcePath
                                                   ? functions::countDistinct
                                                   : functions::count;

    // According to the FHIRPath specification, the count function must return 0 when invoked on an
    // empty collection.
    final Column valueColumn = when(countFunction.apply(subjectColumn).isNull(), 0L)
        .otherwise(countFunction.apply(subjectColumn));

    return buildAggregateResult(inputPath.getDataset(), input.getContext(), inputPath, valueColumn,
        expression, FHIRDefinedType.UNSIGNEDINT);
  }

}
