/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function;

import static au.csiro.pathling.fhirpath.function.NamedFunction.expressionFromInput;
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;
import static org.apache.spark.sql.functions.when;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.NonLiteralPath;
import au.csiro.pathling.fhirpath.element.BooleanPath;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Describes a function which can scope down the previous invocation within a FHIRPath expression,
 * based upon an expression passed in as an argument. Supports the use of `$this` to reference the
 * element currently in scope.
 *
 * @author John Grimes
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#where">where</a>
 */
public class WhereFunction implements NamedFunction {

  private static final String NAME = "where";

  @Nonnull
  @Override
  public FhirPath invoke(@Nonnull final NamedFunctionInput input) {
    checkUserInput(input.getArguments().size() == 1,
        "where function accepts one argument");
    final FhirPath inputPath = input.getInput();
    checkUserInput(input.getArguments().get(0) instanceof NonLiteralPath,
        "Argument to where function cannot be a literal");
    final NonLiteralPath argumentPath = (NonLiteralPath) input.getArguments().get(0);
    checkUserInput(argumentPath instanceof BooleanPath && argumentPath.isSingular(),
        "Argument to where function must be a singular Boolean: " + argumentPath.getExpression());
    checkUserInput(argumentPath.getThisColumn().isPresent(),
        "Argument to where function must be navigable from collection item (use $this): "
            + argumentPath.getExpression());

    // We use the argument dataset alone, to avoid the problems with joining from the input
    // dataset to the argument dataset (which would create duplicate rows).
    final Dataset<Row> dataset = argumentPath.getDataset();

    // The result is the input value if it is equal to true, or null otherwise (signifying the
    // absence of a value).
    final Column argumentTrue = argumentPath.getValueColumn().equalTo(true);
    final Column thisColumn = argumentPath.getThisColumn().get();
    final Column valueColumn = when(argumentTrue, thisColumn).otherwise(null);

    final String expression = expressionFromInput(input, NAME);
    return inputPath
        .copy(expression, dataset, argumentPath.getIdColumn(), valueColumn, inputPath.isSingular(),
            Optional.empty());
  }

}
