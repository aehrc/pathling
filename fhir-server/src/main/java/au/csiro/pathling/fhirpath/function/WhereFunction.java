/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function;

import static au.csiro.pathling.fhirpath.function.NamedFunction.expressionFromInput;
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.NonLiteralPath;
import au.csiro.pathling.fhirpath.element.BooleanPath;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;

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
    final NonLiteralPath inputPath = input.getInput();
    checkUserInput(input.getArguments().get(0) instanceof NonLiteralPath,
        "Argument to where function cannot be a literal: " + input.getArguments().get(0)
            .getExpression());
    final NonLiteralPath argumentPath = (NonLiteralPath) input.getArguments().get(0);
    checkUserInput(argumentPath instanceof BooleanPath && argumentPath.isSingular(),
        "Argument to where function must be a singular Boolean: " + argumentPath.getExpression());
    checkUserInput(argumentPath.getThisColumn().isPresent(),
        "Argument to where function must be navigable from collection item (use $this): "
            + argumentPath.getExpression());
    final Column argumentValue = argumentPath.getValueColumn();

    // The result is the input value if it is equal to true, or null otherwise (signifying the
    // absence of a value).
    final Column idColumn = argumentPath.getIdColumn();
    final Column thisColumn = argumentPath.getThisColumn().get();
    final Column valueColumn = when(argumentValue.equalTo(true), thisColumn).otherwise(lit(null));
    final String expression = expressionFromInput(input, NAME);

    return inputPath
        .copy(expression, argumentPath.getDataset(), idColumn, valueColumn, inputPath.isSingular(),
            Optional.empty());
  }

}
