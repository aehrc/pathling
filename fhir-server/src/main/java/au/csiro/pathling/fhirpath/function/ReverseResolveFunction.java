/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function;

import static au.csiro.pathling.QueryHelpers.joinOnIdAndReference;
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;

import au.csiro.pathling.QueryHelpers.JoinType;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.ResourceDefinition;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.element.ReferencePath;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * A function for accessing elements of resources which refer to the input resource. The path to the
 * referring element is supplied as an argument.
 *
 * @author John Grimes
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#reverseresolve">reverseResolve</a>
 */
public class ReverseResolveFunction implements NamedFunction {

  private static final String NAME = "reverseResolve";

  @Nonnull
  @Override
  public FhirPath invoke(@Nonnull final NamedFunctionInput input) {
    checkUserInput(input.getInput() instanceof ResourcePath,
        "Input to " + NAME + " function must be a resource: " + input.getInput().getExpression());
    final FhirPath inputPath = input.getInput();
    final String expression = NamedFunction.expressionFromInput(input, NAME);
    checkUserInput(input.getArguments().size() == 1,
        "reverseResolve function accepts a single argument: " + expression);
    final FhirPath argument = input.getArguments().get(0);
    checkUserInput(argument instanceof ReferencePath,
        "Argument to reverseResolve function must be a Reference: " + argument.getExpression());

    // Do a left outer join from the input to the argument dataset using the reference field in the
    // argument.
    final Dataset<Row> dataset = joinOnIdAndReference(inputPath, argument.getDataset(),
        argument.getValueColumn(), JoinType.LEFT_OUTER);

    // Check the path for origin information - if it not present, reverse reference resolution will
    // not be possible. This may occur in some cases, e.g. where aggregations are performed within
    // the argument to this function.
    final Optional<Column> optionalOriginColumn = argument.getOriginColumn();
    final Optional<ResourceDefinition> optionalOriginType = argument.getOriginType();
    checkUserInput(optionalOriginColumn.isPresent() && optionalOriginType.isPresent(),
        "Argument to reverse resolve must be an element that is navigable from the "
            + "target resource type, without any aggregations: " + expression);

    return new ResourcePath(expression, dataset, inputPath.getIdColumn(),
        optionalOriginColumn.get(), false, optionalOriginType.get());
  }

}
