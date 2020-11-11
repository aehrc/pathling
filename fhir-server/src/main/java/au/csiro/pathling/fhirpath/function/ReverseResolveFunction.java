/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function;

import static au.csiro.pathling.QueryHelpers.joinOnReference;
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;

import au.csiro.pathling.QueryHelpers.JoinType;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.NonLiteralPath;
import au.csiro.pathling.fhirpath.Referrer;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.element.ReferencePath;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

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
    final ResourcePath inputPath = (ResourcePath) input.getInput();
    final String expression = NamedFunction.expressionFromInput(input, NAME);
    checkUserInput(input.getArguments().size() == 1,
        "reverseResolve function accepts a single argument: " + expression);
    final FhirPath argument = input.getArguments().get(0);
    checkUserInput(argument instanceof ReferencePath,
        "Argument to reverseResolve function must be a Reference: " + argument.getExpression());
    final Referrer referencePath = (ReferencePath) argument;

    // Check that the input type is one of the possible types specified by the argument.
    final Set<ResourceType> argumentTypes = ((ReferencePath) argument).getResourceTypes();
    final ResourceType inputType = inputPath.getResourceType();
    checkUserInput(argumentTypes.contains(inputType),
        "Reference in argument to reverseResolve does not support input resource type: "
            + expression);

    // Do a left outer join from the input to the argument dataset using the reference field in the
    // argument.
    final Dataset<Row> dataset = joinOnReference(referencePath, inputPath, JoinType.RIGHT_OUTER);

    // Check the argument for information about a foreign resource that it originated from - if it
    // not present, reverse reference resolution will not be possible.
    checkUserInput(argument instanceof NonLiteralPath,
        "Argument to reverseResolve cannot be a literal");
    final NonLiteralPath nonLiteralArgument = (NonLiteralPath) argument;
    checkUserInput(nonLiteralArgument.getForeignResource().isPresent(),
        "Argument to reverseResolve must be an element that is navigable from a "
            + "target resource type: " + expression);
    final ResourcePath foreignResource = nonLiteralArgument.getForeignResource().get();

    final Optional<List<Column>> thisColumns = inputPath.getThisColumns();
    return foreignResource
        .copy(expression, dataset, inputPath.getIdColumn(), foreignResource.getValueColumns(),
            false, thisColumns);
  }

}
