/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function;

import static au.csiro.pathling.utilities.Preconditions.checkUserInput;

import au.csiro.pathling.encoders2.ExtensionSupport;
import au.csiro.pathling.fhirpath.Comparable.ComparisonOperation;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.NonLiteralPath;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.literal.StringLiteralPath;
import au.csiro.pathling.fhirpath.operator.ComparisonOperator;
import au.csiro.pathling.fhirpath.operator.OperatorInput;
import au.csiro.pathling.fhirpath.operator.PathTraversalInput;
import au.csiro.pathling.fhirpath.operator.PathTraversalOperator;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import java.util.Collections;
import javax.annotation.Nonnull;

/**
 * A function filters items in the input collection to only those that are of the given type.
 *
 * @author John Grimes
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#extension">extension</a>
 */
public class ExtensionFunction implements NamedFunction {

  private static final String NAME = "extension";

  @Nonnull
  @Override
  public FhirPath invoke(@Nonnull final NamedFunctionInput input) {
    final String expression = NamedFunction.expressionFromInput(input, NAME);

    checkUserInput(input.getArguments().size() == 1,
        "extension function must have one argument: " + expression);
    final FhirPath urlArgument = input.getArguments().get(0);
    checkUserInput(urlArgument instanceof StringLiteralPath,
        "extension function must have argument of type String literal: " + expression);

    final NonLiteralPath inputPath = input.getInput();
    final ElementPath extensionPath = new PathTraversalOperator()
        .invoke(new PathTraversalInput(input.getContext(), inputPath,
            ExtensionSupport.EXTENSION_ELEMENT_NAME()));

    // now we need to create a correct argument context for the where call.
    final ParserContext argumentContext = input.getContext();
    final FhirPath extensionUrlPath = new PathTraversalOperator()
        .invoke(new PathTraversalInput(argumentContext, extensionPath.toThisPath(), "url"));
    final FhirPath extensionUrCondition = new ComparisonOperator(ComparisonOperation.EQUALS)
        .invoke(new OperatorInput(argumentContext, extensionUrlPath, urlArgument));

    // override the expression in function input
    return new WhereFunction()
        .invoke(new NamedFunctionInput(input.getContext(), extensionPath,
            Collections.singletonList(extensionUrCondition), expression));
  }

}
