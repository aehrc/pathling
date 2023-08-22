package au.csiro.pathling.fhirpath.function;

import static au.csiro.pathling.fhirpath.function.NamedFunction.checkNoArguments;
import static au.csiro.pathling.fhirpath.function.NamedFunction.expressionFromInput;
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;

import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.StringCoercible;
import javax.annotation.Nonnull;

/**
 * A function that converts a path to a String, if the operation is supported.
 *
 * @author John Grimes
 */
public class ToStringFunction implements NamedFunction {

  private static final String NAME = "toString";

  @Nonnull
  @Override
  public Collection invoke(@Nonnull final NamedFunctionInput input) {
    // Check that the function has no arguments.
    checkNoArguments(NAME, input);
    final Collection inputPath = input.getInput();

    // Check that the input is coercible to a String.
    checkUserInput(inputPath instanceof StringCoercible,
        "Cannot coerce path to a String type: " + inputPath.getExpression());
    final StringCoercible stringCoercible = (StringCoercible) inputPath;

    // Create an expression for the new path.
    final String expression = expressionFromInput(input, NAME, input.getInput());

    // Coerce the input to a String.
    return stringCoercible.asStringPath(expression, stringCoercible.getExpression());
  }

}
