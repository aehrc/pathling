package au.csiro.pathling.fhirpath.function;

import au.csiro.pathling.fhirpath.annotations.Name;
import au.csiro.pathling.fhirpath.annotations.NotImplemented;

/**
 * A function that converts a path to a String, if the operation is supported.
 *
 * @author John Grimes
 */
@Name("toString")
@NotImplemented
public class ToStringFunction implements NamedFunction {

  // TODO: implement as columns

  // @Nonnull
  // @Override
  // public Collection invoke(@Nonnull final NamedFunctionInput input) {
  //   // Check that the function has no arguments.
  //   checkNoArguments(NAME, input);
  //   final Collection inputPath = input.getInput();
  //
  //   // Check that the input is coercible to a String.
  //   checkUserInput(inputPath instanceof StringCoercible,
  //       "Cannot coerce path to a String type: " + inputPath.getExpression());
  //   final StringCoercible stringCoercible = (StringCoercible) inputPath;
  //
  //   // Create an expression for the new path.
  //   final String expression = expressionFromInput(input, NAME, input.getInput());
  //
  //   // Coerce the input to a String.
  //   return stringCoercible.asStringPath(expression, stringCoercible.getExpression());
  // }

}
