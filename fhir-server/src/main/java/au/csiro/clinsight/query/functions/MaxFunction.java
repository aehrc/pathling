/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.functions;

import au.csiro.clinsight.query.parsing.ParseResult;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A function for aggregating data based on finding the maximum value within the input set.
 *
 * @author John Grimes
 */
public class MaxFunction implements ExpressionFunction {

  @Nonnull
  @Override
  public ParseResult invoke(@Nonnull ExpressionFunctionInput input) {
    ParseResult inputResult = validateInput(input.getInput());
    validateArguments(input.getArguments());

    ParseResult result = new ParseResult();
    result.setFunction(this);
    result.setFunctionInput(input);
    result.setFhirPath(input.getExpression());
    // The max function maps to the function with the same name within Spark SQL.
    result.setSql("MAX(" + inputResult.getSql() + ")");
    // A max operation always returns the same type as the input.
    result.setFhirPathType(inputResult.getFhirPathType());
    result.setFhirType(inputResult.getFhirType());
    result.setPrimitive(inputResult.isPrimitive());
    result.setSingular(true);
    return result;
  }

  private ParseResult validateInput(@Nullable ParseResult input) {
    if (input == null || input.getSql() == null || input.getSql().isEmpty()) {
      throw new InvalidRequestException("Missing input expression for max function");
    }
    // We can't max an element that is not primitive.
    if (!input.isPrimitive()) {
      throw new InvalidRequestException(
          "Input to max function must be of primitive type: " + input.getFhirPath());
    }
    return input;
  }

  private void validateArguments(@Nonnull List<ParseResult> arguments) {
    if (!arguments.isEmpty()) {
      throw new InvalidRequestException("Max function does not accept arguments");
    }
  }

}
