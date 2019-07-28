/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.functions;

import au.csiro.clinsight.query.parsing.ExpressionParserContext;
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
  public ParseResult invoke(@Nonnull String expression, @Nullable ParseResult input,
      @Nonnull List<ParseResult> arguments) {
    validateInput(input);
    validateArguments(arguments);

    ParseResult result = new ParseResult();
    result.setFhirPath(expression);
    // The max function maps to the function with the same name within Spark SQL.
    result.setSql("MAX(" + input.getSql() + ")");
    // A max operation always returns the same type as the input.
    result.setResultType(input.getResultType());
    result.setPrimitive(input.isPrimitive());
    result.setSingular(true);
    return result;
  }

  private void validateInput(@Nullable ParseResult input) {
    if (input == null || input.getSql() == null || input.getSql().isEmpty()) {
      throw new InvalidRequestException("Missing input expression for max function");
    }
    // We can't max an element that is not primitive.
    if (!input.isPrimitive()) {
      throw new InvalidRequestException(
          "Input to max function must be of primitive type: " + input.getFhirPath());
    }
  }

  private void validateArguments(@Nonnull List<ParseResult> arguments) {
    if (!arguments.isEmpty()) {
      throw new InvalidRequestException("Max function does not accept arguments");
    }
  }

  @Override
  public void setContext(@Nonnull ExpressionParserContext context) {
  }

}
