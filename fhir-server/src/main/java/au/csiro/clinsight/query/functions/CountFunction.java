/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.functions;

import static au.csiro.clinsight.fhir.definitions.PathTraversal.ResolvedElementType.PRIMITIVE;
import static au.csiro.clinsight.fhir.definitions.PathTraversal.ResolvedElementType.RESOURCE;

import au.csiro.clinsight.query.parsing.ExpressionParserContext;
import au.csiro.clinsight.query.parsing.ParseResult;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A function for aggregating data based on counting the number of rows within the result.
 *
 * @author John Grimes
 */
public class CountFunction implements ExpressionFunction {

  @Nonnull
  @Override
  public ParseResult invoke(@Nullable ParseResult input, @Nonnull List<ParseResult> arguments) {
    validateInput(input);
    validateArguments(arguments);
    // If the input is a resource, we infer the use of its ID as the field for counting.
    String sqlExpression = input.getElementType() == RESOURCE
        ? input.getSql() + ".id"
        : input.getSql();
    // The count function maps to the function with the same name within Spark SQL.
    input.setSql("COUNT(DISTINCT " + sqlExpression + ")");
    // A count operation always results in a non-negative integer.
    input.setElementTypeCode("unsignedInt");
    return input;
  }

  private void validateInput(@Nullable ParseResult input) {
    if (input == null || input.getSql() == null || input.getSql().isEmpty()) {
      throw new InvalidRequestException("Missing input expression for count function");
    }
    // We can't count an element that is not primitive.
    if (input.getElementType() != PRIMITIVE && input.getElementType() != RESOURCE) {
      throw new InvalidRequestException(
          "Input to count function must be of primitive or resource type: " + input.getFhirPath()
              + " (" + input.getElementTypeCode() + ")");
    }
  }

  private void validateArguments(@Nonnull List<ParseResult> arguments) {
    if (!arguments.isEmpty()) {
      throw new InvalidRequestException("Count function does not accept arguments");
    }
  }

  @Override
  public void setContext(@Nonnull ExpressionParserContext context) {
  }

}
