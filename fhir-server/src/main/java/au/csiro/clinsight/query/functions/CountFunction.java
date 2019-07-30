/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.functions;

import static au.csiro.clinsight.fhir.definitions.PathTraversal.ResolvedElementType.PRIMITIVE;
import static au.csiro.clinsight.fhir.definitions.PathTraversal.ResolvedElementType.RESOURCE;

import au.csiro.clinsight.query.parsing.ParseResult;
import au.csiro.clinsight.query.parsing.ParseResult.FhirPathType;
import au.csiro.clinsight.query.parsing.ParseResult.FhirType;
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
  public ParseResult invoke(@Nonnull ExpressionFunctionInput input) {
    ParseResult inputResult = validateInput(input.getInput());
    validateArguments(input.getArguments());

    // If the inputResult is a resource, we infer the use of its ID as the field for counting.
    String sqlExpression = inputResult.getPathTraversal().getType() == RESOURCE
        ? inputResult.getSql() + ".id"
        : inputResult.getSql();

    ParseResult result = new ParseResult();
    result.setFunction(this);
    result.setFunctionInput(input);
    result.setFhirPath(input.getExpression());
    result.getJoins().addAll(inputResult.getJoins());
    // The count function maps to the function with the same name within Spark SQL.
    result.setSql("COUNT(DISTINCT " + sqlExpression + ")");
    // A count operation always results in a non-negative integer.
    result.setFhirPathType(FhirPathType.INTEGER);
    result.setFhirType(FhirType.UNSIGNED_INT);
    result.setPrimitive(true);
    result.setSingular(true);
    return result;
  }

  private ParseResult validateInput(@Nullable ParseResult input) {
    if (input == null || input.getSql() == null || input.getSql().isEmpty()) {
      throw new InvalidRequestException("Missing input expression for count function");
    }
    // We can't count an element that is not primitive.
    if (input.getPathTraversal().getType() != PRIMITIVE
        && input.getPathTraversal().getType() != RESOURCE) {
      throw new InvalidRequestException(
          "Input to count function must be of primitive or resource type: " + input.getFhirPath());
    }
    return input;
  }

  private void validateArguments(@Nonnull List<ParseResult> arguments) {
    if (!arguments.isEmpty()) {
      throw new InvalidRequestException("Count function does not accept arguments");
    }
  }

}
