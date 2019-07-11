/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.functions;

import static au.csiro.clinsight.fhir.definitions.ResolvedElement.ResolvedElementType.PRIMITIVE;

import au.csiro.clinsight.TerminologyClient;
import au.csiro.clinsight.query.parsing.ParseResult;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.spark.sql.SparkSession;

/**
 * A function for aggregating data based on finding the maximum value within the input set.
 *
 * @author John Grimes
 */
public class MaxFunction implements ExpressionFunction {

  @Nonnull
  @Override
  public ParseResult invoke(@Nullable ParseResult input, @Nonnull List<ParseResult> arguments) {
    validateInput(input);
    validateArguments(arguments);
    // The max function maps to the function with the same name within Spark SQL.
    input.setPreAggregationExpression(input.getSqlExpression());
    input.setSqlExpression("MAX(" + input.getSqlExpression() + ")");
    // A max operation always returns the same type as the input.
    input.setElementTypeCode(input.getElementTypeCode());
    return input;
  }

  private void validateInput(@Nullable ParseResult input) {
    if (input == null || input.getSqlExpression() == null || input.getSqlExpression().isEmpty()) {
      throw new InvalidRequestException("Missing input expression for max function");
    }
    // We can't max an element that is not primitive.
    if (input.getElementType() != PRIMITIVE) {
      throw new InvalidRequestException(
          "Input to max function must be of primitive type: " + input.getExpression()
              + " (" + input.getElementTypeCode() + ")");
    }
  }

  private void validateArguments(@Nonnull List<ParseResult> arguments) {
    if (!arguments.isEmpty()) {
      throw new InvalidRequestException("Max function does not accept arguments");
    }
  }

  @Override
  public void setTerminologyClient(@Nonnull TerminologyClient terminologyClient) {
  }

  @Override
  public void setSparkSession(@Nonnull SparkSession spark) {
  }

  @Override
  public void setDatabaseName(@Nonnull String databaseName) {
  }

}
