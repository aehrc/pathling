/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.spark;

import au.csiro.clinsight.TerminologyClient;
import au.csiro.clinsight.fhir.ResolvedElement.ResolvedElementType;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.spark.sql.SparkSession;

/**
 * @author John Grimes
 */
public class DistinctFunction implements ExpressionFunction {

  @Nonnull
  @Override
  public ParseResult invoke(@Nullable ParseResult input, @Nonnull List<ParseResult> arguments) {
    if (input == null || input.getSqlExpression() == null || input.getSqlExpression().isEmpty()) {
      throw new InvalidRequestException("Missing input expression for distinct function");
    }
    if (!arguments.isEmpty()) {
      throw new InvalidRequestException("Distinct function does not accept arguments");
    }
    if (input.getElementType() != ResolvedElementType.PRIMITIVE) {
      throw new InvalidRequestException(
          "Input to distinct function must be of primitive type: " + input.getExpression()
              + " (" + input.getElementTypeCode() + ")");
    }
    input.setSqlExpression("DISTINCT " + input.getSqlExpression());
    return input;
  }

  @Override
  public void setTerminologyClient(@Nonnull TerminologyClient terminologyClient) {
  }

  @Override
  public void setSparkSession(@Nonnull SparkSession spark) {
  }

}
