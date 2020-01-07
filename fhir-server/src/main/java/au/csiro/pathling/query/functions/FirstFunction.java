/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.pathling.query.functions;

import static org.apache.spark.sql.functions.first;

import au.csiro.pathling.query.parsing.ParsedExpression;
import javax.annotation.Nonnull;

/**
 * This function allows the selection of only the first element of a collection.
 *
 * @author John Grimes
 * @see <a href="https://pathling.app/docs/fhirpath/functions.html#first">first</a>
 */
public class FirstFunction extends AbstractAggFunction {


  public FirstFunction() {
    super("first");
  }

  @Nonnull
  protected ParsedExpression invokeAgg(@Nonnull FunctionInput input) {
    // Use the version of `first` that ignores NULL values.
    return wrapSparkFunction(input, col -> first(col, true), true);
  }

  protected void validateInput(FunctionInput input) {
    validateNoArgumentInput(input);
  }
}
