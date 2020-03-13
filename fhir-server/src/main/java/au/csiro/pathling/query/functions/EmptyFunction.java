/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.query.functions;

import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.when;

import au.csiro.pathling.query.parsing.ParsedExpression;
import javax.annotation.Nonnull;

/**
 * This function returns true if the input collection is empty.
 *
 * @author John Grimes
 * @see <a href="https://pathling.app/docs/fhirpath/functions.html#empty">first</a>
 */
public class EmptyFunction extends AbstractAggFunction {

  /* TODO: Add tests for empty function. */

  public EmptyFunction() {
    super("empty");
  }

  @Nonnull
  protected ParsedExpression invokeAgg(@Nonnull FunctionInput input) {
    // "Empty" means that the group contains only null values.
    return wrapSparkFunction(input, col -> when(max(col).isNull(), true).otherwise(false), true);
  }

  protected void validateInput(FunctionInput input) {
    validateNoArgumentInput(input);
  }
}
