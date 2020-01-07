/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.pathling.query.functions;

import javax.annotation.Nonnull;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import au.csiro.pathling.query.parsing.ParsedExpression;
import au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType;

/**
 * A function for aggregating data based on counting the number of rows within the result.
 *
 * @author John Grimes
 * @see <a href="https://pathling.app/docs/fhirpath/functions.html#count">count</a>
 */
public class CountFunction extends AbstractAggFunction {

  public CountFunction() {
    super("count");
  }

  @Nonnull
  protected ParsedExpression invokeAgg(@Nonnull FunctionInput input) {
    // Construct a new parse result.
    ParsedExpression result =
        wrapSparkFunction(input, org.apache.spark.sql.functions::count, false);
    result.setFhirPathType(FhirPathType.INTEGER);
    result.setFhirType(FHIRDefinedType.UNSIGNEDINT);
    result.setPrimitive(true);
    return result;
  }

  protected void validateInput(FunctionInput input) {
    validateNoArgumentInput(input);
  }

}
