/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.query.functions;

import au.csiro.pathling.query.parsing.ParsedExpression;
import au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType;
import javax.annotation.Nonnull;
import org.apache.spark.sql.functions;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * A function for aggregating data based on counting the number of rows within the result.
 *
 * @author John Grimes
 * @see <a href="https://pathling.app/docs/fhirpath/functions.html#count">count</a>
 */
public class CountFunction extends AbstractAggregateFunction {

  public CountFunction() {
    super("count");
  }

  @Nonnull
  protected ParsedExpression aggregate(@Nonnull FunctionInput input) {
    // Construct a new parse result.
    ParsedExpression result = wrapSparkFunction(input, functions::count, false);
    result.setFhirPathType(FhirPathType.INTEGER);
    result.setFhirType(FHIRDefinedType.UNSIGNEDINT);
    result.setPrimitive(true);
    return result;
  }

  protected void validateInput(FunctionInput input) {
    FunctionValidations.validateNoArgumentInput(functionName, input);
  }

}
