/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.query.functions;

import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.when;

import au.csiro.pathling.query.parsing.ParsedExpression;
import au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import javax.annotation.Nonnull;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * This function returns true if the input collection is empty.
 *
 * @author John Grimes
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#empty">first</a>
 */
public class EmptyFunction extends AbstractAggregateFunction {

  public EmptyFunction() {
    super("empty");
  }

  @Nonnull
  protected ParsedExpression aggregate(@Nonnull FunctionInput input) {
    // "Empty" means that the group contains only null values.
    ParsedExpression result = wrapSparkFunction(input,
        col -> when(max(col).isNull(), true).otherwise(false), true);
    result.clearDefinition();
    result.setFhirPathType(FhirPathType.BOOLEAN);
    result.setFhirType(FHIRDefinedType.BOOLEAN);
    result.setPrimitive(true);
    return result;
  }

  protected void validateInput(FunctionInput input) {
    FunctionValidations.validateNoArgumentInput(functionName, input);
    ParsedExpression inputExpression = input.getInput();
    if (inputExpression.isSingular()) {
      throw new InvalidRequestException(
          "Input to empty function must not be singular: " + inputExpression.getFhirPath());
    }
  }
}
