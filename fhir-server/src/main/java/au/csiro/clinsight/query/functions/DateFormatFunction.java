/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.functions;

import au.csiro.clinsight.query.parsing.ParseResult;
import au.csiro.clinsight.query.parsing.ParseResult.FhirPathType;
import au.csiro.clinsight.query.parsing.ParseResult.FhirType;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.util.List;
import javax.annotation.Nonnull;

/**
 * Describes a function which allows for the creation of formatted strings based upon dates, using
 * the syntax from the Java SimpleDateFormat class.
 *
 * @author John Grimes
 */
public class DateFormatFunction implements ExpressionFunction {

  @Nonnull
  @Override
  public ParseResult invoke(@Nonnull ExpressionFunctionInput input) {
    ParseResult inputResult = validateInput(input.getInput());
    ParseResult argument = validateArgument(input.getArguments());

    ParseResult result = new ParseResult();
    result.setFunction(this);
    result.setFunctionInput(input);
    result.setFhirPath(input.getExpression());
    String newSqlExpression =
        "date_format(" + inputResult.getSql() + ", " + argument.getFhirPath() + ")";
    result.setSql(newSqlExpression);
    result.setFhirPathType(FhirPathType.STRING);
    result.setFhirType(FhirType.STRING);
    result.setPrimitive(true);
    result.setSingular(inputResult.isSingular());
    return result;
  }

  private ParseResult validateInput(ParseResult input) {
    if (input == null || input.getSql() == null || input.getSql().isEmpty()) {
      throw new InvalidRequestException("Missing input expression for dateFormat function");
    }
    if (input.getFhirPathType() != FhirPathType.DATE_TIME) {
      throw new InvalidRequestException(
          "Input to dateFormat function must be a DateTime: " + input.getFhirPath());
    }
    return input;
  }

  private ParseResult validateArgument(List<ParseResult> arguments) {
    if (arguments.size() != 1) {
      throw new InvalidRequestException("Must pass format argument to dateFormat function");
    }
    ParseResult argument = arguments.get(0);
    if (argument.getFhirPathType() != FhirPathType.STRING) {
      throw new InvalidRequestException(
          "Argument to dateFormat function must be a String: " + argument.getFhirPath());
    }
    return argument;
  }

}
