/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.functions;

import static au.csiro.clinsight.query.parsing.ParseResult.FhirType.DATE;
import static au.csiro.clinsight.query.parsing.ParseResult.FhirType.DATE_TIME;
import static au.csiro.clinsight.query.parsing.ParseResult.FhirType.INSTANT;

import au.csiro.clinsight.query.parsing.ParseResult;
import au.csiro.clinsight.query.parsing.ParseResult.FhirPathType;
import au.csiro.clinsight.query.parsing.ParseResult.FhirType;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;

/**
 * Describes the functionality of a group of functions that are used for extracting numeric
 * components from date types.
 *
 * @author John Grimes
 */
public class DateComponentFunction implements ExpressionFunction {

  private static final Map<String, String> functionsMap = new HashMap<String, String>() {{
    put("toSeconds", "second");
    put("toMinutes", "minute");
    put("toHours", "hour");
    put("dayOfMonth", "dayofmonth");
    put("dayOfWeek", "dayofweek");
    put("weekOfYear", "weekofyear");
    put("toMonthNumber", "month");
    put("toQuarter", "quarter");
    put("toYear", "year");
  }};
  private static final Set<FhirType> supportedTypes = new HashSet<FhirType>() {{
    add(INSTANT);
    add(DATE_TIME);
    add(DATE);
  }};
  private final String functionName;

  public DateComponentFunction(String functionName) {
    this.functionName = functionName;
  }

  @Nonnull
  @Override
  public ParseResult invoke(@Nonnull ExpressionFunctionInput input) {
    ParseResult inputResult = validateInput(input.getInput());

    ParseResult result = new ParseResult();
    result.setFunction(this);
    result.setFunctionInput(input);
    result.setFhirPath(input.getExpression());
    String newSqlExpression = functionsMap.get(functionName) + "(" + inputResult.getSql() + ")";
    result.setSql(newSqlExpression);
    result.setFhirPathType(FhirPathType.INTEGER);
    result.setFhirType(FhirType.INTEGER);
    result.setPrimitive(true);
    result.setSingular(inputResult.isSingular());
    result.getJoins().addAll(inputResult.getJoins());
    return result;
  }

  private ParseResult validateInput(ParseResult input) {
    if (input == null || input.getSql() == null || input.getSql().isEmpty()) {
      throw new InvalidRequestException(
          "Missing input expression for " + functionName + " function");
    }
    if (!supportedTypes.contains(input.getFhirType())) {
      throw new InvalidRequestException(
          "Input to " + functionName + " function must be DateTime");
    }
    return input;
  }

}
