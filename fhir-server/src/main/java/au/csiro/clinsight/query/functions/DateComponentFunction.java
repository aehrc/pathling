/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.functions;

import static au.csiro.clinsight.fhir.definitions.PathTraversal.ResolvedElementType.PRIMITIVE;
import static au.csiro.clinsight.query.parsing.ParseResult.ParseResultType.INTEGER;

import au.csiro.clinsight.query.parsing.ExpressionParserContext;
import au.csiro.clinsight.query.parsing.ParseResult;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.util.*;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
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
  private static final Set<String> supportedTypes = new HashSet<String>() {{
    add("instant");
    add("dateTime");
    add("date");
  }};
  private final String functionName;

  public DateComponentFunction(String functionName) {
    this.functionName = functionName;
  }

  @Nonnull
  @Override
  public ParseResult invoke(@Nullable ParseResult input, @Nonnull List<ParseResult> arguments) {
    validateInput(input);
    String newSqlExpression = functionsMap.get(functionName) + "(" + input.getSql() + ")";
    input.setSql(newSqlExpression);
    input.setResultType(INTEGER);
    input.setElementType(null);
    input.setElementTypeCode(null);
    return input;
  }

  private void validateInput(ParseResult input) {
    if (input == null || input.getSql() == null || input.getSql().isEmpty()) {
      throw new InvalidRequestException(
          "Missing input expression for " + functionName + " function");
    }
    if (input.getElementType() != PRIMITIVE || !supportedTypes
        .contains(input.getElementTypeCode())) {
      throw new InvalidRequestException(
          "Input to " + functionName + " function must be instant, dateTime or date");
    }
  }

  @Override
  public void setContext(@Nonnull ExpressionParserContext context) {
  }

}
