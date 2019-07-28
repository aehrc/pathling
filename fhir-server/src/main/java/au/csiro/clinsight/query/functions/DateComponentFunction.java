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
  public ParseResult invoke(@Nonnull String expression, @Nullable ParseResult input,
      @Nonnull List<ParseResult> arguments) {
    validateInput(input);

    ParseResult result = new ParseResult();
    result.setFhirPath(expression);
    String newSqlExpression = functionsMap.get(functionName) + "(" + input.getSql() + ")";
    result.setSql(newSqlExpression);
    result.setResultType(INTEGER);
    result.setPrimitive(true);
    result.setSingular(input.isSingular());
    return result;
  }

  private void validateInput(ParseResult input) {
    if (input == null || input.getSql() == null || input.getSql().isEmpty()) {
      throw new InvalidRequestException(
          "Missing input expression for " + functionName + " function");
    }
    if (input.getPathTraversal().getType() != PRIMITIVE || !supportedTypes
        .contains(input.getPathTraversal().getElementDefinition().getTypeCode())) {
      throw new InvalidRequestException(
          "Input to " + functionName + " function must be DateTime");
    }
  }

  @Override
  public void setContext(@Nonnull ExpressionParserContext context) {
  }

}
