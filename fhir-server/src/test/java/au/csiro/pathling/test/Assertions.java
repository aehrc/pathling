package au.csiro.pathling.test;

import au.csiro.pathling.query.parsing.ParsedExpression;

public class Assertions {

  public static ParsedExpressionAssert assertThat(ParsedExpression parsedExpression) {
    return new ParsedExpressionAssert(parsedExpression);
  }
}
