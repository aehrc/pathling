package au.csiro.clinsight.test;

import au.csiro.clinsight.query.parsing.ParsedExpression;

public class Assertions {
	public static ParsedExpressionAssert assertThat(ParsedExpression parsedExpression) {
		return new ParsedExpressionAssert(parsedExpression);
	}
}
