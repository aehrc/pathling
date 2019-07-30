/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.functions;

import au.csiro.clinsight.query.parsing.ParseResult;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;

/**
 * A function that is supported for use within FHIRPath expressions. The input is the expression
 * that invoked the function (i.e. on the left hand side of the period), and the arguments are
 * passed within parentheses.
 *
 * A function can optionally accept a terminology server and/or a Spark Session in order to augment
 * its behaviour. Functions that don't need these should just implement do-nothing methods.
 *
 * @author John Grimes
 */
public interface ExpressionFunction {

  // Maps supported aggregate FHIRPath functions to the equivalent functions within Spark SQL.
  Map<String, ExpressionFunction> funcToClass = new HashMap<String, ExpressionFunction>() {{
    put("where", new WhereFunction());
    put("count", new CountFunction());
    put("max", new MaxFunction());
    put("resolve", new ResolveFunction());
    put("reverseResolve", new ReverseResolveFunction());
    put("memberOf", new MemberOfFunction());
    put("dateFormat", new DateFormatFunction());
    put("toSeconds", new DateComponentFunction("toSeconds"));
    put("toMinutes", new DateComponentFunction("toMinutes"));
    put("toHours", new DateComponentFunction("toHours"));
    put("dayOfMonth", new DateComponentFunction("dayOfMonth"));
    put("dayOfWeek", new DateComponentFunction("dayOfWeek"));
    put("weekOfYear", new DateComponentFunction("weekOfYear"));
    put("toMonthNumber", new DateComponentFunction("toMonthNumber"));
    put("toQuarter", new DateComponentFunction("toQuarter"));
    put("toYear", new DateComponentFunction("toYear"));
  }};

  static ExpressionFunction getFunction(String functionName) {
    return funcToClass.get(functionName);
  }

  @Nonnull
  ParseResult invoke(@Nonnull ExpressionFunctionInput input);

}
