/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.query.functions;

import au.csiro.pathling.query.parsing.ParsedExpression;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;

/**
 * A function that is supported for use within FHIRPath expressions. The input is the expression
 * that invoked the function (i.e. on the left hand side of the period), and the arguments are
 * passed within parentheses.
 *
 * @author John Grimes
 */
public interface Function {

  // Maps FHIRPath functions to the equivalent functions within Spark SQL.
  Map<String, Function> funcToObject = new HashMap<String, Function>() {{
    put("where", new WhereFunction());
    put("count", new CountFunction());
    put("first", new FirstFunction());
    put("memberOf", new MemberOfFunction());
    put("subsumes", new SubsumesFunction());
    put("subsumedBy", new SubsumesFunction(true));
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
    put("resolve", new ResolveFunction());
    put("reverseResolve", new ReverseResolveFunction());
    put("ofType", new OfTypeFunction());
  }};

  static Function getFunction(String functionName) {
    return funcToObject.get(functionName);
  }

  @Nonnull
  ParsedExpression invoke(@Nonnull FunctionInput input);

}
