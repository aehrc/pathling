/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query;

import au.csiro.clinsight.query.functions.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Mappings between data types and functions within FHIR and Apache Spark.
 *
 * @author John Grimes
 */
public abstract class Mappings {

  // Maps supported aggregate FHIRPath functions to the equivalent functions within Spark SQL.
  private static final Map<String, ExpressionFunction> funcToClass = new HashMap<String, ExpressionFunction>() {{
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

  public static ExpressionFunction getFunction(String functionName) {
    return funcToClass.get(functionName);
  }

}
