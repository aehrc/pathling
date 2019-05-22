/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query;

import au.csiro.clinsight.query.functions.*;
import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.hl7.fhir.dstu3.model.*;

/**
 * Mappings between data types and functions within FHIR and Apache Spark.
 *
 * @author John Grimes
 */
public abstract class Mappings {

  // Maps a FHIR type code to the class that can be used to populate a value into a resource using
  // HAPI.
  private static final Map<String, Class> fhirTypeToFhirClass = new HashMap<String, Class>() {{
    put("decimal", DecimalType.class);
    put("markdown", MarkdownType.class);
    put("id", IdType.class);
    put("dateTime", DateTimeType.class);
    put("time", TimeType.class);
    put("date", DateType.class);
    put("code", CodeType.class);
    put("string", StringType.class);
    put("uri", UriType.class);
    put("oid", OidType.class);
    put("integer", IntegerType.class);
    put("unsignedInt", UnsignedIntType.class);
    put("positiveInt", PositiveIntType.class);
    put("boolean", BooleanType.class);
    put("instant", InstantType.class);
  }};

  // Maps a FHIR type code to a Java class that can be used to receive the value extracted from the
  // Row in the Spark Dataset.
  private static final Map<String, Class> fhirTypeToJavaClass = new HashMap<String, Class>() {{
    put("decimal", BigDecimal.class);
    put("markdown", String.class);
    put("id", String.class);
    put("dateTime", String.class);
    put("time", String.class);
    put("date", String.class);
    put("code", String.class);
    put("string", String.class);
    put("uri", String.class);
    put("oid", String.class);
    put("integer", int.class);
    put("unsignedInt", Long.class);
    put("positiveInt", Long.class);
    put("boolean", Boolean.class);
    put("instant", Date.class);
  }};

  // Maps supported aggregate FHIRPath functions to the equivalent functions within Spark SQL.
  private static final Map<String, ExpressionFunction> funcToClass = new HashMap<String, ExpressionFunction>() {{
    put("count", new CountFunction());
    put("max", new MaxFunction());
    put("resolve", new ResolveFunction());
    put("reverseResolve", new ReverseResolveFunction());
    put("inValueSet", new InValueSetFunction());
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

  static Class getFhirClass(String fhirTypeCode) {
    return fhirTypeToFhirClass.get(fhirTypeCode);
  }

  static Class getJavaClass(String fhirTypeCode) {
    return fhirTypeToJavaClass.get(fhirTypeCode);
  }

  public static ExpressionFunction getFunction(String functionName) {
    return funcToClass.get(functionName);
  }

}
