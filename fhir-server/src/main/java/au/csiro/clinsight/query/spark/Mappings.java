/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.spark;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.hl7.fhir.dstu3.model.BooleanType;
import org.hl7.fhir.dstu3.model.CodeType;
import org.hl7.fhir.dstu3.model.DateTimeType;
import org.hl7.fhir.dstu3.model.DateType;
import org.hl7.fhir.dstu3.model.DecimalType;
import org.hl7.fhir.dstu3.model.IdType;
import org.hl7.fhir.dstu3.model.InstantType;
import org.hl7.fhir.dstu3.model.IntegerType;
import org.hl7.fhir.dstu3.model.MarkdownType;
import org.hl7.fhir.dstu3.model.OidType;
import org.hl7.fhir.dstu3.model.PositiveIntType;
import org.hl7.fhir.dstu3.model.StringType;
import org.hl7.fhir.dstu3.model.TimeType;
import org.hl7.fhir.dstu3.model.UnsignedIntType;
import org.hl7.fhir.dstu3.model.UriType;

/**
 * Mappings between data types and functions within FHIR and Apache Spark.
 *
 * @author John Grimes
 */
abstract class Mappings {

  // A list of FHIR types that are permitted to be used within queries.
  //
  // This should be a subset of com.cerner.bunsen.stu3.Stu3DataTypeMappings within Bunsen, in order
  // to ensure that we can reliably extract values from Bunsen-encoded Datasets.
  private static final List<String> supportedFhirTypes = Arrays.asList(
      "decimal",
      "markdown",
      "id",
      "dateTime",
      "time",
      "date",
      "code",
      "string",
      "uri",
      "oid",
      "integer",
      "unsignedInt",
      "positiveInt",
      "boolean",
      "instant"
  );

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
  private static final Map<String, String> funcToSpark = new HashMap<String, String>() {{
    put("count", "COUNT");
  }};

  // Maps aggregate FHIRPath functions to the type that they return, as a FHIR type code.
  private static final Map<String, String> funcToDataType = new HashMap<String, String>() {{
    put("count", "unsignedInt");
  }};

  static boolean isFhirTypeSupported(String fhirTypeCode) {
    return supportedFhirTypes.contains(fhirTypeCode);
  }

  static Class getFhirClass(String fhirTypeCode) {
    return fhirTypeToFhirClass.get(fhirTypeCode);
  }

  static Class getJavaClass(String fhirTypeCode) {
    return fhirTypeToJavaClass.get(fhirTypeCode);
  }

  static String translateFunctionToSpark(String functionName) {
    return funcToSpark.get(functionName);
  }

  static String getFhirTypeForFunction(String functionName) {
    return funcToDataType.get(functionName);
  }

}
