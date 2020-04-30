/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.query.parsing;

import au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import org.hl7.fhir.r4.model.*;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * This class knows how to take HAPI FHIR-typed object and create a literal FHIRPath expression to
 * represent it.
 *
 * @author John Grimes
 */
public abstract class LiteralComposer {

  public static String getFhirPathForType(Type type) {
    FHIRDefinedType fhirType = FHIRDefinedType.fromCode(type.fhirType());
    FhirPathType fhirPathType = FhirPathType.forFhirTypeCode(fhirType);
    switch (fhirPathType) {
      case CODING:
        return composeCoding((Coding) type);
      case STRING:
        return composeString((StringType) type);
      case DATE_TIME:
        return composeDateTime((DateTimeType) type);
      case TIME:
        return composeTime((TimeType) type);
      case INTEGER:
        return composeInteger((IntegerType) type);
      case DECIMAL:
        return composeDecimal((DecimalType) type);
      case BOOLEAN:
        return composeBoolean((BooleanType) type);
      default:
        throw new RuntimeException(
            "Attempt to compose literal from unsupported data type: " + type.getClass());
    }
  }

  private static String composeCoding(Coding type) {
    return type.getVersion() == null
           ? type.getSystem() + "|" + type.getCode()
           : type.getSystem() + "|" + type.getVersion() + "|" + type.getCode();
  }

  private static String composeString(StringType type) {
    return "'" + type.getValue() + "'";
  }

  private static String composeDateTime(DateTimeType type) {
    Date date = type.getValue();
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
    return "@" + dateFormat.format(date);
  }

  private static String composeTime(TimeType type) {
    return "@T" + type.getValue();
  }

  private static String composeInteger(IntegerType type) {
    return type.getValueAsString();
  }

  private static String composeDecimal(DecimalType type) {
    return type.getValue().toPlainString();
  }

  private static String composeBoolean(BooleanType type) {
    return type.asStringValue();
  }

}
