/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.pathling.test;

import au.csiro.pathling.query.parsing.ParsedExpression;
import au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType;
import java.math.BigDecimal;
import org.hl7.fhir.r4.model.*;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * @author John Grimes
 */
public class PrimitiveExpressionBuilder extends ExpressionBuilder {

  public PrimitiveExpressionBuilder(FHIRDefinedType primitiveType, FhirPathType fhirPathType) {
    super();
    expression.setFhirType(primitiveType);
    expression.setFhirPathType(fhirPathType);
    expression.setPrimitive(true);
  }

  public static ParsedExpression literalBoolean(boolean value) {
    return literal(String.valueOf(value),
        FhirPathType.BOOLEAN,
        FHIRDefinedType.BOOLEAN,
        new BooleanType(value));
  }

  public static ParsedExpression literalString(String value) {
    return literal("'" + value + "'",
        FhirPathType.STRING,
        FHIRDefinedType.STRING,
        new StringType(value));
  }

  public static ParsedExpression literalInteger(Integer value) {
    return literal(value.toString(),
        FhirPathType.INTEGER,
        FHIRDefinedType.INTEGER,
        new IntegerType(value));
  }

  public static ParsedExpression literalDecimal(BigDecimal value) {
    return literal(value.toString(),
        FhirPathType.DECIMAL,
        FHIRDefinedType.DECIMAL,
        new DecimalType(value));
  }

  public static ParsedExpression literalDateTime(String value) {
    return literal("@" + value,
        FhirPathType.DATE_TIME,
        FHIRDefinedType.DATETIME,
        new StringType(value));
  }

  public static ParsedExpression literalDate(String value) {
    return literal("@" + value,
        FhirPathType.DATE,
        FHIRDefinedType.DATE,
        new StringType(value));
  }

  public static ParsedExpression literalCoding(Coding value) {
    String fhirPath = value.getVersion() == null
        ? value.getSystem() + "|" + value.getCode()
        : value.getSystem() + "|" + value.getVersion() + "|" + value.getCode();
    return literal(fhirPath,
        FhirPathType.CODING,
        FHIRDefinedType.CODING,
        value);
  }

  public static ParsedExpression literalCoding(String system, String version, String code) {
    Coding value = new Coding(system, code, null);
    if (version != null) {
      value.setVersion(version);
    }
    return literalCoding(value);
  }

  private static ParsedExpression literal(String fhirPath, FhirPathType fhirPathType,
      FHIRDefinedType fhirType, Type value) {
    ParsedExpression expression = new ParsedExpression();
    expression.setSingular(true);
    expression.setPrimitive(true);
    expression.setFhirPath(fhirPath);
    expression.setFhirPathType(fhirPathType);
    expression.setFhirType(fhirType);
    expression.setLiteralValue(value);
    return expression;
  }
}
