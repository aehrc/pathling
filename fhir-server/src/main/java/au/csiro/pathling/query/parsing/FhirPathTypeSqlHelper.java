/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.pathling.query.parsing;

import au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType;
import java.util.function.BiFunction;
import org.apache.spark.sql.Column;

public interface FhirPathTypeSqlHelper {

  Column getLiteralColumn(ParsedExpression expression);

  BiFunction<Column, Column, Column> getEquality();

  static FhirPathTypeSqlHelper forType(FhirPathType pathType) {
    if (pathType.isPrimitive()) {
      return PrimitiveFhirPathTypeSqlHelper.INSTANCE;
    } else if (FhirPathType.CODING.equals(pathType)) {
      return CodingFhirPathTypeSqlHelper.INSTANCE;
    } else {
      throw new IllegalArgumentException(
          "Cannot construct SQLHelper for FhirPathType: " + pathType);
    }
  }
}
