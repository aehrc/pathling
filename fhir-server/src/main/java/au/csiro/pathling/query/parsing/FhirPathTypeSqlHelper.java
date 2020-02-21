/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
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
