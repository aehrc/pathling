/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.query.parsing;

import static org.apache.spark.sql.functions.lit;

import java.util.function.BiFunction;
import org.apache.spark.sql.Column;

public class PrimitiveFhirPathTypeSqlHelper implements FhirPathTypeSqlHelper {

  public static final PrimitiveFhirPathTypeSqlHelper INSTANCE =
      new PrimitiveFhirPathTypeSqlHelper();

  @Override
  public Column getLiteralColumn(ParsedExpression expression) {
    return lit(expression.getJavaLiteralValue());
  }

  @Override
  public BiFunction<Column, Column, Column> getEquality() {
    return Column::equalTo;
  }

}
