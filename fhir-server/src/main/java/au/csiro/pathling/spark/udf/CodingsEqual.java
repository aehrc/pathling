/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.spark.udf;

import javax.annotation.Nullable;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF2;

/**
 * @author John Grimes
 */
public class CodingsEqual implements UDF2<Row, Row, Boolean> {

  private static final long serialVersionUID = -8947535869559021947L;

  @Nullable
  @Override
  public Boolean call(@Nullable final Row left, @Nullable final Row right) {
    // If either operand is null, the result will be null.
    if (left == null || right == null) {
      return null;
    }

    final String leftSystem = left.getString(left.fieldIndex("system"));
    final String leftCode = left.getString(left.fieldIndex("code"));
    final String leftVersion = left.getString(left.fieldIndex("version"));
    final String rightSystem = right.getString(right.fieldIndex("system"));
    final String rightCode = right.getString(right.fieldIndex("code"));
    final String rightVersion = right.getString(right.fieldIndex("version"));

    final boolean eitherCodingIsIncomplete =
        leftSystem == null || leftCode == null || rightSystem == null || rightCode == null;
    if (eitherCodingIsIncomplete) {
      return null;
    }

    final boolean eitherCodingIsMissingVersion = leftVersion == null || rightVersion == null;
    final boolean versionAgnosticTest =
        leftSystem.equals(rightSystem) && leftCode.equals(rightCode);
    if (eitherCodingIsMissingVersion) {
      return versionAgnosticTest;
    } else {
      return versionAgnosticTest && leftVersion.equals(rightVersion);
    }
  }

}
