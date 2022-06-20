/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.sql;

import au.csiro.pathling.fhirpath.encoding.CodingEncoding;
import au.csiro.pathling.fhirpath.literal.CodingLiteral;
import javax.annotation.Nullable;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF1;
import org.hl7.fhir.r4.model.Coding;

/**
 * Spark UDF to convert a Coding struct to a valid Coding literal string.
 *
 * @author John Grimes
 */
public class CodingToLiteral implements UDF1<Row, String> {

  /**
   * The name of this function when used within SQL.
   */
  public static final String FUNCTION_NAME = "coding_to_literal";

  private static final long serialVersionUID = -6274263255779613070L;

  @Override
  @Nullable
  public String call(@Nullable final Row row) {
    if (row == null) {
      return null;
    }
    final Coding coding = CodingEncoding.decode(row);
    return CodingLiteral.toLiteral(coding);
  }

}
