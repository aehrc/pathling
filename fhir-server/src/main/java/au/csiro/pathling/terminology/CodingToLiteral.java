/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.terminology;

import au.csiro.pathling.fhirpath.encoding.CodingEncoding;
import au.csiro.pathling.fhirpath.literal.CodingLiteral;
import au.csiro.pathling.sql.udf.SqlFunction1;
import javax.annotation.Nullable;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Coding;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * Spark UDF to convert a Coding struct to a valid Coding literal string.
 *
 * @author John Grimes
 */
@Component
@Profile("core")
public class CodingToLiteral implements SqlFunction1<Row, String> {

  /**
   * The name of this function when used within SQL.
   */
  public static final String FUNCTION_NAME = "coding_to_literal";

  private static final long serialVersionUID = -6274263255779613070L;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public DataType getReturnType() {
    return DataTypes.StringType;
  }

  @Override
  @Nullable
  public String call(@Nullable final Row row) throws Exception {
    if (row == null) {
      return null;
    }
    final Coding coding = CodingEncoding.decode(row);
    return CodingLiteral.toLiteral(coding);
  }

}
