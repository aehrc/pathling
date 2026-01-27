/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package au.csiro.pathling.sql.misc;

import au.csiro.pathling.fhirpath.FhirPathDateTime;
import au.csiro.pathling.sql.udf.SqlFunction1;
import jakarta.annotation.Nullable;
import java.io.Serial;
import java.sql.Timestamp;
import java.time.format.DateTimeParseException;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

/**
 * UDF that calculates the high boundary for a FHIR date/dateTime string or a Timestamp value.
 *
 * <p>This function handles partial dates and returns the latest possible timestamp for the given
 * precision level. Timestamps are treated dates with fractional seconds precision.
 *
 * @author John Grimes
 * @author Piotr Szul
 */
public class HighBoundaryForDateTime implements SqlFunction1<Object, Timestamp> {

  @Serial private static final long serialVersionUID = 413946955701564309L;

  /** The name of this UDF as registered in Spark. */
  public static final String FUNCTION_NAME = "high_boundary_for_date";

  /**
   * Returns the name of this UDF.
   *
   * @return the function name
   */
  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  /**
   * Returns the return type of this UDF.
   *
   * @return the Spark DataType for timestamp
   */
  @Override
  public DataType getReturnType() {
    return DataTypes.TimestampType;
  }

  /**
   * Calculates the high boundary timestamp for either a FHIR date/dateTime string or an Instant
   * value.
   *
   * @param input the date/dateTime string or Timestamp representing a dateTime or an Instant.
   * @return the high boundary timestamp, or null if input is null
   * @throws IllegalArgumentException if the date format is invalid or the input type is unsupported
   */
  @Nullable
  @Override
  public Timestamp call(@Nullable final Object input) throws Exception {
    if (input == null) {
      return null;
    } else if (input instanceof final Timestamp tst) {
      return tst;
    } else if (input instanceof final String s) {
      try {
        return Timestamp.from(FhirPathDateTime.parse(s).getUpperBoundary());
      } catch (final DateTimeParseException e) {
        throw new IllegalArgumentException("Invalid date/dateTime format: " + s, e);
      }
    } else {
      throw new IllegalArgumentException(
          "Unsupported input type: "
              + input.getClass().getName()
              + ", expected String or Timestamp");
    }
  }
}
