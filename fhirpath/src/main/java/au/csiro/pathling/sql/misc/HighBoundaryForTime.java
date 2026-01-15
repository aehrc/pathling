/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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

import au.csiro.pathling.fhirpath.FhirPathTime;
import au.csiro.pathling.sql.udf.SqlFunction1;
import jakarta.annotation.Nullable;
import java.io.Serial;
import java.sql.Timestamp;
import java.time.format.DateTimeParseException;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

/**
 * UDF that calculates the high boundary for a FHIR time string.
 *
 * <p>This function handles partial times and returns the latest possible timestamp for the given
 * precision level.
 *
 * @author Piotr Szul
 */
public class HighBoundaryForTime implements SqlFunction1<String, Timestamp> {

  @Serial private static final long serialVersionUID = 413946955701564310L;

  /** The name of this UDF as registered in Spark. */
  public static final String FUNCTION_NAME = "high_boundary_for_time";

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
   * Calculates the high boundary timestamp for a FHIR time string.
   *
   * @param input the time string (e.g., @T12:00).
   * @return the high boundary timestamp, or null if input is null
   * @throws IllegalArgumentException if the time format is invalid
   */
  @Nullable
  @Override
  public Timestamp call(@Nullable final String input) throws Exception {
    if (input == null) {
      return null;
    }
    try {
      return Timestamp.from(FhirPathTime.parse(input).getUpperBoundary());
    } catch (final DateTimeParseException e) {
      throw new IllegalArgumentException("Invalid time format: " + input, e);
    }
  }
}
