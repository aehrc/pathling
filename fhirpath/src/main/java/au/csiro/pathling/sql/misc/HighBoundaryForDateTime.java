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

import au.csiro.pathling.fhirpath.FhirpathDateTime;
import au.csiro.pathling.sql.udf.SqlFunction1;
import jakarta.annotation.Nullable;
import java.io.Serial;
import java.sql.Timestamp;
import java.time.format.DateTimeParseException;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

/**
 * UDF that calculates the high boundary for a FHIR date/dateTime string.
 * <p>
 * This function handles partial dates and returns the latest possible timestamp for the given
 * precision level.
 *
 * @author John Grimes
 */
public class HighBoundaryForDateTime implements SqlFunction1<String, Timestamp> {

  @Serial
  private static final long serialVersionUID = 413946955701564309L;

  /**
   * The name of this UDF as registered in Spark.
   */
  public static final String FUNCTION_NAME = "high_boundary_for_date";

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public DataType getReturnType() {
    return DataTypes.TimestampType;
  }

  @Nullable
  @Override
  public Timestamp call(@Nullable final String s) throws Exception {
    if (s == null) {
      return null;
    }
    try {
      return Timestamp.from(FhirpathDateTime.parse(s).getUpperBoundary());
    } catch (final DateTimeParseException e) {
      throw new IllegalArgumentException("Invalid date/dateTime format: " + s, e);
    }
  }
}
