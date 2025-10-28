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

package au.csiro.pathling.sql;

import jakarta.annotation.Nonnull;
import lombok.experimental.UtilityClass;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;

/**
 * Pathling-specific SQL functions that extend Spark SQL functionality.
 * <p>
 * This interface provides utility functions for working with Spark SQL columns in the context of
 * FHIR data processing. These functions handle common operations like pruning annotations, safely
 * concatenating maps, and collecting maps during aggregation.
 */
@UtilityClass
public class SqlFunctions {

  private static final String FHIR_INSTANT_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
  
  /**
   * Formats a TIMESTAMP column to a string in FHIR instant format. Always returns UTC time as Spark
   * TIMESTAMP does not preserve the original timezone.
   *
   * @param col The column containing TIMESTAMP values to format
   * @return A new column with TIMESTAMP values formatted as strings in FHIR instant format
   */
  @Nonnull
  public static Column toFhirInstant(@Nonnull final Column col) {
    return functions.date_format(functions.to_utc_timestamp(col, functions.current_timezone()),
        FHIR_INSTANT_FORMAT);
  }
}
