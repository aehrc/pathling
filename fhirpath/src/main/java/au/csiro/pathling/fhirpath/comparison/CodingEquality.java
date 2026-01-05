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

package au.csiro.pathling.fhirpath.comparison;

import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;

import jakarta.annotation.Nonnull;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Column;

/**
 * Implementation of a Spark SQL equality for the Coding type.
 *
 * @author Piotr Szul
 */
public class CodingEquality implements ElementWiseEquality {

  @Nonnull private static final CodingEquality INSTANCE = new CodingEquality();

  /**
   * Gets the singleton instance of the comparator.
   *
   * @return the singleton instance
   */
  @Nonnull
  public static CodingEquality getInstance() {
    return INSTANCE;
  }

  private static final List<String> EQUALITY_COLUMNS =
      Arrays.asList("system", "code", "version", "display", "userSelected");

  @Nonnull
  @Override
  public Column equalsTo(@Nonnull final Column left, @Nonnull final Column right) {
    return when(left.isNull().or(right.isNull()), lit(null))
        .otherwise(
            EQUALITY_COLUMNS.stream()
                .map(f -> left.getField(f).eqNullSafe(right.getField(f)))
                .reduce(Column::and)
                .orElseThrow(() -> new AssertionError("No fields to compare")));
  }
}
