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

import jakarta.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;

/**
 * An interface that defines methods for comparing two {@link Column} objects for equality and
 * inequality.
 *
 * @author Piotr Szul
 */
public interface ColumnEquality {

  /**
   * Creates an equality comparison between two columns.
   *
   * @param left the left column
   * @param right the right column
   * @return a column representing the equality comparison
   */
  @Nonnull
  Column equalsTo(@Nonnull Column left, @Nonnull Column right);

  /**
   * Creates an inequality comparison between two columns.
   *
   * @param left the left column
   * @param right the right column
   * @return a column representing the inequality comparison
   */
  @Nonnull
  default Column notEqual(@Nonnull final Column left, @Nonnull final Column right) {
    return functions.not(equalsTo(left, right));
  }
}
