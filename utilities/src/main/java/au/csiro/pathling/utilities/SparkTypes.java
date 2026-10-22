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

package au.csiro.pathling.utilities;

import jakarta.annotation.Nonnull;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;

/** Helpers for Spark SQL data types. */
public final class SparkTypes {

  private SparkTypes() {}

  /**
   * Returns the type of the values a column carries, unwrapping one level of array where it
   * repeats.
   *
   * @param type the type of the column
   * @return the element type of an array, or the type itself where it is not an array
   */
  @Nonnull
  public static DataType elementTypeOf(@Nonnull final DataType type) {
    return type instanceof final ArrayType array ? array.elementType() : type;
  }
}
