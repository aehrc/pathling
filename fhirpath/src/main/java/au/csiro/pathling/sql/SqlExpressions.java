/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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
import org.apache.spark.sql.Column;

/**
 * Pathling specific SQL functions.
 */
public interface SqlExpressions {

  /**
   * A function that removes all fields starting with '_' (underscore) from struct values. Other
   * types of values are not affected.
   *
   * @param col the column to apply the function to
   * @return the column transformed by the function
   */

  @Nonnull
  static Column pruneSyntheticFields(@Nonnull final Column col) {
    return new Column(new PruneSyntheticFields(col.expr()));
  }
}
