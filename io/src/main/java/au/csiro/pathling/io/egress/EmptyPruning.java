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

package au.csiro.pathling.io.egress;

import jakarta.annotation.Nonnull;
import org.apache.spark.sql.Column;

/** Reduces to nothing what the output must omit rather than write as empty (FR-019). */
public final class EmptyPruning {

  private EmptyPruning() {}

  /**
   * Returns a structure that is null where every one of its fields is.
   *
   * @param fields the fields of the structure
   * @return the structure
   */
  @Nonnull
  public static Column structure(@Nonnull final Column[] fields) {
    throw new UnsupportedOperationException("Empty pruning is not implemented");
  }

  /**
   * Returns an array with its null elements removed, which is null where none survive.
   *
   * @param elements the elements of the array
   * @return the array
   */
  @Nonnull
  public static Column array(@Nonnull final Column elements) {
    throw new UnsupportedOperationException("Empty pruning is not implemented");
  }
}
