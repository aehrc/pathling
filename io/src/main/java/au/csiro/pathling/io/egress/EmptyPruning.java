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
import java.util.stream.Stream;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;

/**
 * Reduces to nothing what the output must omit rather than write as empty (FR-019).
 *
 * <p>The JSON writer leaves out a field that is null, which is most of what FR-019 asks for and is
 * why an absent element needs nothing done to it. Two shapes it does not leave out: a structure
 * that is present and whose every field is null, which it writes as an empty object; and an array
 * whose elements are null, which it writes as an array of nulls. Neither is FHIR — an element that
 * has nothing in it is an element that is not there — and neither can be told apart afterwards from
 * an element the source really carried.
 *
 * <p>Both are answered by making the value null before the writer sees it, and both have to be
 * applied from the leaves upwards: a structure becomes empty because the structures inside it did,
 * and an array becomes empty because its elements did.
 */
public final class EmptyPruning {

  private EmptyPruning() {}

  /**
   * Returns a structure that is null where every one of its fields is.
   *
   * @param fields the fields of the structure, named by their aliases
   * @return the structure
   */
  @Nonnull
  public static Column structure(@Nonnull final Column[] fields) {
    final Column populated =
        Stream.of(fields)
            .map(Column::isNotNull)
            .reduce(Column::or)
            .orElseThrow(() -> new IllegalArgumentException("A structure has at least one field"));
    return functions.when(populated, functions.struct(fields));
  }

  /**
   * Returns an array with its null elements removed, which is null where none survive.
   *
   * <p>An element is dropped rather than the array being emptied wholesale, because an array of
   * three values one of which pruned away is an array of two values, not an absent element.
   *
   * @param elements the elements of the array
   * @return the array
   */
  @Nonnull
  public static Column array(@Nonnull final Column elements) {
    final Column populated = functions.filter(elements, Column::isNotNull);
    return functions.when(functions.size(populated).gt(0), populated);
  }
}
