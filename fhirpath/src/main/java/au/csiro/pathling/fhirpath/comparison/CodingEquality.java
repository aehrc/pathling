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

package au.csiro.pathling.fhirpath.comparison;

import static au.csiro.pathling.sql.SqlFunctions.let;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;

import au.csiro.pathling.encoders.ColumnFunctions;
import au.csiro.pathling.fhirpath.encoding.CodingSchema;
import jakarta.annotation.Nonnull;
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
      List.of(
          CodingSchema.SYSTEM_FIELD,
          CodingSchema.CODE_FIELD,
          CodingSchema.VERSION_FIELD,
          CodingSchema.DISPLAY_FIELD,
          CodingSchema.USER_SELECTED_FIELD);

  @Nonnull
  @Override
  public Column equalsTo(@Nonnull final Column left, @Nonnull final Column right) {
    return let(
        left,
        l ->
            let(
                right,
                r ->
                    when(l.isNull().or(r.isNull()), lit(null))
                        .otherwise(
                            EQUALITY_COLUMNS.stream()
                                .map(f -> field(l, f).eqNullSafe(field(r, f)))
                                .reduce(Column::and)
                                .orElseThrow(() -> new AssertionError("No fields to compare")))));
  }

  /**
   * Reads a field of a Coding by name through the tolerant traversal, so that a Coding whose
   * structure does not carry the field, such as one pruned to the fields its data populates, reads
   * it as null, which is what an unpopulated field holds (FR-031).
   */
  @Nonnull
  private static Column field(@Nonnull final Column coding, @Nonnull final String name) {
    return ColumnFunctions.resolveOrNull(
        coding, name, CodingSchema.DATA_TYPE.apply(name).dataType());
  }
}
