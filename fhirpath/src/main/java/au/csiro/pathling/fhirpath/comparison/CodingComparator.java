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

package au.csiro.pathling.fhirpath.comparison;

import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.operator.ColumnComparator;
import jakarta.annotation.Nonnull;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Column;

/**
 * Implementation of a Spark SQL comparator for the Coding type.
 *
 * @author Piotr Szul
 */
public class CodingComparator implements ColumnComparator {

  private static final List<String> EQUALITY_COLUMNS = Arrays
      .asList("system", "code", "version", "display", "userSelected");

  @Nonnull
  @Override
  public Column equalsTo(@Nonnull final Column left, @Nonnull final Column right) {
    return when(left.isNull().or(right.isNull()), lit(null))
        .otherwise(
            EQUALITY_COLUMNS.stream()
                .map(f -> left.getField(f).eqNullSafe(right.getField(f)))
                .reduce(Column::and).orElseThrow(() -> new AssertionError("No fields to compare"))
        );
  }

  @Nonnull
  @Override
  public Column lessThan(@Nonnull final Column left, @Nonnull final Column right) {
    throw new InvalidUserInputError("Coding type does not support the less than operator");

  }

  @Nonnull
  @Override
  public Column greaterThan(@Nonnull final Column left, @Nonnull final Column right) {
    throw new InvalidUserInputError("Coding type does not support the greater than operator");
  }

}
