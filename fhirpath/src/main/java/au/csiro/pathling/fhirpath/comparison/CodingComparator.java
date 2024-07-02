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
import au.csiro.pathling.fhirpath.operator.Comparable;
import au.csiro.pathling.fhirpath.operator.ColumnComparator;
import au.csiro.pathling.fhirpath.operator.Comparable.ComparisonOperation;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;

/**
 * Implementation of a Spark SQL comparator for the Coding type.
 *
 * @author Piotr Szul
 */
public class CodingComparator implements ColumnComparator {

  private static final List<String> EQUALITY_COLUMNS = Arrays
      .asList("system", "code", "version", "display", "userSelected");

  private static final CodingComparator INSTANCE = new CodingComparator();

  @Override
  public Column equalsTo(@Nonnull final Column left, @Nonnull final Column right) {
    //noinspection OptionalGetWithoutIsPresent
    return when(left.isNull().or(right.isNull()), lit(null))
        .otherwise(
            EQUALITY_COLUMNS.stream()
                .map(f -> left.getField(f).eqNullSafe(right.getField(f)))
                .reduce(Column::and).get()
        );
  }

  @Override
  public Column lessThan(final Column left, final Column right) {
    throw new InvalidUserInputError("Coding type does not support the less than operator");

  }

  @Override
  public Column greaterThan(final Column left, final Column right) {
    throw new InvalidUserInputError("Coding type does not support the greater than operator");
  }

  /**
   * Builds a comparison function for Coding paths.
   *
   * @param source The path to build the comparison function for
   * @param operation The {@link Comparable.ComparisonOperation} type to build
   * @return A new {@link Function}
   */
  @Nonnull
  public static Function<Comparable, Column> buildComparison(@Nonnull final Comparable source,
      @Nonnull final ComparisonOperation operation) {
    return Comparable.buildComparison(source, operation, INSTANCE);
  }

}
