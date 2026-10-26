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

import au.csiro.pathling.fhirpath.column.QuantityValue;
import au.csiro.pathling.fhirpath.encoding.QuantityEncoding;
import jakarta.annotation.Nonnull;
import java.util.function.BinaryOperator;
import java.util.function.UnaryOperator;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;

/**
 * Implementation of Spark SQL comparator for the Quantity type. It compares the exact canonical
 * forms the quantities carry where they are comparable, and their original values otherwise.
 *
 * @author Piotr Szul
 */
public class QuantityComparator implements ColumnComparator, ElementWiseEquality {

  private static final QuantityComparator INSTANCE = new QuantityComparator();

  public static QuantityComparator getInstance() {
    return INSTANCE;
  }

  @SuppressWarnings("MissingJavadoc")
  private QuantityComparator() {}

  /**
   * Builds a comparison that compares the exact canonical forms the quantities carry where both
   * have one in the same unit, and their values otherwise, where their systems and codes match.
   *
   * @param decimalComparator compares two decoded values
   * @param test tests the sign of the comparison of two canonical forms
   */
  private static BinaryOperator<Column> wrap(
      @Nonnull final BinaryOperator<Column> decimalComparator,
      @Nonnull final UnaryOperator<Column> test) {

    return (left, right) ->
        let(
            left,
            right,
            (l, r) ->
                functions.coalesce(
                    test.apply(
                        QuantityEncoding.compareCanonical(
                            QuantityEncoding.canonical(l), QuantityEncoding.canonical(r))),
                    QuantityValue.of(l)
                        .originalValue()
                        .compare(QuantityValue.of(r).originalValue(), decimalComparator)));
  }

  @Override
  @Nonnull
  public Column equalsTo(@Nonnull final Column left, @Nonnull final Column right) {
    return wrap(Column::equalTo, sign -> sign.equalTo(0)).apply(left, right);
  }

  @Override
  @Nonnull
  public Column lessThan(@Nonnull final Column left, @Nonnull final Column right) {
    return wrap(Column::lt, sign -> sign.lt(0)).apply(left, right);
  }

  @Override
  @Nonnull
  public Column lessThanOrEqual(@Nonnull final Column left, @Nonnull final Column right) {
    return wrap(Column::leq, sign -> sign.leq(0)).apply(left, right);
  }

  @Override
  @Nonnull
  public Column greaterThan(@Nonnull final Column left, @Nonnull final Column right) {
    return wrap(Column::gt, sign -> sign.gt(0)).apply(left, right);
  }

  @Override
  @Nonnull
  public Column greaterThanOrEqual(@Nonnull final Column left, @Nonnull final Column right) {
    return wrap(Column::geq, sign -> sign.geq(0)).apply(left, right);
  }
}
