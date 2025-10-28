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

import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;
import static org.apache.spark.sql.functions.when;

import au.csiro.pathling.fhirpath.encoding.QuantityEncoding;
import au.csiro.pathling.sql.types.FlexiDecimal;
import jakarta.annotation.Nonnull;
import java.util.function.BinaryOperator;
import jakarta.annotation.Nullable;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;

/**
 * Implementation of Spark SQL comparator for the Quantity type. It uses canonicalized values and
 * units for comparison rather than the original values.
 *
 * @author Piotr Szul
 */
public class QuantityComparator implements ColumnComparator, ElementWiseEquality {

  private static final QuantityComparator INSTANCE = new QuantityComparator();

  public static QuantityComparator getInstance() {
    return INSTANCE;
  }

  @SuppressWarnings("MissingJavadoc")
  private QuantityComparator() {
  }

  /**
   * A helper record to hold a value and a unit column.
   *
   * <p>It provides methods to extract normalized and original values from a Quantity column, and
   * to compare two {@link ValueWithUnit} instances using a provided comparator, ensuring that the
   * units are the same.
   */
  private record ValueWithUnit(
      @Nonnull Column value,
      @Nonnull Column unit,
      @Nullable Column system
  ) {

    /**
     * Extracts the normalized (canonicalized) value and unit from a Quantity column.
     *
     * @param quantity the Quantity column
     * @return a ValueWithUnit instance containing the normalized value and unit
     */
    @Nonnull
    static ValueWithUnit normalizedQuantity(@Nonnull Column quantity) {
      return new ValueWithUnit(
          quantity.getField(QuantityEncoding.CANONICALIZED_VALUE_COLUMN),
          quantity.getField(QuantityEncoding.CANONICALIZED_CODE_COLUMN),
          null
      );
    }

    /**
     * Extracts the original value and unit from a Quantity column.
     *
     * @param quantity the Quantity column
     * @return a ValueWithUnit instance containing the original value and unit
     */
    @Nonnull
    static ValueWithUnit originalQuantity(@Nonnull Column quantity) {
      return new ValueWithUnit(
          quantity.getField(QuantityEncoding.VALUE_COLUMN),
          quantity.getField(QuantityEncoding.CODE_COLUMN),
          quantity.getField(QuantityEncoding.SYSTEM_COLUMN)
      );
    }


    @Nonnull
    Column compare(@Nonnull final ValueWithUnit other, BinaryOperator<Column> comparator) {
      // assert that both (this and other) systems are either non-null or null
      // that is we compare either two original or two normalized quantities
      if ((system == null) != (other.system == null)) {
        throw new IllegalArgumentException("Both quantities must have system or neither.");
      }
      return when(
          nonNull(system)
          ? requireNonNull(system).equalTo(requireNonNull(other.system))
              .and(unit.equalTo(other.unit))
          : unit.equalTo(other.unit),
          comparator.apply(value, other.value)
      );
    }
  }

  private static BinaryOperator<Column> wrap(
      @Nonnull final BinaryOperator<Column> decimalComparator,
      @Nonnull final BinaryOperator<Column> flexComparator
  ) {

    return (left, right) -> functions.coalesce(
        ValueWithUnit.normalizedQuantity(left)
            .compare(ValueWithUnit.normalizedQuantity(right), flexComparator),
        ValueWithUnit.originalQuantity(left)
            .compare(ValueWithUnit.originalQuantity(right), decimalComparator)
    );
  }

  @Override
  @Nonnull
  public Column equalsTo(@Nonnull final Column left, @Nonnull final Column right) {
    return wrap(Column::equalTo, FlexiDecimal::equalTo).apply(left, right);
  }

  @Override
  @Nonnull
  public Column lessThan(@Nonnull final Column left, @Nonnull final Column right) {
    return wrap(Column::lt, FlexiDecimal::lt).apply(left, right);
  }

  @Override
  @Nonnull
  public Column lessThanOrEqual(@Nonnull final Column left, @Nonnull final Column right) {
    return wrap(Column::leq, FlexiDecimal::leq).apply(left, right);
  }

  @Override
  @Nonnull
  public Column greaterThan(@Nonnull final Column left, @Nonnull final Column right) {
    return wrap(Column::gt, FlexiDecimal::gt).apply(left, right);
  }

  @Override
  @Nonnull
  public Column greaterThanOrEqual(@Nonnull final Column left, @Nonnull final Column right) {
    return wrap(Column::geq, FlexiDecimal::geq).apply(left, right);
  }
}
