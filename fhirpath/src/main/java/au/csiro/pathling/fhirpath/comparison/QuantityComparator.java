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

import au.csiro.pathling.fhirpath.column.QuantityValue;
import au.csiro.pathling.sql.types.FlexiDecimal;
import jakarta.annotation.Nonnull;
import java.util.function.BinaryOperator;
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
  private QuantityComparator() {}

  private static BinaryOperator<Column> wrap(
      @Nonnull final BinaryOperator<Column> decimalComparator,
      @Nonnull final BinaryOperator<Column> flexComparator) {

    return (left, right) ->
        functions.coalesce(
            QuantityValue.of(left)
                .normalizedValue()
                .compare(QuantityValue.of(right).normalizedValue(), flexComparator),
            QuantityValue.of(left)
                .originalValue()
                .compare(QuantityValue.of(right).originalValue(), decimalComparator));
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
