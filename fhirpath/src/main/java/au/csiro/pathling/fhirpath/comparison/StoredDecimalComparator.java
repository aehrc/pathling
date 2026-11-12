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

import au.csiro.pathling.fhirpath.column.StoredDecimal;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import java.util.function.UnaryOperator;
import org.apache.spark.sql.Column;

/**
 * The comparator of stored decimals, which are held as their text and numeric annotation (see
 * {@link StoredDecimal}). They are compared and deduplicated by their numbers, never by their text
 * or by the structure that holds them (decision 151), so that {@code 1.0} equals {@code 1.00} and
 * {@code 10} is greater than {@code 9}. The number of each is read once per element as the key of a
 * deduplication.
 */
public final class StoredDecimalComparator implements ColumnComparator, ElementWiseEquality {

  private static final StoredDecimalComparator INSTANCE = new StoredDecimalComparator();

  private static final DecimalComparator NUMERIC = DecimalComparator.getInstance();

  private StoredDecimalComparator() {}

  /**
   * Gets the singleton instance.
   *
   * @return the instance
   */
  @Nonnull
  public static StoredDecimalComparator getInstance() {
    return INSTANCE;
  }

  @Override
  @Nonnull
  public Column equalsTo(@Nonnull final Column left, @Nonnull final Column right) {
    return NUMERIC.equalsTo(StoredDecimal.number(left), StoredDecimal.number(right));
  }

  @Override
  @Nonnull
  public Column lessThan(@Nonnull final Column left, @Nonnull final Column right) {
    return NUMERIC.lessThan(StoredDecimal.number(left), StoredDecimal.number(right));
  }

  @Override
  @Nonnull
  public Column lessThanOrEqual(@Nonnull final Column left, @Nonnull final Column right) {
    return NUMERIC.lessThanOrEqual(StoredDecimal.number(left), StoredDecimal.number(right));
  }

  @Override
  @Nonnull
  public Column greaterThan(@Nonnull final Column left, @Nonnull final Column right) {
    return NUMERIC.greaterThan(StoredDecimal.number(left), StoredDecimal.number(right));
  }

  @Override
  @Nonnull
  public Column greaterThanOrEqual(@Nonnull final Column left, @Nonnull final Column right) {
    return NUMERIC.greaterThanOrEqual(StoredDecimal.number(left), StoredDecimal.number(right));
  }

  @Override
  @Nonnull
  public Optional<UnaryOperator<Column>> deduplicationKey() {
    return Optional.of(StoredDecimal::number);
  }
}
