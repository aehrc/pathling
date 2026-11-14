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
import org.apache.spark.sql.functions;

/**
 * The comparator of stored decimals, which are held as their text and numeric annotation (see
 * {@link StoredDecimal}). They are compared and deduplicated by their numbers, never by their text
 * or by the structure that holds them (decision 151), so that {@code 1.0} equals {@code 1.00} and
 * {@code 10} is greater than {@code 9}. The number of each is read once per element as the key of a
 * deduplication.
 */
public final class StoredDecimalComparator implements ColumnComparator, ElementWiseEquality {

  private static final StoredDecimalComparator INSTANCE = new StoredDecimalComparator(false);

  private static final StoredDecimalComparator WITH_METADATA = new StoredDecimalComparator(true);

  private static final DecimalComparator NUMERIC = DecimalComparator.getInstance();

  /**
   * Whether the decimals are held with their id and extensions, so that the ones without a value
   * share one deduplication key.
   */
  private final boolean withMetadata;

  private StoredDecimalComparator(final boolean withMetadata) {
    this.withMetadata = withMetadata;
  }

  /**
   * Gets the singleton instance.
   *
   * @return the instance
   */
  @Nonnull
  public static StoredDecimalComparator getInstance() {
    return INSTANCE;
  }

  /**
   * Gets the comparator of decimals held with their id and extensions: the decimals without a value
   * share one deduplication key, so that a deduplication keeps one of them, as it keeps one of
   * every other primitive without a value.
   *
   * @return the instance
   */
  @Nonnull
  public static StoredDecimalComparator withMetadata() {
    return WITH_METADATA;
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
    return withMetadata
        ? Optional.of(decimal -> functions.struct(StoredDecimal.number(decimal)))
        : Optional.of(StoredDecimal::number);
  }
}
