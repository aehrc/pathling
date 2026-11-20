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

import au.csiro.pathling.fhirpath.column.StoredPrimitive;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import java.util.function.UnaryOperator;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;

/**
 * The comparator of primitives held with their id and extensions (see {@link StoredPrimitive}).
 * They are compared and deduplicated by their values, as the comparator of their System type
 * compares them, so that a primitive's {@code id} and {@code extension} take no part (decision
 * 151).
 */
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public final class StoredPrimitiveComparator implements ColumnComparator, ElementWiseEquality {

  /** The comparator of the System values. */
  @Nonnull private final ColumnComparator system;

  /**
   * Returns the comparator of primitives held with their id and extensions, whose values the given
   * comparator compares.
   *
   * @param system the comparator of the System values
   * @return the comparator
   */
  @Nonnull
  public static StoredPrimitiveComparator of(@Nonnull final ColumnComparator system) {
    return new StoredPrimitiveComparator(system);
  }

  @Override
  @Nonnull
  public Column equalsTo(@Nonnull final Column left, @Nonnull final Column right) {
    // Two elements without a value are taken as equal, so that a deduplication by comparison keeps
    // one of them, as a deduplication by key does. An operator compares System values, never these.
    return functions
        .when(
            StoredPrimitive.valueOf(left).isNull().and(StoredPrimitive.valueOf(right).isNull()),
            functions.lit(true))
        .otherwise(system.equalsTo(StoredPrimitive.valueOf(left), StoredPrimitive.valueOf(right)));
  }

  @Override
  @Nonnull
  public Column lessThan(@Nonnull final Column left, @Nonnull final Column right) {
    return system.lessThan(StoredPrimitive.valueOf(left), StoredPrimitive.valueOf(right));
  }

  @Override
  @Nonnull
  public Column lessThanOrEqual(@Nonnull final Column left, @Nonnull final Column right) {
    return system.lessThanOrEqual(StoredPrimitive.valueOf(left), StoredPrimitive.valueOf(right));
  }

  @Override
  @Nonnull
  public Column greaterThan(@Nonnull final Column left, @Nonnull final Column right) {
    return system.greaterThan(StoredPrimitive.valueOf(left), StoredPrimitive.valueOf(right));
  }

  @Override
  @Nonnull
  public Column greaterThanOrEqual(@Nonnull final Column left, @Nonnull final Column right) {
    return system.greaterThanOrEqual(StoredPrimitive.valueOf(left), StoredPrimitive.valueOf(right));
  }

  @Override
  @Nonnull
  public Optional<UnaryOperator<Column>> deduplicationKey() {
    // The key is the System comparator's key of the value, or the value itself where the System
    // comparator uses SQL equality, held in a structure so that the elements without a value share
    // one key and are kept once, as FHIRPath implementations keep them. Otherwise there is no key,
    // and the values are compared.
    if (system.usesDefaultSqlEquality()) {
      return Optional.of(primitive -> functions.struct(StoredPrimitive.valueOf(primitive)));
    }
    return system
        .deduplicationKey()
        .map(key -> primitive -> key.apply(StoredPrimitive.valueOf(primitive)));
  }
}
