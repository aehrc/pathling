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

import jakarta.annotation.Nonnull;
import java.util.Optional;
import java.util.function.UnaryOperator;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;

/**
 * An interface that defines methods for comparing two {@link Column} objects for equality and
 * inequality.
 *
 * @author Piotr Szul
 */
public interface ColumnEquality {

  /**
   * Creates an element equality comparison between two columns.
   *
   * @param left the left column
   * @param right the right column
   * @return a column representing the equality comparison
   */
  @Nonnull
  Column equalsTo(@Nonnull Column left, @Nonnull Column right);

  /**
   * Creates an element inequality comparison between two columns.
   *
   * @param left the left column
   * @param right the right column
   * @return a column representing the inequality comparison
   */
  @Nonnull
  default Column notEqual(@Nonnull final Column left, @Nonnull final Column right) {
    return functions.not(equalsTo(left, right));
  }

  /**
   * Returns a comparator that can be used to compare arrays of the elements that this comparator
   * can compare.
   *
   * @return a comparator for arrays of comparable elements
   */
  @Nonnull
  default ColumnEquality asArrayComparator() {
    // by default, the comparator for arrays is the same as for single elements
    // this should work for SQL primitive types
    return this;
  }

  /**
   * Returns the key by which this comparator's equality deduplicates, where there is one: a value
   * computed from each element such that two elements are equal exactly where their keys are, and
   * null where the element is (decision 151). Deduplication then computes the key once for each
   * element rather than comparing every pair.
   *
   * @return the key of an element, or empty where equality is not decided by one
   */
  @Nonnull
  default Optional<UnaryOperator<Column>> deduplicationKey() {
    return Optional.empty();
  }

  /**
   * Indicates whether this comparator uses default SQL equality semantics. When true, arrays are
   * deduplicated by Spark's native {@code array_distinct}, on a key without another producer's
   * annotation fields where there are any (decision 151). When false, a deduplication key or a
   * custom element-wise comparison is required.
   *
   * @return true if this comparator uses default SQL equality, false otherwise
   */
  default boolean usesDefaultSqlEquality() {
    return this instanceof DefaultComparator;
  }
}
