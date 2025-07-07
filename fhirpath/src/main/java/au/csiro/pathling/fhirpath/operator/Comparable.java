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

package au.csiro.pathling.fhirpath.operator;

import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.column.ColumnRepresentation;
import jakarta.annotation.Nonnull;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.commons.lang3.function.TriFunction;
import org.apache.spark.sql.Column;

/**
 * Describes a set of methods that can be used to compare {@link Collection} objects to other paths,
 * e.g. for equality.
 *
 * @author John Grimes
 * @author Piotr Szul
 */
public interface Comparable {

  ColumnComparator DEFAULT_COMPARATOR = new DefaultComparator();

  /**
   * Get a function that can take two Comparable paths and return a function that can compare their
   * columnar representations. The type of comparison is controlled by supplying a
   * {@link ComparisonOperation}.
   *
   * @param other The other path to compare to
   * @param operation The {@link ComparisonOperation} type to retrieve a comparison for
   * @return A {@link BiFunction} that takes two {@link Column} objects as its parameters, and
   * returns a {@link Column} with the result of the comparison.
   */
  @Nonnull
  default BiFunction<Column, Column, Column> getSqlComparator(@Nonnull final Comparable other,
      @Nonnull final ComparisonOperation operation) {
    return buildSqlComparator(this, other, operation);
  }

  /**
   * Get a function that can take two Comparable paths and return a function that can compare their
   * columnar representations. The type of comparison is controlled by supplying a
   * {@link ComparisonOperation}.
   *
   * @param left The left path to compare
   * @param right The right path to compare
   * @param operation The {@link ComparisonOperation} type to retrieve a comparison for
   * @param comparator The {@link ColumnComparator} to use
   * @return A {@link BiFunction} that takes two {@link Column} objects as its parameters, and
   * returns a {@link Column} with the result of the comparison.
   */
  @Nonnull
  static BiFunction<Column, Column, Column> buildSqlComparator(
      @Nonnull final Comparable left, @Nonnull final Comparable right,
      @Nonnull final ComparisonOperation operation, @Nonnull final ColumnComparator comparator) {
    if (!left.isComparableTo(right)) {
      throw new IllegalArgumentException("Cannot compare " + left + " to " + right);
    }
    return (x, y) -> operation.compFunction.apply(comparator, x, y);
  }

  /**
   * Get a function that can take two Comparable paths and return a function that can compare their
   * columnar representations using the default comparator.
   *
   * @param left The left path to compare
   * @param right The right path to compare
   * @param operation The {@link ComparisonOperation} type to retrieve a comparison for
   * @return A {@link BiFunction} that takes two {@link Column} objects as its parameters, and
   */
  @Nonnull
  static BiFunction<Column, Column, Column> buildSqlComparator(
      @Nonnull final Comparable left, @Nonnull final Comparable right,
      @Nonnull final ComparisonOperation operation) {
    return buildSqlComparator(left, right, operation, DEFAULT_COMPARATOR);
  }

  /**
   * Get a function that can take two Comparable paths and return a {@link Column} that contains a
   * comparison condition. The type of condition is controlled by supplying a
   * {@link ComparisonOperation}.
   * <p>
   * Please use {@link #isComparableTo(Comparable)} to first check whether the specified path should
   * be compared to this path.
   *
   * @param operation The {@link ComparisonOperation} type to retrieve a comparison for
   * @return A {@link Function} that takes a Comparable as its parameter, and returns a
   * {@link Column}
   */
  @Nonnull
  default Function<Comparable, Column> getComparison(@Nonnull ComparisonOperation operation) {
    return target -> {
      final BiFunction<Column, Column, Column> sqlComparator = getSqlComparator(target,
          operation);
      return sqlComparator.apply(getColumn().singular().getValue(),
          target.getColumn().singular().getValue());
    };
  }

  /**
   * Returns a {@link Column} within the dataset containing the values of the nodes.
   *
   * @return A {@link Column}
   */
  @Nonnull
  ColumnRepresentation getColumn();

  /**
   * @return {@code true} if this path can be compared to the specified class
   */
  default boolean isComparableTo(@Nonnull Comparable path) {
    return true;
  }

  /**
   * Represents a type of comparison operation.
   */
  enum ComparisonOperation {
    /**
     * The equals operation.
     */
    EQUALS("=", ColumnComparator::equalsTo),

    /**
     * The not equals operation.
     */
    NOT_EQUALS("!=", ColumnComparator::notEqual),

    /**
     * The less than or equal to operation.
     */
    LESS_THAN_OR_EQUAL_TO("<=", ColumnComparator::lessThanOrEqual),

    /**
     * The less than operation.
     */
    LESS_THAN("<", ColumnComparator::lessThan),

    /**
     * The greater than or equal to operation.
     */
    GREATER_THAN_OR_EQUAL_TO(">=", ColumnComparator::greaterThanOrEqual),

    /**
     * The greater than operation.
     */
    GREATER_THAN(">", ColumnComparator::greaterThan);

    @Nonnull
    private final String fhirPath;

    @Nonnull
    private final TriFunction<ColumnComparator, Column, Column, Column> compFunction;

    ComparisonOperation(@Nonnull final String fhirPath,
        @Nonnull final TriFunction<ColumnComparator, Column, Column, Column> compFunction) {
      this.fhirPath = fhirPath;
      this.compFunction = compFunction;
    }

    @Override
    @Nonnull
    public String toString() {
      return fhirPath;
    }

  }

}
