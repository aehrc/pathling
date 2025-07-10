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
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import jakarta.annotation.Nonnull;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import org.apache.commons.lang3.function.TriFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;

/**
 * Describes a set of methods that can be used to compare {@link Collection} objects to other paths,
 * e.g. for equality.
 *
 * @author John Grimes
 * @author Piotr Szul
 */
public interface Comparable {

  ColumnComparator DEFAULT_COMPARATOR = new DefaultComparator();

  @Nonnull
  default ColumnComparator getComparator() {
    return DEFAULT_COMPARATOR;
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
   * {@link ColumnRepresentation}
   */
  @Nonnull
  default Function<Comparable, ColumnRepresentation> getComparison(
      @Nonnull final ComparisonOperation operation) {
    return target -> {

      final UnaryOperator<Column> arrayExpression = sourceArray -> {
        final Column targetArray = target.getColumn().toArray().getValue();

        // If the comparison is between two arrays, use the zip_with function to compare element-wise.
        final Column zip = functions.zip_with(sourceArray, targetArray,
            (left, right) -> operation.comparisonFunction.apply(getComparator(), left, right));

        // Check if all elements in the zipped array are true.
        final Column allTrue = functions.forall(zip, c -> c);

        // If the arrays are of different sizes, return false.
        return functions.when(
                functions.size(sourceArray).equalTo(functions.size(targetArray)), allTrue)
            .otherwise(functions.lit(false));
      };

      // If the comparison is between singular values, use the comparison function directly.
      final UnaryOperator<Column> singularExpression = column ->
          operation.comparisonFunction.apply(getComparator(), column,
              // We need to assert that the target is singular. We already know that the source is
              // singular, but in the case of equality we may still be looking at an array
              // target.
              target.getColumn().singular("Comparison requires singular target").getValue());

      final ColumnRepresentation result = getColumn().vectorize(arrayExpression,
          singularExpression);
      // The result needs to be wrapped in a DefaultRepresentation, as the vectorize method
      // may have been called on a different type of ColumnRepresentation.
      return new DefaultRepresentation(result.getValue());
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
  default boolean isComparableTo(@Nonnull final Comparable path) {
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
    private final TriFunction<ColumnComparator, Column, Column, Column> comparisonFunction;

    ComparisonOperation(@Nonnull final String fhirPath,
        @Nonnull final TriFunction<ColumnComparator, Column, Column, Column> comparisonFunction) {
      this.fhirPath = fhirPath;
      this.comparisonFunction = comparisonFunction;
    }

    @Override
    @Nonnull
    public String toString() {
      return fhirPath;
    }

  }

}
