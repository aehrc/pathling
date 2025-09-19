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

import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.column.ColumnRepresentation;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import jakarta.annotation.Nonnull;
import java.util.function.Function;
import org.apache.commons.lang3.function.TriFunction;
import org.apache.spark.sql.Column;

/**
 * Describes a set of methods that can be used to compare {@link Collection} objects with an order
 * to other paths, e.g. determining which is greater than the other.
 *
 * @author John Grimes
 * @author Piotr Szul
 */
public interface Comparable extends Equatable {


  /**
   * Gets the column comparator for this comparable object.
   *
   * @return the column comparator to use for comparisons
   */
  @Override
  @Nonnull
  default ColumnComparator getComparator() {
    return DefaultComparator.getInstance();
  }

  /**
   * Get a function that can take two Comparable paths and return a {@link Column} that contains a
   * comparison condition. The type of condition is controlled by supplying a
   * {@link ComparisonOperation}.
   * <p>
   *
   * @param operation The {@link ComparisonOperation} type to retrieve a comparison for
   * @return A {@link Function} that takes a Comparable as its parameter, and returns a
   * {@link ColumnRepresentation}
   */
  @Nonnull
  default Function<Comparable, ColumnRepresentation> getComparison(
      @Nonnull final ComparisonOperation operation) {
    return target -> {

      final Column columnResult = operation.comparisonFunction.apply(getComparator(),
          getColumn().singular("Comparison requires singular left operand").getValue(),
          target.getColumn().singular("Comparison requires singular right operand").getValue()
      );
      return new DefaultRepresentation(columnResult);
    };
  }

  /**
   * Represents a type of comparison operation.
   */
  enum ComparisonOperation {

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
