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

import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.exists;
import static org.apache.spark.sql.functions.forall;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.not;
import static org.apache.spark.sql.functions.size;
import static org.apache.spark.sql.functions.when;
import static org.apache.spark.sql.functions.zip_with;

import jakarta.annotation.Nonnull;
import org.apache.spark.sql.Column;

/**
 * An implementation of {@link ColumnEquality} that performs element-wise comparison on two array
 * columns by delegating the comparison of individual elements to another {@link ColumnEquality}.
 * <p>
 * For equality operations: - Returns true if all corresponding elements in the arrays are equal -
 * Returns false if any element differs or if arrays have different sizes
 * <p>
 * For inequality operations: - Returns true if any corresponding elements in the arrays are not
 * equal or if arrays have different sizes - Returns false if all elements are equal
 *
 * @author Piotr Szul
 */
public class ArrayElementWiseColumnEquality implements ColumnEquality {

  @Nonnull
  private final ColumnEquality elementComparator;

  /**
   * Creates a new ArrayColumnEquality that delegates element comparisons to the provided
   * comparator.
   *
   * @param elementComparator the comparator to use for individual element comparisons
   */
  public ArrayElementWiseColumnEquality(@Nonnull final ColumnEquality elementComparator) {
    this.elementComparator = elementComparator;
  }

  @Nonnull
  @Override
  public Column equalsTo(@Nonnull final Column left, @Nonnull final Column right) {
    return performArrayComparison(left, right, false);
  }

  @Nonnull
  @Override
  public Column notEqual(@Nonnull final Column left, @Nonnull final Column right) {
    return performArrayComparison(left, right, true);
  }

  /**
   * Performs element-wise comparison on two array columns.
   *
   * @param left the left array column
   * @param right the right array column
   * @param isNotEqual whether this is a not-equal comparison (true) or equal comparison (false)
   * @return a column representing the array comparison result
   */
  @Nonnull
  private Column performArrayComparison(@Nonnull final Column left, @Nonnull final Column right,
      final boolean isNotEqual) {
    // Zip the arrays and apply the element comparator to each pair
    final Column elementComparisons = zip_with(left, right,
        isNotEqual
        ? elementComparator::notEqual
        : elementComparator::equalsTo);

    // For equality: all elements must be equal (use forall)
    // For inequality: any element can be unequal (use exists with negated comparison)
    final Column arrayResult = isNotEqual
                               ? exists(elementComparisons, e -> e)
                               : forall(elementComparisons, e -> e);

    // If arrays have different sizes, they are not equal
    final Column sizeComparison = size(left).equalTo(size(right));

    return when(not(sizeComparison), lit(isNotEqual))
        .otherwise(
            // Handle the case where some elements cannot be compared (null results)
            // For equality: null comparisons default to false (not equal)
            // For inequality: null comparisons default to true (assume not equal)
            coalesce(arrayResult, lit(isNotEqual))
        );
  }
}
