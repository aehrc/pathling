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

package au.csiro.pathling.fhirpath.operator;

import static org.apache.spark.sql.functions.array_distinct;
import static org.apache.spark.sql.functions.array_union;
import static org.apache.spark.sql.functions.concat;

import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.DecimalCollection;
import au.csiro.pathling.fhirpath.comparison.ColumnEquality;
import au.csiro.pathling.sql.SqlFunctions;
import jakarta.annotation.Nonnull;
import lombok.experimental.UtilityClass;
import org.apache.spark.sql.Column;

/**
 * Shared array-level primitives used by the FHIRPath combining operators. These helpers are used by
 * {@link UnionOperator} (which deduplicates) and {@link CombineOperator} (which concatenates
 * without deduplication) to share type reconciliation, Decimal normalization, and comparator-aware
 * merging.
 *
 * <p>The helpers operate on already type-reconciled, non-empty {@link Collection} instances. Empty
 * operand handling and type reconciliation remain the responsibility of the enclosing operator's
 * {@link SameTypeBinaryOperator} template methods.
 *
 * @author Piotr Szul
 */
@UtilityClass
public class CombiningLogic {

  /**
   * Extracts the array column from a collection in a form suitable for combining. For {@link
   * DecimalCollection}, normalizes the Decimal representation to {@code DECIMAL(32,6)} so that two
   * operands with different precisions can be merged without schema mismatch.
   *
   * @param collection the collection to extract the array column from
   * @return the array column ready for combining
   */
  @Nonnull
  public static Column prepareArray(@Nonnull final Collection collection) {
    if (collection instanceof final DecimalCollection decimalCollection) {
      return decimalCollection.normalizeDecimalType().getColumn().plural().getValue();
    }
    return collection.getColumn().plural().getValue();
  }

  /**
   * Deduplicates the values in an array using the appropriate equality strategy. Types that use
   * default SQL equality leverage Spark's {@code array_distinct}, while types with custom equality
   * (Quantity, Coding, temporal types) use element-wise comparison via {@link
   * SqlFunctions#arrayDistinctWithEquality}.
   *
   * @param arrayColumn the array column to deduplicate
   * @param comparator the equality comparator that defines element equality
   * @return the deduplicated array column
   */
  @Nonnull
  public static Column dedupeArray(
      @Nonnull final Column arrayColumn, @Nonnull final ColumnEquality comparator) {
    if (comparator.usesDefaultSqlEquality()) {
      return array_distinct(arrayColumn);
    }
    return SqlFunctions.arrayDistinctWithEquality(arrayColumn, comparator::equalsTo);
  }

  /**
   * Merges two arrays and deduplicates the result using the appropriate equality strategy. Types
   * that use default SQL equality leverage Spark's {@code array_union}, while types with custom
   * equality use element-wise comparison via {@link SqlFunctions#arrayUnionWithEquality}.
   *
   * @param leftArray the left array column
   * @param rightArray the right array column
   * @param comparator the equality comparator that defines element equality
   * @return the merged, deduplicated array column
   */
  @Nonnull
  public static Column unionArrays(
      @Nonnull final Column leftArray,
      @Nonnull final Column rightArray,
      @Nonnull final ColumnEquality comparator) {
    if (comparator.usesDefaultSqlEquality()) {
      return array_union(leftArray, rightArray);
    }
    return SqlFunctions.arrayUnionWithEquality(leftArray, rightArray, comparator::equalsTo);
  }

  /**
   * Concatenates two arrays without deduplication, preserving all duplicate values from both
   * operands. Used by the FHIRPath {@code combine(other)} function.
   *
   * @param leftArray the left array column
   * @param rightArray the right array column
   * @return the concatenated array column
   */
  @Nonnull
  public static Column combineArrays(
      @Nonnull final Column leftArray, @Nonnull final Column rightArray) {
    return concat(leftArray, rightArray);
  }
}
