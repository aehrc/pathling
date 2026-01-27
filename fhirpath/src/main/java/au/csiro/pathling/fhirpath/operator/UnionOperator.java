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

package au.csiro.pathling.fhirpath.operator;

import static org.apache.spark.sql.functions.array_distinct;
import static org.apache.spark.sql.functions.array_union;

import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.DecimalCollection;
import au.csiro.pathling.fhirpath.comparison.ColumnEquality;
import au.csiro.pathling.sql.SqlFunctions;
import jakarta.annotation.Nonnull;
import org.apache.spark.sql.Column;

/**
 * Provides the functionality of the union operator within FHIRPath, i.e. {@code |}.
 *
 * <p>Merges two collections into a single collection, eliminating any duplicate values using
 * equality semantics. There is no expectation of order in the resulting collection.
 *
 * <p>Type compatibility is determined through FHIRPath type reconciliation. Collections with
 * compatible types are merged after type promotion (e.g., Date → DateTime, Integer → Decimal).
 *
 * <p>Equality semantics are determined by the collection's comparator. Types using default SQL
 * equality leverage Spark's native array operations, while types with custom equality (Quantity,
 * Coding, temporal types) use element-wise comparison.
 *
 * @author Piotr Szul
 * @see <a href="https://hl7.org/fhirpath/#union-collections">union</a>
 */
public class UnionOperator extends SameTypeBinaryOperator {

  @Nonnull
  @Override
  protected Collection handleOneEmpty(
      @Nonnull final Collection nonEmpty, @Nonnull final BinaryOperatorInput input) {
    final Column array = getArrayForUnion(nonEmpty);
    final Column deduplicatedArray = deduplicateArray(array, nonEmpty.getComparator());
    return nonEmpty.copyWithColumn(deduplicatedArray);
  }

  @Nonnull
  @Override
  protected Collection handleEquivalentTypes(
      @Nonnull final Collection left,
      @Nonnull final Collection right,
      @Nonnull final BinaryOperatorInput input) {

    final Column leftArray = getArrayForUnion(left);
    final Column rightArray = getArrayForUnion(right);
    final Column unionResult = unionArrays(leftArray, rightArray, left.getComparator());

    return left.copyWithColumn(unionResult);
  }

  /**
   * Extracts and prepares an array column for union operations. For DecimalCollection, normalizes
   * to DECIMAL(32,6) to ensure type compatibility.
   *
   * @param collection the collection to extract array from
   * @return the array column ready for union operation
   */
  @Nonnull
  private Column getArrayForUnion(@Nonnull final Collection collection) {
    if (collection instanceof DecimalCollection decimalCollection) {
      return decimalCollection.normalizeDecimalType().getColumn().plural().getValue();
    }
    return collection.getColumn().plural().getValue();
  }

  /**
   * Deduplicates an array using the appropriate strategy based on comparator type.
   *
   * @param arrayColumn the array column to deduplicate
   * @param comparator the equality comparator to use
   * @return deduplicated array column
   */
  @Nonnull
  private Column deduplicateArray(
      @Nonnull final Column arrayColumn, @Nonnull final ColumnEquality comparator) {
    if (comparator.usesDefaultSqlEquality()) {
      return array_distinct(arrayColumn);
    } else {
      return SqlFunctions.arrayDistinctWithEquality(arrayColumn, comparator::equalsTo);
    }
  }

  /**
   * Merges and deduplicates two arrays using the appropriate strategy.
   *
   * @param leftArray the left array column
   * @param rightArray the right array column
   * @param comparator the equality comparator to use
   * @return merged and deduplicated array column
   */
  @Nonnull
  private Column unionArrays(
      @Nonnull final Column leftArray,
      @Nonnull final Column rightArray,
      @Nonnull final ColumnEquality comparator) {
    if (comparator.usesDefaultSqlEquality()) {
      return array_union(leftArray, rightArray);
    } else {
      return SqlFunctions.arrayUnionWithEquality(leftArray, rightArray, comparator::equalsTo);
    }
  }

  @Nonnull
  @Override
  public String getOperatorName() {
    return "|";
  }
}
