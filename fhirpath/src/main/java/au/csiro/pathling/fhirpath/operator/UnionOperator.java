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

import static org.apache.spark.sql.functions.aggregate;
import static org.apache.spark.sql.functions.array;
import static org.apache.spark.sql.functions.array_distinct;
import static org.apache.spark.sql.functions.array_union;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.exists;
import static org.apache.spark.sql.functions.filter;
import static org.apache.spark.sql.functions.ifnull;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.not;
import static org.apache.spark.sql.functions.when;

import au.csiro.pathling.errors.UnsupportedFhirPathFeatureError;
import au.csiro.pathling.fhirpath.collection.BooleanCollection;
import au.csiro.pathling.fhirpath.collection.CodingCollection;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.DateCollection;
import au.csiro.pathling.fhirpath.collection.DateTimeCollection;
import au.csiro.pathling.fhirpath.collection.DecimalCollection;
import au.csiro.pathling.fhirpath.collection.IntegerCollection;
import au.csiro.pathling.fhirpath.collection.QuantityCollection;
import au.csiro.pathling.fhirpath.collection.StringCollection;
import au.csiro.pathling.fhirpath.collection.TimeCollection;
import au.csiro.pathling.fhirpath.comparison.ColumnEquality;
import jakarta.annotation.Nonnull;
import org.apache.spark.sql.Column;

/**
 * Provides the functionality of the union operator within FHIRPath, i.e. {@code |}.
 * <p>
 * Merges two collections into a single collection, eliminating any duplicate values using equality
 * semantics. There is no expectation of order in the resulting collection.
 * <p>
 * Supports primitive types that use native Spark equality (Boolean, Integer, Decimal, String) and
 * types with custom equality semantics (Quantity, Coding, Time, Date, DateTime).
 * <p>
 * Date and DateTime are compatible types and can be unioned together with implicit type promotion
 * from Date to DateTime.
 *
 * @author Piotr Szul
 * @see <a href="https://hl7.org/fhirpath/#union-collections">union</a>
 */
public class UnionOperator extends SameTypeBinaryOperator {

  @Nonnull
  @Override
  protected Collection handleOneEmpty(@Nonnull final Collection nonEmpty,
      @Nonnull final BinaryOperatorInput input) {
    // For union, if one operand is empty, return the non-empty operand with duplicates eliminated
    // Per FHIRPath spec: "Unioning an empty collection to a non-empty collection will return
    // the non-empty collection with duplicates eliminated."

    // Check if the collection type is supported
    if (!(nonEmpty instanceof BooleanCollection || nonEmpty instanceof IntegerCollection
        || nonEmpty instanceof DecimalCollection || nonEmpty instanceof StringCollection
        || nonEmpty instanceof QuantityCollection || nonEmpty instanceof CodingCollection
        || nonEmpty instanceof TimeCollection || nonEmpty instanceof DateCollection
        || nonEmpty instanceof DateTimeCollection)) {
      throw new UnsupportedFhirPathFeatureError(
          "Union operator is not supported for type: " + nonEmpty.getFhirType());
    }

    // QuantityCollection, CodingCollection, TimeCollection, DateCollection, and DateTimeCollection
    // require custom equality for deduplication
    if (nonEmpty instanceof QuantityCollection || nonEmpty instanceof CodingCollection
        || nonEmpty instanceof TimeCollection || nonEmpty instanceof DateCollection
        || nonEmpty instanceof DateTimeCollection) {
      final Column array = getArrayForUnion(nonEmpty);
      return nonEmpty.copyWithColumn(deduplicateWithEquality(array, nonEmpty));
    }

    // Primitive types use native Spark equality
    final Column array = getArrayForUnion(nonEmpty);
    return nonEmpty.copyWithColumn(array_distinct(array));
  }

  @Nonnull
  @Override
  protected Collection handleEquivalentTypes(@Nonnull final Collection left,
      @Nonnull final Collection right, @Nonnull final BinaryOperatorInput input) {

    // Check if the collection type is supported
    if (!(left instanceof BooleanCollection || left instanceof IntegerCollection
        || left instanceof DecimalCollection || left instanceof StringCollection
        || left instanceof QuantityCollection || left instanceof CodingCollection
        || left instanceof TimeCollection || left instanceof DateCollection
        || left instanceof DateTimeCollection)) {
      throw new UnsupportedFhirPathFeatureError(
          "Union operator is not supported for type: " + left.getFhirType());
    }

    // QuantityCollection, CodingCollection, TimeCollection, DateCollection, and DateTimeCollection
    // require custom equality for deduplication
    if (left instanceof QuantityCollection || left instanceof CodingCollection
        || left instanceof TimeCollection || left instanceof DateCollection
        || left instanceof DateTimeCollection) {
      final Column leftArray = getArrayForUnion(left);
      final Column rightArray = getArrayForUnion(right);
      final Column combined = concat(leftArray, rightArray);
      return left.copyWithColumn(deduplicateWithEquality(combined, left));
    }

    // Primitive types use native Spark equality
    final Column leftArray = getArrayForUnion(left);
    final Column rightArray = getArrayForUnion(right);
    final Column unionResult = array_union(leftArray, rightArray);

    // Return a collection of the same type as the input
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
      return decimalCollection.normalizeDecimalType()
          .getColumn().plural().getValue();
    }
    return collection.getColumn().plural().getValue();
  }

  /**
   * Deduplicates an array using the collection's custom equality semantics. This is used for types
   * that require custom comparison logic (e.g., Quantity) instead of native Spark equality.
   *
   * @param arrayColumn the array column to deduplicate
   * @param collection the collection providing the equality comparator
   * @return deduplicated array column
   */
  @Nonnull
  private Column deduplicateWithEquality(@Nonnull final Column arrayColumn,
      @Nonnull final Collection collection) {
    final ColumnEquality comparator = collection.getComparator();
    // Create a typed empty array by filtering arrayColumn with a condition that always returns false
    final Column emptyTypedArray = filter(arrayColumn, x -> lit(false));
    return aggregate(
        arrayColumn,
        emptyTypedArray,  // accumulator starts as typed empty array
        (acc, elem) -> when(
            // make sure not to de-duplicate non-comparable values (if non-comparable both should be retained)
            not(exists(acc, x -> ifnull(comparator.equalsTo(x, elem), lit(false)))),
            concat(acc, array(elem))
        ).otherwise(acc)
    );
  }

  @Nonnull
  @Override
  public String getOperatorName() {
    return "|";
  }
}
