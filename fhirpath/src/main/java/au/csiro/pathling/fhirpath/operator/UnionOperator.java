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

import au.csiro.pathling.errors.UnsupportedFhirPathFeatureError;
import au.csiro.pathling.fhirpath.collection.BooleanCollection;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.DecimalCollection;
import au.csiro.pathling.fhirpath.collection.IntegerCollection;
import au.csiro.pathling.fhirpath.collection.StringCollection;
import jakarta.annotation.Nonnull;
import org.apache.spark.sql.Column;

/**
 * Provides the functionality of the union operator within FHIRPath, i.e. {@code |}.
 * <p>
 * Merges two collections into a single collection, eliminating any duplicate values using equality
 * semantics. There is no expectation of order in the resulting collection.
 * <p>
 * Currently supports only primitive types that use native Spark equality: Boolean, Integer, Decimal,
 * and String.
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
        || nonEmpty instanceof DecimalCollection || nonEmpty instanceof StringCollection)) {
      throw new UnsupportedFhirPathFeatureError(
          "Union operator is not supported for type: " + nonEmpty.getFhirType());
    }

    final Column array = getArrayForUnion(nonEmpty);
    return nonEmpty.copyWithColumn(array_distinct(array));
  }

  @Nonnull
  @Override
  protected Collection handleEquivalentTypes(@Nonnull final Collection left,
      @Nonnull final Collection right, @Nonnull final BinaryOperatorInput input) {

    // Check if the collection type is supported
    // Only primitive types with native Spark equality are currently supported
    if (!(left instanceof BooleanCollection || left instanceof IntegerCollection
        || left instanceof DecimalCollection || left instanceof StringCollection)) {
      throw new UnsupportedFhirPathFeatureError(
          "Union operator is not supported for type: " + left.getFhirType());
    }

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

  @Nonnull
  @Override
  public String getOperatorName() {
    return "|";
  }
}
