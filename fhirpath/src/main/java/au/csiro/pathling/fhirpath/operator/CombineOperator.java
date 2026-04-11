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

import au.csiro.pathling.fhirpath.collection.Collection;
import jakarta.annotation.Nonnull;
import org.apache.spark.sql.Column;

/**
 * Provides the functionality of the FHIRPath {@code combine(other)} function, which merges two
 * collections into a single collection without eliminating duplicate values. Combining an empty
 * collection with a non-empty collection returns the non-empty collection. There is no expectation
 * of order in the resulting collection.
 *
 * <p>Unlike {@link UnionOperator}, {@code combine} does not deduplicate and does not need to
 * consult the collection's equality comparator. The array-level merge primitive is shared with
 * {@link UnionOperator} via {@link CombiningLogic}.
 *
 * @author Piotr Szul
 * @see <a href="https://hl7.org/fhirpath/#combineother-collection-collection">combine</a>
 */
public class CombineOperator extends SameTypeBinaryOperator {

  @Nonnull
  @Override
  protected Collection handleOneEmpty(
      @Nonnull final Collection nonEmpty, @Nonnull final BinaryOperatorInput input) {
    // Combine preserves duplicates, so no deduplication is required against an empty peer.
    return nonEmpty;
  }

  @Nonnull
  @Override
  protected Collection handleEquivalentTypes(
      @Nonnull final Collection left,
      @Nonnull final Collection right,
      @Nonnull final BinaryOperatorInput input) {
    final Column leftArray = CombiningLogic.prepareArray(left);
    final Column rightArray = CombiningLogic.prepareArray(right);
    final Column combined = CombiningLogic.combineArrays(leftArray, rightArray);
    return left.copyWithColumn(combined);
  }

  @Nonnull
  @Override
  public String getOperatorName() {
    return "combine";
  }
}
