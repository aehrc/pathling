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

import au.csiro.pathling.fhirpath.collection.Collection;
import jakarta.annotation.Nonnull;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Represents a binary operator in FHIRPath.
 *
 * @author John Grimes
 */
public interface FhirPathBinaryOperator {

  /**
   * Invokes this operator with the specified inputs.
   *
   * @param input An {@link BinaryOperatorInput} object
   * @return A {@link Collection} object representing the resulting expression
   */
  @Nonnull
  Collection invoke(@Nonnull BinaryOperatorInput input);

  /**
   * Gets the name of this operator, typically the simple class name.
   *
   * @return the name of this operator
   */
  @Nonnull
  default String getOperatorName() {
    return this.getClass().getSimpleName();
  }

  /**
   * Reconciles two collections to a common type, if possible.
   *
   * @param left The left collection
   * @param right The right collection
   * @return A pair of collections that can be reconciled to a common type
   */
  @Nonnull
  static Pair<Collection, Collection> reconcileTypes(
      @Nonnull final Collection left, @Nonnull final Collection right) {
    // finds if left and right elements can be reconciled to a common type
    if (right.convertibleTo(left)) {
      return Pair.of(left, right.castAs(left));
    } else if (left.convertibleTo(right)) {
      return Pair.of(left.castAs(right), right);
    } else {
      return Pair.of(left, right);
    }
  }
}
