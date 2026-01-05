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

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.EmptyCollection;
import jakarta.annotation.Nonnull;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Base class for binary operators that require both arguments to be of the same type.
 *
 * @author Piotr Szul
 */
public abstract class SameTypeBinaryOperator implements FhirPathBinaryOperator {

  @Nonnull
  @Override
  public Collection invoke(@Nonnull final BinaryOperatorInput input) {
    final Collection left = input.left();
    final Collection right = input.right();

    // If either operand is an EmptyCollection, return an EmptyCollection.
    if (left instanceof EmptyCollection || right instanceof EmptyCollection) {
      return EmptyCollection.getInstance();
    }

    // find common type for comparison and promote both sides to that type
    // e.g. IntegerCollection and DecimalCollection -> promote IntegerCollection to
    // DecimalCollection

    // if a common type does not exist then we have to options:
    // - either throw an error early
    // - or allow for implicit empty collections return the empty comparator,
    // which returns empty if any of the arguments is empty and trows a runtime error otherwise

    final Pair<Collection, Collection> reconciledArguments =
        FhirPathBinaryOperator.reconcileTypes(left, right);

    final Collection reconciledLeft = reconciledArguments.getLeft();
    final Collection reconciledRight = reconciledArguments.getRight();
    return reconciledLeft.typeEquivalentWith(reconciledRight)
        ? handleEquivalentTypes(reconciledLeft, reconciledRight, input)
        : handleNonEquivalentTypes(reconciledLeft, reconciledRight, input);
  }

  /**
   * Handles the case when the two collections cannot be reconciled to a common type. By default,
   * this fails with an error, but subclasses may override this to provide alternative behaviour.
   *
   * @param ignoredLeft the left collection
   * @param ignoredRight the right collection
   * @param input the original input for diagnostic purposes
   * @return A {@link Collection} object representing the resulting expression
   */
  @Nonnull
  protected Collection handleNonEquivalentTypes(
      @Nonnull final Collection ignoredLeft,
      @Nonnull final Collection ignoredRight,
      @Nonnull final BinaryOperatorInput input) {
    return fail(input);
  }

  /**
   * Handles the case when the two collections have been reconciled to a common type. Subclasses
   * must implement this method to provide the specific operator logic.
   *
   * @param left The left collection, promoted to the common type
   * @param right The right collection, promoted to the common type
   * @param input The original input for diagnostic purposes
   * @return A {@link Collection} object representing the resulting expression
   */
  @Nonnull
  protected abstract Collection handleEquivalentTypes(
      @Nonnull final Collection left,
      @Nonnull final Collection right,
      @Nonnull final BinaryOperatorInput input);

  /**
   * Fails with an {@link InvalidUserInputError}, indicating that the operator is not supported for
   * the provided input types.
   *
   * @param input The original input for diagnostic purposes
   * @return This method does not return a value; it always throws an exception
   */
  @Nonnull
  protected Collection fail(@Nonnull final BinaryOperatorInput input) {
    throw new InvalidUserInputError(
        "Operator `"
            + getOperatorName()
            + "` is not supported for: "
            + input.left().getDisplayExpression()
            + ", "
            + input.right().getDisplayExpression());
  }
}
