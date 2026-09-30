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

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.EmptyCollection;
import jakarta.annotation.Nonnull;
import java.util.List;

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

    // Handle empty collections with special semantics
    final boolean leftIsEmpty = left instanceof EmptyCollection;
    final boolean rightIsEmpty = right instanceof EmptyCollection;

    if (leftIsEmpty && rightIsEmpty) {
      return EmptyCollection.getInstance();
    }
    if (leftIsEmpty || rightIsEmpty) {
      final Collection nonEmpty = leftIsEmpty ? right : left;
      return handleOneEmpty(nonEmpty, input);
    }

    // Unify the operands: promote both sides to a common FHIR type where one exists, e.g. an
    // integer to a decimal, and, unless the subclass unifies them as it combines them, unify their
    // SQL shapes, which differ where one FHIR type is stored in a different shape at each path
    // (FR-056). Where there is no common type, the subclass decides what that means.
    final List<Collection> unified = unify(List.of(left, right));

    final Collection reconciledLeft = unified.get(0);
    final Collection reconciledRight = unified.get(1);
    return reconciledLeft.typeEquivalentWith(reconciledRight)
        ? handleEquivalentTypes(reconciledLeft, reconciledRight, input)
        : handleNonEquivalentTypes(reconciledLeft, reconciledRight, input);
  }

  /**
   * Unifies the operands before they are handed to {@link #handleEquivalentTypes} or {@link
   * #handleNonEquivalentTypes}. By default, this promotes their types and unifies their shapes
   * through {@link CombiningLogic#unify(List)}. A subclass that combines its operands into one
   * collection may instead promote only their types, and unify their shapes as it combines them.
   *
   * @param operands the left and right operands, in that order
   * @return the unified operands, in the same order
   */
  @Nonnull
  protected List<Collection> unify(@Nonnull final List<Collection> operands) {
    return CombiningLogic.unify(operands);
  }

  /**
   * Handles the case when exactly one operand is empty. By default, this returns an empty
   * collection, but subclasses may override this to provide alternative behaviour.
   *
   * @param nonEmpty the non-empty operand
   * @param input the original input for diagnostic purposes
   * @return A {@link Collection} object representing the resulting expression
   */
  @Nonnull
  protected Collection handleOneEmpty(
      @Nonnull final Collection nonEmpty, @Nonnull final BinaryOperatorInput input) {
    return EmptyCollection.getInstance();
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
