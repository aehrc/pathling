/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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

import static au.csiro.pathling.utilities.Preconditions.check;
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;

import au.csiro.pathling.fhirpath.collection.BooleanCollection;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.EmptyCollection;
import au.csiro.pathling.fhirpath.column.ColumnRepresentation;
import au.csiro.pathling.fhirpath.comparison.Comparable;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import java.util.function.BinaryOperator;
import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Column;

/**
 * Provides the functionality of the collection operators within FHIRPath.
 *
 * @author John Grimes
 * @author Piotr Szul
 * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#collections-1">FHIRPath specification -
 * Collections</a>
 */
@UtilityClass
public class CollectionOperations {

  private static final String IN_OPERATOR = "in";
  private static final String CONTAINS_OPERATOR = "contains";

  private static final String LEFT_OPERAND = "left";
  private static final String RIGHT_OPERAND = "right";

  /**
   * If the left operand is a collection with a single item, this operator returns {@code true} if
   * the item is in the right operand using equality semantics. If the left-hand side of the
   * operator is empty, the result is empty, if the right-hand side is empty, the result is
   * {@code false}. If the left operand has multiple items, an exception is thrown.
   *
   * @param element The element to check for membership
   * @param collection The collection to check membership within
   * @return A {@link BooleanCollection} representing the result of the operation
   * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#in-membership">FHIRPath specification -
   * in (membership)</a>
   */
  @FhirPathOperator
  @Nonnull
  public static Collection in(@Nonnull final Collection element,
      @Nonnull final Collection collection) {
    return executeContains(collection, element, IN_OPERATOR, true);
  }

  /**
   * If the right operand is a collection with a single item, this operator returns {@code true} if
   * the item is in the left operand using equality semantics. If the right-hand side of the
   * operator is empty, the result is empty, if the left-hand side is empty, the result is
   * {@code @code false}. This is the converse operation of {@link #in(Collection, Collection)}.
   *
   * @param collection The collection to check membership within
   * @param element The element to check for membership
   * @return A {@link BooleanCollection} representing the result of the operation
   * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#contains-containership">FHIRPath
   * specification - contains (containership)</a>
   */
  @FhirPathOperator
  @Nonnull
  public static Collection contains(@Nonnull final Collection collection,
      @Nonnull final Collection element) {
    return executeContains(collection, element, CONTAINS_OPERATOR, false);
  }

  @Nonnull
  private static Collection executeContains(@Nonnull final Collection collection,
      @Nonnull final Collection element, final String operator, final boolean invert) {
    // Check if either operand is an EmptyCollection and handle accordingly.
    final Optional<Collection> returnValue = checkForEmptyOperands(element, collection);
    if (returnValue.isPresent()) {
      return returnValue.get();
    }

    // Check that both operands are comparable, and that they can be compared to each other.
    checkComparable(collection, operator, invert
                                          ? RIGHT_OPERAND
                                          : LEFT_OPERAND);
    checkComparable(element, operator, invert
                                       ? LEFT_OPERAND
                                       : RIGHT_OPERAND);
    checkOperandsAreComparable(collection, element);
    check(collection instanceof au.csiro.pathling.fhirpath.comparison.Comparable);
    check(element instanceof au.csiro.pathling.fhirpath.comparison.Comparable);
    final au.csiro.pathling.fhirpath.comparison.Comparable collectionComparable = (au.csiro.pathling.fhirpath.comparison.Comparable) collection;

    // Cast the element to the type of the collection if it is convertible, otherwise use the
    // element as is. This allows for type adjustments in cases where the element is not of the
    // same type as the collection.
    final Collection typeAdjustedElement = element.convertibleTo(collection)
                                           ? element.castAs(collection)
                                           : element;

    // The element must be a singular value for the contains operation.
    final ColumnRepresentation singular = typeAdjustedElement.getColumn().singular();

    // Use the collection's equality comparator to check if the singular value is contained within 
    // the collection.
    final BinaryOperator<Column> comparator =
        (left, right) -> collectionComparable.getComparator().equalsTo(left, right);

    // Return a BooleanCollection containing the result.
    final ColumnRepresentation column = collection.getColumn().contains(singular, comparator);
    return BooleanCollection.build(column);
  }

  @Nonnull
  private static Optional<Collection> checkForEmptyOperands(final @Nonnull Collection element,
      final @Nonnull Collection collection) {
    if (element instanceof EmptyCollection) {
      return Optional.of(EmptyCollection.getInstance());
    } else if (collection instanceof EmptyCollection) {
      return Optional.of(BooleanCollection.fromValue(false));
    }
    return Optional.empty();
  }

  private static void checkComparable(@Nonnull final Collection collection,
      @Nonnull final String operator,
      @Nonnull final String operand) {
    checkUserInput(collection instanceof au.csiro.pathling.fhirpath.comparison.Comparable,
        StringUtils.capitalize(operand) + " operand to " + operator
            + " operator must be comparable");
  }

  private static void checkOperandsAreComparable(final @Nonnull Collection collection,
      final @Nonnull Collection element) {
    // Check that the collection and element are compatible for comparison.
    final au.csiro.pathling.fhirpath.comparison.Comparable collectionComparable = (au.csiro.pathling.fhirpath.comparison.Comparable) collection;
    final au.csiro.pathling.fhirpath.comparison.Comparable elementComparable = (Comparable) element;
    checkUserInput(collectionComparable.isComparableTo(elementComparable),
        "Comparison of paths is not supported: " + collection.getDisplayExpression() + ", "
            + element.getDisplayExpression());
  }

}
