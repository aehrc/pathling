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

import static au.csiro.pathling.utilities.Preconditions.checkUserInput;

import au.csiro.pathling.fhirpath.collection.BooleanCollection;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.operator.Comparable.ComparisonOperation;
import jakarta.annotation.Nonnull;
import java.util.function.BiFunction;
import lombok.experimental.UtilityClass;
import org.apache.commons.lang.StringUtils;
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
  public static BooleanCollection in(@Nonnull final Collection element,
      @Nonnull final Collection collection) {
    checkComparable(element, IN_OPERATOR, LEFT_OPERAND);
    checkComparable(collection, IN_OPERATOR, RIGHT_OPERAND);
    return containsImpl(collection, element);
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
  public static BooleanCollection contains(@Nonnull final Collection collection,
      @Nonnull final Collection element) {
    checkComparable(collection, CONTAINS_OPERATOR, LEFT_OPERAND);
    checkComparable(element, CONTAINS_OPERATOR, RIGHT_OPERAND);
    return containsImpl(collection, element);
  }

  private static void checkComparable(@Nonnull final Collection collection,
      @Nonnull final String operator,
      @Nonnull final String operand) {
    checkUserInput(collection instanceof Comparable,
        StringUtils.capitalize(operand) + " operand to " + operator
            + " operator must be Comparable");
  }

  @Nonnull
  private static BooleanCollection containsImpl(@Nonnull final Collection collection,
      @Nonnull final Collection element) {

    final BiFunction<Column, Column, Column> columnComparator = ((Comparable) collection)
        .getSqlComparator((Comparable) element, ComparisonOperation.EQUALS);

    // In the future, type adjustment may be done before the operator is called.
    final Collection typeAdjustedElement = element.convertibleTo(collection)
                                           ? element.castAs(collection)
                                           : element;
    return BooleanCollection.build(
        collection.getColumn()
            .contains(typeAdjustedElement.getColumn().singular(), columnComparator));
  }
}
