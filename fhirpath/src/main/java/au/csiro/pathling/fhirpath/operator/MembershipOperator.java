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

import au.csiro.pathling.fhirpath.annotations.NotImplemented;
import javax.annotation.Nonnull;

/**
 * An expression that tests whether a singular value is present within a collection.
 *
 * @author John Grimes
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/operators.html#membership">Membership</a>
 */
@NotImplemented
public class MembershipOperator /*extends AggregateFunction */ implements BinaryOperator {

  private final MembershipOperatorType type;

  /**
   * @param type The type of operator
   */
  public MembershipOperator(final MembershipOperatorType type) {
    this.type = type;
  }

  // TODO: implement with columns

  // @Nonnull
  // @Override
  // public Collection invoke(@Nonnull final BinaryOperatorInput input) {
  //   final Collection left = input.getLeft();
  //   final Collection right = input.getRight();
  //   final Collection element = type.equals(MembershipOperatorType.IN)
  //                          ? left
  //                          : right;
  //   final Collection collection = type.equals(MembershipOperatorType.IN)
  //                             ? right
  //                             : left;
  //
  //   checkUserInput(element.isSingular(),
  //       "Element operand used with " + type + " operator is not singular: " + element
  //           .getExpression());
  //   checkArgumentsAreComparable(input, type.toString());
  //   final Column elementValue = element.getValueColumn();
  //   final Column collectionValue = collection.getValueColumn();
  //
  //   final String expression = left.getExpression() + " " + type + " " + right.getExpression();
  //   final Comparable leftComparable = (Comparable) left;
  //   final Comparable rightComparable = (Comparable) right;
  //   final Column equality = leftComparable.getComparison(ComparisonOperation.EQUALS)
  //       .apply(rightComparable);
  //
  //   // If the left-hand side of the operator (element) is empty, the result is empty. If the
  //   // right-hand side (collection) is empty, the result is false. Otherwise, a Boolean is returned
  //   // based on whether the element is present in the collection, using equality semantics.
  //   final Column equalityWithNullChecks = when(elementValue.isNull(), lit(null))
  //       .when(collectionValue.isNull(), lit(false))
  //       .otherwise(equality);
  //
  //   // In order to reduce the result to a single Boolean, we take the max of the boolean equality
  //   // values.
  //   final Column aggregateColumn = max(equalityWithNullChecks);
  //
  //   return buildAggregateResult(right.getDataset(), input.getContext(), Arrays.asList(left, right),
  //       aggregateColumn, expression, FHIRDefinedType.BOOLEAN);
  // }

  /**
   * Represents a type of membership operator.
   */
  public enum MembershipOperatorType {
    /**
     * Contains operator
     */
    CONTAINS("contains"),
    /**
     * In operator
     */
    IN("in");

    @Nonnull
    private final String fhirPath;

    MembershipOperatorType(@Nonnull final String fhirPath) {
      this.fhirPath = fhirPath;
    }

    @Override
    public String toString() {
      return fhirPath;
    }
  }

}
