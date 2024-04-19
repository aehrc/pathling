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

import static au.csiro.pathling.QueryHelpers.join;
import static au.csiro.pathling.fhirpath.operator.Operator.checkArgumentsAreComparable;
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.when;

import au.csiro.pathling.QueryHelpers.JoinType;
import au.csiro.pathling.fhirpath.Comparable;
import au.csiro.pathling.fhirpath.Comparable.ComparisonOperation;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.function.AggregateFunction;
import jakarta.annotation.Nonnull;
import java.util.Arrays;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * An expression that tests whether a singular value is present within a collection.
 *
 * @author John Grimes
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/operators.html#membership">Membership</a>
 */
public class MembershipOperator extends AggregateFunction implements Operator {

  private final MembershipOperatorType type;

  /**
   * @param type The type of operator
   */
  public MembershipOperator(final MembershipOperatorType type) {
    this.type = type;
  }

  @Nonnull
  @Override
  public FhirPath invoke(@Nonnull final OperatorInput input) {
    final FhirPath left = input.getLeft();
    final FhirPath right = input.getRight();
    final FhirPath element = type.equals(MembershipOperatorType.IN)
                             ? left
                             : right;
    final FhirPath collection = type.equals(MembershipOperatorType.IN)
                                ? right
                                : left;

    checkUserInput(element.isSingular(),
        "Element operand used with " + type + " operator is not singular: " + element
            .getExpression());
    checkArgumentsAreComparable(input, type.toString());
    final Column elementValue = element.getValueColumn();
    final Column collectionValue = collection.getValueColumn();

    final String expression = left.getExpression() + " " + type + " " + right.getExpression();
    final Comparable leftComparable = (Comparable) left;
    final Comparable rightComparable = (Comparable) right;
    final Column equality = leftComparable.getComparison(ComparisonOperation.EQUALS)
        .apply(rightComparable);

    // If the left-hand side of the operator (element) is empty, the result is empty. If the
    // right-hand side (collection) is empty, the result is false. Otherwise, a Boolean is returned
    // based on whether the element is present in the collection, using equality semantics.
    final Column equalityWithNullChecks = when(elementValue.isNull(), lit(null))
        .when(collectionValue.isNull(), lit(false))
        .otherwise(equality);

    // We need to join the datasets in order to access values from both operands.
    final Dataset<Row> dataset = join(input.getContext(), left, right, JoinType.LEFT_OUTER);

    // In order to reduce the result to a single Boolean, we take the max of the boolean equality
    // values.
    final Column valueColumn = max(equalityWithNullChecks);

    return buildAggregateResult(dataset, input.getContext(), Arrays.asList(left, right),
        valueColumn, expression, FHIRDefinedType.BOOLEAN);
  }

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
