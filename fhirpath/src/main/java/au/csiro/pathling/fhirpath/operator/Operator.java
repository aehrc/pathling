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

import au.csiro.pathling.fhirpath.Comparable;
import au.csiro.pathling.fhirpath.Comparable.ComparisonOperation;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.Numeric.MathOperation;
import au.csiro.pathling.fhirpath.operator.BooleanOperator.BooleanOperatorType;
import au.csiro.pathling.fhirpath.operator.MembershipOperator.MembershipOperatorType;
import com.google.common.collect.ImmutableMap;
import jakarta.annotation.Nonnull;
import java.util.Map;

/**
 * Represents a binary operator in FHIRPath.
 *
 * @author John Grimes
 */
public interface Operator {

  /**
   * Mapping of operator names to instances of those operators.
   */
  Map<String, Operator> NAME_TO_INSTANCE = new ImmutableMap.Builder<String, Operator>()
      .put("and", new BooleanOperator(BooleanOperatorType.AND))
      .put("or", new BooleanOperator(BooleanOperatorType.OR))
      .put("xor", new BooleanOperator(BooleanOperatorType.XOR))
      .put("implies", new BooleanOperator(BooleanOperatorType.IMPLIES))
      .put("=", new ComparisonOperator(ComparisonOperation.EQUALS))
      .put("!=", new ComparisonOperator(ComparisonOperation.NOT_EQUALS))
      .put("<=", new ComparisonOperator(ComparisonOperation.LESS_THAN_OR_EQUAL_TO))
      .put("<", new ComparisonOperator(ComparisonOperation.LESS_THAN))
      .put(">=", new ComparisonOperator(ComparisonOperation.GREATER_THAN_OR_EQUAL_TO))
      .put(">", new ComparisonOperator(ComparisonOperation.GREATER_THAN))
      .put("in", new MembershipOperator(MembershipOperatorType.IN))
      .put("contains", new MembershipOperator(MembershipOperatorType.CONTAINS))
      .put("+", new MathOperator(MathOperation.ADDITION))
      .put("-", new MathOperator(MathOperation.SUBTRACTION))
      .put("*", new MathOperator(MathOperation.MULTIPLICATION))
      .put("/", new MathOperator(MathOperation.DIVISION))
      .put("mod", new MathOperator(MathOperation.MODULUS))
      .put("combine", new CombineOperator())
      .build();

  /**
   * Invokes this operator with the specified inputs.
   *
   * @param input An {@link OperatorInput} object
   * @return A {@link FhirPath} object representing the resulting expression
   */
  @Nonnull
  FhirPath invoke(@Nonnull OperatorInput input);

  /**
   * Retrieves an instance of the specified operator.
   *
   * @param name The operator string
   * @return An instance of an Operator
   */
  @Nonnull
  static Operator getInstance(@Nonnull final String name) {
    final Operator operator = NAME_TO_INSTANCE.get(name);
    checkUserInput(operator != null, "Unsupported operator: " + name);
    return operator;
  }

  /**
   * @param input The inputs to the operator
   * @param operatorName The FHIRPath representation of the operator
   * @return A FHIRPath expression describing the invocation of the operator
   */
  @Nonnull
  static String buildExpression(@Nonnull final OperatorInput input,
      @Nonnull final String operatorName) {
    final FhirPath left = input.getLeft();
    final FhirPath right = input.getRight();
    return left.getExpression() + " " + operatorName + " " + right.getExpression();
  }

  /**
   * Performs a set of validation checks on inputs that are intended to be used within a comparison
   * operation.
   *
   * @param input The inputs to the operator
   * @param operatorName The FHIRPath representation of the operator
   */
  static void checkArgumentsAreComparable(@Nonnull final OperatorInput input,
      @Nonnull final String operatorName) {
    final FhirPath left = input.getLeft();
    final FhirPath right = input.getRight();

    checkUserInput(left instanceof Comparable,
        operatorName + " operator does not support left operand: " + left.getExpression());
    checkUserInput(right instanceof Comparable,
        operatorName + " operator does not support right operand: " + left.getExpression());

    final Comparable leftComparable = (Comparable) left;
    final Comparable rightComparable = (Comparable) right;
    final String expression = buildExpression(input, operatorName);
    checkUserInput(leftComparable.isComparableTo(rightComparable.getClass()),
        "Left operand to " + operatorName + " operator is not comparable to right operand: "
            + expression);
  }

}
