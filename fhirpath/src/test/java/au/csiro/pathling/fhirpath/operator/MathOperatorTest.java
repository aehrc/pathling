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

import static au.csiro.pathling.test.assertions.Assertions.assertThat;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.literal.DecimalLiteralPath;
import au.csiro.pathling.fhirpath.literal.IntegerLiteralPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.builders.DatasetBuilder;
import au.csiro.pathling.test.builders.ElementPathBuilder;
import au.csiro.pathling.test.builders.ParserContextBuilder;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import lombok.Value;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author John Grimes
 */
@SpringBootUnitTest
@TestInstance(Lifecycle.PER_CLASS)
class MathOperatorTest {

  @Autowired
  SparkSession spark;

  @Autowired
  FhirContext fhirContext;

  static final List<String> EXPRESSION_TYPES = Arrays
      .asList("Integer", "Decimal", "Integer (literal)", "Decimal (literal)");
  static final String ID_ALIAS = "_abc123";

  @Value
  static class TestParameters {

    @Nonnull
    String name;

    @Nonnull
    FhirPath left;

    @Nonnull
    FhirPath right;

    @Nonnull
    ParserContext context;

    boolean leftOperandIsInteger;

    boolean leftTypeIsLiteral;

    boolean rightTypeIsLiteral;

    @Override
    public String toString() {
      return name;
    }

  }

  Stream<TestParameters> parameters() {
    final Collection<TestParameters> parameters = new ArrayList<>();
    for (final String leftType : EXPRESSION_TYPES) {
      for (final String rightType : EXPRESSION_TYPES) {
        final FhirPath left = getExpressionForType(leftType, true);
        final FhirPath right = getExpressionForType(rightType, false);
        final boolean leftOperandIsInteger =
            leftType.equals("Integer") || leftType.equals("Integer (literal)");
        final boolean leftTypeIsLiteral =
            leftType.equals("Integer (literal)") || leftType.equals("Decimal (literal)");
        final boolean rightTypeIsLiteral =
            rightType.equals("Integer (literal)") || rightType.equals("Decimal (literal)");
        final ParserContext context = new ParserContextBuilder(spark, fhirContext).groupingColumns(
            Collections.singletonList(left.getIdColumn())).build();
        parameters.add(
            new TestParameters(leftType + ", " + rightType, left, right, context,
                leftOperandIsInteger, leftTypeIsLiteral, rightTypeIsLiteral));
      }
    }
    return parameters.stream();
  }

  FhirPath getExpressionForType(final String expressionType,
      final boolean leftOperand) {
    final Dataset<Row> literalContextDataset = new DatasetBuilder(spark)
        .withIdColumn(ID_ALIAS)
        .withColumn(DataTypes.BooleanType)
        .withIdsAndValue(false, Arrays
            .asList("patient-1", "patient-2", "patient-3", "patient-4"))
        .build();
    final ElementPath literalContext = new ElementPathBuilder(spark)
        .dataset(literalContextDataset)
        .idAndValueColumns()
        .build();
    switch (expressionType) {
      case "Integer":
        return buildIntegerExpression(leftOperand);
      case "Integer (literal)":
        return IntegerLiteralPath.fromString(leftOperand
                                             ? "1"
                                             : "2", literalContext);
      case "Decimal":
        return buildDecimalExpression(leftOperand);
      case "Decimal (literal)":
        return DecimalLiteralPath.fromString(leftOperand
                                             ? "1.0"
                                             : "2.0", literalContext);
      default:
        throw new RuntimeException("Invalid data type");
    }
  }

  FhirPath buildIntegerExpression(final boolean leftOperand) {
    final Dataset<Row> dataset = new DatasetBuilder(spark)
        .withIdColumn(ID_ALIAS)
        .withColumn(DataTypes.IntegerType)
        .withRow("patient-1", leftOperand
                              ? 1
                              : 2)
        .withRow("patient-2", leftOperand
                              ? null
                              : 2)
        .withRow("patient-3", leftOperand
                              ? 1
                              : null)
        .withRow("patient-4", null)
        .build();
    return new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.INTEGER)
        .dataset(dataset)
        .idAndValueColumns()
        .singular(true)
        .build();
  }

  FhirPath buildDecimalExpression(final boolean leftOperand) {
    final Dataset<Row> dataset = new DatasetBuilder(spark)
        .withIdColumn(ID_ALIAS)
        .withColumn(DataTypes.createDecimalType())
        .withRow("patient-1", new BigDecimal(leftOperand
                                             ? "1.0"
                                             : "2.0"))
        .withRow("patient-2", leftOperand
                              ? null
                              : new BigDecimal("2.0"))
        .withRow("patient-3", leftOperand
                              ? new BigDecimal("1.0")
                              : null)
        .withRow("patient-4", null)
        .build();
    return new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.DECIMAL)
        .dataset(dataset)
        .idAndValueColumns()
        .singular(true)
        .build();
  }

  @ParameterizedTest
  @MethodSource("parameters")
  void addition(final TestParameters parameters) {
    final OperatorInput input = new OperatorInput(parameters.getContext(), parameters.getLeft(),
        parameters.getRight());
    final Operator comparisonOperator = Operator.getInstance("+");
    final FhirPath result = comparisonOperator.invoke(input);
    final Object value = parameters.isLeftOperandIsInteger()
                         ? 3
                         : new BigDecimal("3.0");

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-1", value),
        RowFactory.create("patient-2", parameters.isLeftTypeIsLiteral()
                                       ? value
                                       : null),
        RowFactory.create("patient-3", parameters.isRightTypeIsLiteral()
                                       ? value
                                       : null),
        RowFactory
            .create("patient-4",
                parameters.isLeftTypeIsLiteral() && parameters.isRightTypeIsLiteral()
                ? value
                : null)
    );
  }

  @ParameterizedTest
  @MethodSource("parameters")
  void subtraction(final TestParameters parameters) {
    final OperatorInput input = new OperatorInput(parameters.getContext(), parameters.getLeft(),
        parameters.getRight());
    final Operator comparisonOperator = Operator.getInstance("-");
    final FhirPath result = comparisonOperator.invoke(input);
    final Object value = parameters.isLeftOperandIsInteger()
                         ? -1
                         : new BigDecimal("-1.0");

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-1", value),
        RowFactory.create("patient-2", parameters.isLeftTypeIsLiteral()
                                       ? value
                                       : null),
        RowFactory.create("patient-3", parameters.isRightTypeIsLiteral()
                                       ? value
                                       : null),
        RowFactory
            .create("patient-4",
                parameters.isLeftTypeIsLiteral() && parameters.isRightTypeIsLiteral()
                ? value
                : null)
    );
  }

  @ParameterizedTest
  @MethodSource("parameters")
  void multiplication(final TestParameters parameters) {
    final OperatorInput input = new OperatorInput(parameters.getContext(), parameters.getLeft(),
        parameters.getRight());
    final Operator comparisonOperator = Operator.getInstance("*");
    final FhirPath result = comparisonOperator.invoke(input);
    final Object value = parameters.isLeftOperandIsInteger()
                         ? 2
                         : new BigDecimal("2.0");

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-1", value),
        RowFactory.create("patient-2", parameters.isLeftTypeIsLiteral()
                                       ? value
                                       : null),
        RowFactory.create("patient-3", parameters.isRightTypeIsLiteral()
                                       ? value
                                       : null),
        RowFactory
            .create("patient-4",
                parameters.isLeftTypeIsLiteral() && parameters.isRightTypeIsLiteral()
                ? value
                : null)
    );
  }

  @ParameterizedTest
  @MethodSource("parameters")
  void division(final TestParameters parameters) {
    final OperatorInput input = new OperatorInput(parameters.getContext(), parameters.getLeft(),
        parameters.getRight());
    final Operator comparisonOperator = Operator.getInstance("/");
    final FhirPath result = comparisonOperator.invoke(input);
    final Object value = new BigDecimal("0.5");

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-1", value),
        RowFactory.create("patient-2", parameters.isLeftTypeIsLiteral()
                                       ? value
                                       : null),
        RowFactory.create("patient-3", parameters.isRightTypeIsLiteral()
                                       ? value
                                       : null),
        RowFactory
            .create("patient-4",
                parameters.isLeftTypeIsLiteral() && parameters.isRightTypeIsLiteral()
                ? value
                : null)
    );
  }

  @ParameterizedTest
  @MethodSource("parameters")
  void modulus(final TestParameters parameters) {
    final OperatorInput input = new OperatorInput(parameters.getContext(), parameters.getLeft(),
        parameters.getRight());
    final Operator comparisonOperator = Operator.getInstance("mod");
    final FhirPath result = comparisonOperator.invoke(input);
    final Object value = 1;

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-1", value),
        RowFactory.create("patient-2", parameters.isLeftTypeIsLiteral()
                                       ? value
                                       : null),
        RowFactory.create("patient-3", parameters.isRightTypeIsLiteral()
                                       ? value
                                       : null),
        RowFactory
            .create("patient-4",
                parameters.isLeftTypeIsLiteral() && parameters.isRightTypeIsLiteral()
                ? value
                : null)
    );
  }

}
