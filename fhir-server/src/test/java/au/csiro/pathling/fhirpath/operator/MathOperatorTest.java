/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.operator;

import static au.csiro.pathling.test.assertions.Assertions.assertThat;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.literal.DecimalLiteralPath;
import au.csiro.pathling.fhirpath.literal.IntegerLiteralPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.test.builders.DatasetBuilder;
import au.csiro.pathling.test.builders.ElementPathBuilder;
import au.csiro.pathling.test.builders.ParserContextBuilder;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Value;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * @author John Grimes
 */
@Tag("UnitTest")
public class MathOperatorTest {

  private static final List<String> EXPRESSION_TYPES = Arrays
      .asList("Integer", "Decimal", "Integer (literal)", "Decimal (literal)");
  private ParserContext parserContext;

  @BeforeEach
  void setUp() {
    parserContext = new ParserContextBuilder().build();
  }

  @Value
  private static class TestParameters {

    @Nonnull
    String name;

    @Nonnull
    FhirPath left;

    @Nonnull
    FhirPath right;

    boolean leftOperandIsInteger;

    boolean leftTypeIsLiteral;

    boolean rightTypeIsLiteral;

    @Override
    public String toString() {
      return name;
    }

  }

  public static Stream<TestParameters> parameters() {
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
        parameters.add(
            new TestParameters(leftType + ", " + rightType, left, right, leftOperandIsInteger,
                leftTypeIsLiteral, rightTypeIsLiteral));
      }
    }
    return parameters.stream();
  }

  private static FhirPath getExpressionForType(final String expressionType,
      final boolean leftOperand) {
    final Dataset<Row> literalContextDataset = new DatasetBuilder()
        .withIdColumn()
        .withValueColumn(DataTypes.BooleanType)
        .withIdsAndValue(false, Arrays.asList("abc1", "abc2", "abc3", "abc4"))
        .build();
    final ElementPath literalContext = new ElementPathBuilder()
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

  private static FhirPath buildIntegerExpression(final boolean leftOperand) {
    final Dataset<Row> dataset = new DatasetBuilder()
        .withIdColumn()
        .withValueColumn(DataTypes.IntegerType)
        .withRow("abc1", leftOperand
                         ? 1
                         : 2)
        .withRow("abc2", leftOperand
                         ? null
                         : 2)
        .withRow("abc3", leftOperand
                         ? 1
                         : null)
        .withRow("abc4", null)
        .build();
    return new ElementPathBuilder()
        .fhirType(FHIRDefinedType.INTEGER)
        .dataset(dataset)
        .idAndValueColumns()
        .singular(true)
        .build();
  }

  private static FhirPath buildDecimalExpression(final boolean leftOperand) {
    final Dataset<Row> dataset = new DatasetBuilder()
        .withIdColumn()
        .withValueColumn(DataTypes.createDecimalType())
        .withRow("abc1", new BigDecimal(leftOperand
                                        ? "1.0"
                                        : "2.0"))
        .withRow("abc2", leftOperand
                         ? null
                         : new BigDecimal("2.0"))
        .withRow("abc3", leftOperand
                         ? new BigDecimal("1.0")
                         : null)
        .withRow("abc4", null)
        .build();
    return new ElementPathBuilder()
        .fhirType(FHIRDefinedType.DECIMAL)
        .dataset(dataset)
        .idAndValueColumns()
        .singular(true)
        .build();
  }

  @ParameterizedTest
  @MethodSource("parameters")
  public void addition(final TestParameters parameters) {
    final OperatorInput input = new OperatorInput(parserContext, parameters.getLeft(),
        parameters.getRight());
    final Operator comparisonOperator = Operator.getInstance("+");
    final FhirPath result = comparisonOperator.invoke(input);
    final Object value = parameters.isLeftOperandIsInteger()
                         ? 3
                         : new BigDecimal("3.0");

    assertThat(result).selectResult().hasRows(
        RowFactory.create("abc1", value),
        RowFactory.create("abc2", parameters.isLeftTypeIsLiteral()
                                  ? value
                                  : null),
        RowFactory.create("abc3", parameters.isRightTypeIsLiteral()
                                  ? value
                                  : null),
        RowFactory
            .create("abc4", parameters.isLeftTypeIsLiteral() && parameters.isRightTypeIsLiteral()
                            ? value
                            : null)
    );
  }

  @ParameterizedTest
  @MethodSource("parameters")
  public void subtraction(final TestParameters parameters) {
    final OperatorInput input = new OperatorInput(parserContext, parameters.getLeft(),
        parameters.getRight());
    final Operator comparisonOperator = Operator.getInstance("-");
    final FhirPath result = comparisonOperator.invoke(input);
    final Object value = parameters.isLeftOperandIsInteger()
                         ? -1
                         : new BigDecimal("-1.0");

    assertThat(result).selectResult().hasRows(
        RowFactory.create("abc1", value),
        RowFactory.create("abc2", parameters.isLeftTypeIsLiteral()
                                  ? value
                                  : null),
        RowFactory.create("abc3", parameters.isRightTypeIsLiteral()
                                  ? value
                                  : null),
        RowFactory
            .create("abc4", parameters.isLeftTypeIsLiteral() && parameters.isRightTypeIsLiteral()
                            ? value
                            : null)
    );
  }

  @ParameterizedTest
  @MethodSource("parameters")
  public void multiplication(final TestParameters parameters) {
    final OperatorInput input = new OperatorInput(parserContext, parameters.getLeft(),
        parameters.getRight());
    final Operator comparisonOperator = Operator.getInstance("*");
    final FhirPath result = comparisonOperator.invoke(input);
    final Object value = parameters.isLeftOperandIsInteger()
                         ? 2
                         : new BigDecimal("2.0");

    assertThat(result).selectResult().hasRows(
        RowFactory.create("abc1", value),
        RowFactory.create("abc2", parameters.isLeftTypeIsLiteral()
                                  ? value
                                  : null),
        RowFactory.create("abc3", parameters.isRightTypeIsLiteral()
                                  ? value
                                  : null),
        RowFactory
            .create("abc4", parameters.isLeftTypeIsLiteral() && parameters.isRightTypeIsLiteral()
                            ? value
                            : null)
    );
  }

  @ParameterizedTest
  @MethodSource("parameters")
  public void division(final TestParameters parameters) {
    final OperatorInput input = new OperatorInput(parserContext, parameters.getLeft(),
        parameters.getRight());
    final Operator comparisonOperator = Operator.getInstance("/");
    final FhirPath result = comparisonOperator.invoke(input);
    final Object value = new BigDecimal("0.5");

    assertThat(result).selectResult().hasRows(
        RowFactory.create("abc1", value),
        RowFactory.create("abc2", parameters.isLeftTypeIsLiteral()
                                  ? value
                                  : null),
        RowFactory.create("abc3", parameters.isRightTypeIsLiteral()
                                  ? value
                                  : null),
        RowFactory
            .create("abc4", parameters.isLeftTypeIsLiteral() && parameters.isRightTypeIsLiteral()
                            ? value
                            : null)
    );
  }

  @ParameterizedTest
  @MethodSource("parameters")
  public void modulus(final TestParameters parameters) {
    final OperatorInput input = new OperatorInput(parserContext, parameters.getLeft(),
        parameters.getRight());
    final Operator comparisonOperator = Operator.getInstance("mod");
    final FhirPath result = comparisonOperator.invoke(input);
    final Object value = 1;

    assertThat(result).selectResult().hasRows(
        RowFactory.create("abc1", value),
        RowFactory.create("abc2", parameters.isLeftTypeIsLiteral()
                                  ? value
                                  : null),
        RowFactory.create("abc3", parameters.isRightTypeIsLiteral()
                                  ? value
                                  : null),
        RowFactory
            .create("abc4", parameters.isLeftTypeIsLiteral() && parameters.isRightTypeIsLiteral()
                            ? value
                            : null)
    );
  }

}