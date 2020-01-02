/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.pathling.query.operators;

import static au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType.DECIMAL;
import static au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType.INTEGER;
import static au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType.STRING;
import static au.csiro.pathling.test.Assertions.assertThat;
import static au.csiro.pathling.test.PrimitiveExpressionBuilder.literalInteger;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import au.csiro.pathling.query.parsing.ParsedExpression;
import au.csiro.pathling.test.PrimitiveExpressionBuilder;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * @author John Grimes
 */
@Category(au.csiro.pathling.UnitTest.class)
@RunWith(Parameterized.class)
public class MathOperatorTest {

  private static List<String> expressionTypes = Arrays
      .asList("Integer", "Decimal", "Integer (literal)", "Decimal (literal)");

  @Parameters(name = "{0}, {1}")
  public static Object[] parameters() {
    List<Object[]> parameters = new ArrayList<>();
    for (String leftType : expressionTypes) {
      for (String rightType : expressionTypes) {
        if (!(leftType.contains("literal") && rightType.contains("literal"))) {
          parameters.add(new Object[]{leftType, rightType});
        }
      }
    }
    return parameters.toArray();
  }

  private final String leftType, rightType;
  private ParsedExpression left, right;
  private boolean leftOperandIsInteger;
  private boolean leftTypeIsLiteral;
  private boolean rightTypeIsLiteral;

  public MathOperatorTest(String leftType, String rightType) {
    this.leftType = leftType;
    this.rightType = rightType;
  }

  @Before
  public void setUp() {
    left = getExpressionForType(leftType, true);
    right = getExpressionForType(rightType, false);
    leftOperandIsInteger = leftType.equals("Integer") || leftType.equals("Integer (literal)");
    leftTypeIsLiteral =
        leftType.equals("Integer (literal)") || leftType.equals("Decimal (literal)");
    rightTypeIsLiteral =
        rightType.equals("Integer (literal)") || rightType.equals("Decimal (literal)");
  }

  private ParsedExpression getExpressionForType(String expressionType, boolean leftOperand) {
    switch (expressionType) {
      case "Integer":
        return buildIntegerExpression(leftOperand);
      case "Integer (literal)":
        return literalInteger(4);
      case "Decimal":
        return buildDecimalExpression(leftOperand);
      case "Decimal (literal)":
        return PrimitiveExpressionBuilder.literalDecimal(new BigDecimal("4.0"));
      default:
        throw new RuntimeException("Invalid data type");
    }
  }

  private ParsedExpression buildIntegerExpression(boolean leftOperand) {
    ParsedExpression expression = new PrimitiveExpressionBuilder(FHIRDefinedType.INTEGER, INTEGER)
        .withColumn("123abcd_id", DataTypes.StringType)
        .withColumn("123abcd", DataTypes.IntegerType)
        .withRow("abc1", 4)
        .withRow("abc2", leftOperand ? null : 4)
        .withRow("abc3", leftOperand ? 4 : null)
        .withRow("abc4", null)
        .build();
    expression.setSingular(true);
    return expression;
  }

  private ParsedExpression buildDecimalExpression(boolean leftOperand) {
    ParsedExpression expression = new PrimitiveExpressionBuilder(FHIRDefinedType.DECIMAL, DECIMAL)
        .withColumn("123abcd_id", DataTypes.StringType)
        .withColumn("123abcd", DataTypes.createDecimalType())
        .withRow("abc1", new BigDecimal("4.0"))
        .withRow("abc2", leftOperand ? null : new BigDecimal("4.0"))
        .withRow("abc3", leftOperand ? new BigDecimal("4.0") : null)
        .withRow("abc4", null)
        .build();
    expression.setSingular(true);
    return expression;
  }

  @Test
  public void addition() {
    BinaryOperatorInput input = new BinaryOperatorInput();
    input.setLeft(left);
    input.setRight(right);

    MathOperator mathOperator = new MathOperator(MathOperator.ADDITION);
    ParsedExpression result = mathOperator.invoke(input);
    Object value = leftOperandIsInteger
        ? 8
        : new BigDecimal("8.0");

    assertThat(result).selectResult().hasRows(
        RowFactory.create("abc1", value),
        RowFactory.create("abc2", leftTypeIsLiteral ? value : null),
        RowFactory.create("abc3", rightTypeIsLiteral ? value : null),
        RowFactory.create("abc4", null)
    );
  }

  @Test
  public void subtraction() {
    BinaryOperatorInput input = new BinaryOperatorInput();
    input.setLeft(left);
    input.setRight(right);

    MathOperator mathOperator = new MathOperator(MathOperator.SUBTRACTION);
    ParsedExpression result = mathOperator.invoke(input);
    Object value = leftOperandIsInteger
        ? 0
        : new BigDecimal("0.0");

    assertThat(result).selectResult().hasRows(
        RowFactory.create("abc1", value),
        RowFactory.create("abc2", leftTypeIsLiteral ? value : null),
        RowFactory.create("abc3", rightTypeIsLiteral ? value : null),
        RowFactory.create("abc4", null)
    );
  }

  @Test
  public void multiplication() {
    BinaryOperatorInput input = new BinaryOperatorInput();
    input.setLeft(left);
    input.setRight(right);

    MathOperator mathOperator = new MathOperator(MathOperator.MULTIPLICATION);
    ParsedExpression result = mathOperator.invoke(input);
    Object value = leftOperandIsInteger
        ? 16
        : new BigDecimal("16.0");

    assertThat(result).selectResult().hasRows(
        RowFactory.create("abc1", value),
        RowFactory.create("abc2", leftTypeIsLiteral ? value : null),
        RowFactory.create("abc3", rightTypeIsLiteral ? value : null),
        RowFactory.create("abc4", null)
    );
  }

  @Test
  public void division() {
    BinaryOperatorInput input = new BinaryOperatorInput();
    input.setLeft(left);
    input.setRight(right);

    MathOperator mathOperator = new MathOperator(MathOperator.DIVISION);
    ParsedExpression result = mathOperator.invoke(input);
    Object value = new BigDecimal("1.0");

    assertThat(result).selectResult().hasRows(
        RowFactory.create("abc1", value),
        RowFactory.create("abc2", leftTypeIsLiteral ? value : null),
        RowFactory.create("abc3", rightTypeIsLiteral ? value : null),
        RowFactory.create("abc4", null)
    );
  }

  @Test
  public void modulus() {
    BinaryOperatorInput input = new BinaryOperatorInput();
    input.setLeft(left);
    input.setRight(right);

    MathOperator mathOperator = new MathOperator(MathOperator.MODULUS);
    ParsedExpression result = mathOperator.invoke(input);
    Object value = 0;

    assertThat(result).selectResult().hasRows(
        RowFactory.create("abc1", value),
        RowFactory.create("abc2", leftTypeIsLiteral ? value : null),
        RowFactory.create("abc3", rightTypeIsLiteral ? value : null),
        RowFactory.create("abc4", null)
    );
  }

  @Test
  public void leftOperandIsNotCorrectType() {
    left.setFhirPathType(STRING);

    BinaryOperatorInput input = new BinaryOperatorInput();
    input.setLeft(left);
    input.setRight(right);

    MathOperator mathOperator = new MathOperator(MathOperator.ADDITION);
    assertThatExceptionOfType(InvalidRequestException.class)
        .isThrownBy(() -> mathOperator.invoke(input))
        .withMessage(
            "Left operand to + operator is of unsupported type, or is not singular: " + left
                .getFhirPath());
  }

  @Test
  public void rightOperandIsNotCorrectType() {
    right.setFhirPathType(STRING);

    BinaryOperatorInput input = new BinaryOperatorInput();
    input.setLeft(left);
    input.setRight(right);

    MathOperator mathOperator = new MathOperator(MathOperator.ADDITION);
    assertThatExceptionOfType(InvalidRequestException.class)
        .isThrownBy(() -> mathOperator.invoke(input))
        .withMessage(
            "Right operand to + operator is of unsupported type, or is not singular: "
                + right
                .getFhirPath());
  }

  @Test
  public void leftOperandIsNotSingular() {
    left.setSingular(false);

    BinaryOperatorInput input = new BinaryOperatorInput();
    input.setLeft(left);
    input.setRight(right);

    MathOperator mathOperator = new MathOperator(MathOperator.ADDITION);
    assertThatExceptionOfType(InvalidRequestException.class)
        .isThrownBy(() -> mathOperator.invoke(input))
        .withMessage(
            "Left operand to + operator is of unsupported type, or is not singular: " + left
                .getFhirPath());
  }

  @Test
  public void rightOperandIsNotSingular() {
    right.setSingular(false);

    BinaryOperatorInput input = new BinaryOperatorInput();
    input.setLeft(left);
    input.setRight(right);

    MathOperator mathOperator = new MathOperator(MathOperator.ADDITION);
    assertThatExceptionOfType(InvalidRequestException.class)
        .isThrownBy(() -> mathOperator.invoke(input))
        .withMessage(
            "Right operand to + operator is of unsupported type, or is not singular: " + right
                .getFhirPath());
  }

  @Test
  public void bothOperandsAreLiteral() {
    ParsedExpression literalLeft = literalInteger(1);
    ParsedExpression literalRight = literalInteger(1);

    BinaryOperatorInput input = new BinaryOperatorInput();
    input.setLeft(literalLeft);
    input.setRight(literalRight);
    input.setExpression("1 + 1");

    MathOperator mathOperator = new MathOperator(MathOperator.ADDITION);
    assertThatExceptionOfType(InvalidRequestException.class)
        .isThrownBy(() -> mathOperator.invoke(input))
        .withMessage(
            "Cannot have two literal operands to + operator: 1 + 1");
  }
}