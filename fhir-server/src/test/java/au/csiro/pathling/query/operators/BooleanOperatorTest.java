/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.pathling.query.operators;

import static au.csiro.pathling.test.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import au.csiro.pathling.query.parsing.ParsedExpression;
import au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType;
import au.csiro.pathling.test.FunctionTest;
import au.csiro.pathling.test.PrimitiveRowFixture;
import au.csiro.pathling.test.RowListBuilder;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.FamilyMemberHistory;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * @author John Grimes
 */
@Category(au.csiro.pathling.UnitTest.class)
public class BooleanOperatorTest extends FunctionTest {

  private ParsedExpression left;
  private ParsedExpression right;

  @Before
  public void setUp() {
    super.setUp();

    StructType schema = PrimitiveRowFixture
        .createPrimitiveRowStruct(DataTypes.BooleanType);
    RowListBuilder rowListBuilder = new RowListBuilder();
    List<Row> leftRows = rowListBuilder
        .withRow("abc1", true)
        .withRow("abc2", true)
        .withRow("abc3", false)
        .withRow("abc4", false)
        .withRow("abc5", null)
        .withRow("abc6", null)
        .withRow("abc7", true)
        .withRow("abc8", null)
        .build();
    Dataset<Row> leftDataset = spark.createDataFrame(leftRows, schema);
    List<Row> rightRows = rowListBuilder
        .withRow("abc1", false)
        .withRow("abc2", null)
        .withRow("abc3", true)
        .withRow("abc4", null)
        .withRow("abc5", true)
        .withRow("abc6", false)
        .withRow("abc7", true)
        .withRow("abc8", null)
        .build();
    Dataset<Row> rightDataset = spark.createDataFrame(rightRows, schema);

    left = createPrimitiveParsedExpression(leftDataset, "estimatedAge",
        FamilyMemberHistory.class, FhirPathType.BOOLEAN, FHIRDefinedType.BOOLEAN);
    left.setSingular(true);
    right = createPrimitiveParsedExpression(rightDataset, "deceasedBoolean",
        FamilyMemberHistory.class, FhirPathType.BOOLEAN, FHIRDefinedType.BOOLEAN);
    right.setSingular(true);
  }

  @Test
  public void and() {
    BinaryOperatorInput input = new BinaryOperatorInput();
    input.setLeft(left);
    input.setRight(right);
    input.setExpression("estimatedAge and deceasedBoolean");

    BooleanOperator booleanOperator = new BooleanOperator(BooleanOperator.AND);
    ParsedExpression result = booleanOperator.invoke(input);

    assertThat(result).selectResult().hasRows(
        RowFactory.create("abc1", false),
        RowFactory.create("abc2", null),
        RowFactory.create("abc3", false),
        RowFactory.create("abc4", false),
        RowFactory.create("abc5", null),
        RowFactory.create("abc6", false),
        RowFactory.create("abc7", true),
        RowFactory.create("abc8", null)
    );
  }

  @Test
  public void or() {
    BinaryOperatorInput input = new BinaryOperatorInput();
    input.setLeft(left);
    input.setRight(right);
    input.setExpression("estimatedAge or deceasedBoolean");

    BooleanOperator booleanOperator = new BooleanOperator(BooleanOperator.OR);
    ParsedExpression result = booleanOperator.invoke(input);

    assertThat(result).selectResult().hasRows(
        RowFactory.create("abc1", true),
        RowFactory.create("abc2", true),
        RowFactory.create("abc3", true),
        RowFactory.create("abc4", null),
        RowFactory.create("abc5", true),
        RowFactory.create("abc6", null),
        RowFactory.create("abc7", true),
        RowFactory.create("abc8", null)
    );
  }

  @Test
  public void xor() {
    BinaryOperatorInput input = new BinaryOperatorInput();
    input.setLeft(left);
    input.setRight(right);
    input.setExpression("estimatedAge xor deceasedBoolean");

    BooleanOperator booleanOperator = new BooleanOperator(BooleanOperator.XOR);
    ParsedExpression result = booleanOperator.invoke(input);

    assertThat(result).selectResult().hasRows(
        RowFactory.create("abc1", true),
        RowFactory.create("abc2", null),
        RowFactory.create("abc3", true),
        RowFactory.create("abc4", null),
        RowFactory.create("abc5", null),
        RowFactory.create("abc6", null),
        RowFactory.create("abc7", false),
        RowFactory.create("abc8", null)
    );
  }

  @Test
  public void implies() {
    BinaryOperatorInput input = new BinaryOperatorInput();
    input.setLeft(left);
    input.setRight(right);
    input.setExpression("estimatedAge implies deceasedBoolean");

    BooleanOperator booleanOperator = new BooleanOperator(BooleanOperator.IMPLIES);
    ParsedExpression result = booleanOperator.invoke(input);

    assertThat(result).selectResult().hasRows(
        RowFactory.create("abc1", false),
        RowFactory.create("abc2", null),
        RowFactory.create("abc3", true),
        RowFactory.create("abc4", true),
        RowFactory.create("abc5", true),
        RowFactory.create("abc6", null),
        RowFactory.create("abc7", true),
        RowFactory.create("abc8", null)
    );
  }

  @Test
  public void leftIsLiteral() {
    ParsedExpression literalLeft = createLiteralExpression(true);

    BinaryOperatorInput input = new BinaryOperatorInput();
    input.setLeft(literalLeft);
    input.setRight(right);
    input.setExpression("estimatedAge and deceasedBoolean");

    BooleanOperator booleanOperator = new BooleanOperator(BooleanOperator.AND);
    ParsedExpression result = booleanOperator.invoke(input);

    assertThat(result).selectResult().hasRows(
        RowFactory.create("abc1", false),
        RowFactory.create("abc2", null),
        RowFactory.create("abc3", true),
        RowFactory.create("abc4", null),
        RowFactory.create("abc5", true),
        RowFactory.create("abc6", false),
        RowFactory.create("abc7", true),
        RowFactory.create("abc8", null)
    );
  }

  @Test
  public void rightIsLiteral() {
    ParsedExpression literalRight = createLiteralExpression(true);

    BinaryOperatorInput input = new BinaryOperatorInput();
    input.setLeft(left);
    input.setRight(literalRight);
    input.setExpression("estimatedAge and deceasedBoolean");

    BooleanOperator booleanOperator = new BooleanOperator(BooleanOperator.AND);
    ParsedExpression result = booleanOperator.invoke(input);

    assertThat(result).selectResult().hasRows(
        RowFactory.create("abc1", true),
        RowFactory.create("abc2", true),
        RowFactory.create("abc3", false),
        RowFactory.create("abc4", false),
        RowFactory.create("abc5", null),
        RowFactory.create("abc6", null),
        RowFactory.create("abc7", true),
        RowFactory.create("abc8", null)
    );
  }

  @Test
  public void leftOperandIsNotSingular() {
    left.setSingular(false);

    BinaryOperatorInput input = new BinaryOperatorInput();
    input.setLeft(left);
    input.setRight(right);
    input.setExpression("estimatedAge and deceasedBoolean");

    BooleanOperator booleanOperator = new BooleanOperator(BooleanOperator.AND);
    assertThatExceptionOfType(InvalidRequestException.class)
        .isThrownBy(() -> booleanOperator.invoke(input))
        .withMessage("Left operand to and operator must be singular Boolean: estimatedAge");
  }

  @Test
  public void rightOperandIsNotSingular() {
    right.setSingular(false);

    BinaryOperatorInput input = new BinaryOperatorInput();
    input.setLeft(left);
    input.setRight(right);
    input.setExpression("estimatedAge and deceasedBoolean");

    BooleanOperator booleanOperator = new BooleanOperator(BooleanOperator.AND);
    assertThatExceptionOfType(InvalidRequestException.class)
        .isThrownBy(() -> booleanOperator.invoke(input))
        .withMessage("Right operand to and operator must be singular Boolean: deceasedBoolean");
  }

  @Test
  public void bothOperandsAreLiteral() {
    ParsedExpression literalLeft = createLiteralExpression(true);
    ParsedExpression literalRight = createLiteralExpression(true);

    BinaryOperatorInput input = new BinaryOperatorInput();
    input.setLeft(literalLeft);
    input.setRight(literalRight);
    input.setExpression("estimatedAge and deceasedBoolean");

    BooleanOperator booleanOperator = new BooleanOperator(BooleanOperator.AND);
    assertThatExceptionOfType(InvalidRequestException.class)
        .isThrownBy(() -> booleanOperator.invoke(input))
        .withMessage(
            "Cannot have two literal operands to and operator: estimatedAge and deceasedBoolean");
  }
}