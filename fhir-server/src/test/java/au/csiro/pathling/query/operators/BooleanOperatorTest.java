/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.query.operators;

import static au.csiro.pathling.test.Assertions.assertThat;
import static au.csiro.pathling.test.PrimitiveExpressionBuilder.literalBoolean;

import au.csiro.pathling.query.parsing.ParsedExpression;
import au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType;
import au.csiro.pathling.test.PrimitiveExpressionBuilder;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * @author John Grimes
 */
@Category(au.csiro.pathling.UnitTest.class)
public class BooleanOperatorTest {

  private ParsedExpression left;
  private ParsedExpression right;

  @Before
  public void setUp() {
    left = new PrimitiveExpressionBuilder(FHIRDefinedType.BOOLEAN, FhirPathType.BOOLEAN)
        .withColumn("123abcd_id", DataTypes.StringType)
        .withColumn("123abcd", DataTypes.BooleanType)
        .withRow("abc1", true)
        .withRow("abc2", true)
        .withRow("abc3", false)
        .withRow("abc4", false)
        .withRow("abc5", null)
        .withRow("abc6", null)
        .withRow("abc7", true)
        .withRow("abc8", null)
        .build();
    left.setSingular(true);
    left.setFhirPath("estimatedAge");

    right = new PrimitiveExpressionBuilder(FHIRDefinedType.BOOLEAN, FhirPathType.BOOLEAN)
        .withColumn("123abcd_id", DataTypes.StringType)
        .withColumn("123abcd", DataTypes.BooleanType)
        .withRow("abc1", false)
        .withRow("abc2", null)
        .withRow("abc3", true)
        .withRow("abc4", null)
        .withRow("abc5", true)
        .withRow("abc6", false)
        .withRow("abc7", true)
        .withRow("abc8", null)
        .build();
    right.setSingular(true);
    right.setFhirPath("deceasedBoolean");
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
    ParsedExpression literalLeft = literalBoolean(true);

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
    ParsedExpression literalRight = literalBoolean(true);

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
}