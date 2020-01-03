/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.pathling.query.operators;

import static au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType.*;
import static au.csiro.pathling.test.Assertions.assertThat;
import static au.csiro.pathling.test.PrimitiveExpressionBuilder.*;

import au.csiro.pathling.query.parsing.ParsedExpression;
import au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType;
import au.csiro.pathling.test.PrimitiveExpressionBuilder;
import java.math.BigDecimal;
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
public class EqualityOperatorTest {

  @Parameters(name = "{0}")
  public static Object[] parameters() {
    return new Object[]{
        STRING,
        INTEGER,
        DECIMAL,
        BOOLEAN,
        DATE,
        DATE_TIME
    };
  }

  private final FhirPathType dataType;
  private ParsedExpression left;
  private ParsedExpression right;
  private ParsedExpression literal;

  public EqualityOperatorTest(FhirPathType dataType) {
    this.dataType = dataType;
  }

  @Before
  public void setUp() {
    switch (dataType) {
      case STRING:
        buildStringExpressions(
            "Evelyn",
            "Jude",
            literalString("Evelyn"),
            STRING, FHIRDefinedType.STRING
        );
        break;
      case INTEGER:
        buildIntegerExpressions();
        break;
      case DECIMAL:
        buildDecimalExpressions();
        break;
      case BOOLEAN:
        buildBooleanExpressions();
        break;
      case DATE:
        buildStringExpressions(
            "2015-02-07",
            "2015-02-08",
            literalDate("2015-02-07"),
            DATE, FHIRDefinedType.DATE
        );
        break;
      case DATE_TIME:
        buildStringExpressions(
            "2015-02-07T13:28:17-05:00",
            "2015-02-08T13:28:17-05:00",
            literalDateTime("2015-02-07T13:28:17-05:00"),
            DATE_TIME, FHIRDefinedType.DATETIME
        );
        break;
      default:
        throw new RuntimeException("Invalid data type");
    }
  }

  private void buildStringExpressions(String value1, String value2, ParsedExpression literal,
      FhirPathType fhirPathType, FHIRDefinedType fhirType) {
    left = new PrimitiveExpressionBuilder(fhirType, fhirPathType)
        .withColumn("123abcd_id", DataTypes.StringType)
        .withColumn("123abcd", DataTypes.StringType)
        .withRow("abc1", value1)
        .withRow("abc2", value1)
        .withRow("abc3", null)
        .withRow("abc4", value1)
        .withRow("abc5", null)
        .build();
    left.setSingular(true);
    right = new PrimitiveExpressionBuilder(fhirType, fhirPathType)
        .withColumn("123abcd_id", DataTypes.StringType)
        .withColumn("123abcd", DataTypes.StringType)
        .withRow("abc1", value1)
        .withRow("abc2", value2)
        .withRow("abc3", value1)
        .withRow("abc4", null)
        .withRow("abc5", null)
        .build();
    right.setSingular(true);
    this.literal = literal;
  }

  private void buildIntegerExpressions() {
    left = new PrimitiveExpressionBuilder(FHIRDefinedType.INTEGER, INTEGER)
        .withColumn("123abcd_id", DataTypes.StringType)
        .withColumn("123abcd", DataTypes.IntegerType)
        .withRow("abc1", 1)
        .withRow("abc2", 1)
        .withRow("abc3", null)
        .withRow("abc4", 1)
        .withRow("abc5", null)
        .build();
    left.setSingular(true);
    right = new PrimitiveExpressionBuilder(FHIRDefinedType.INTEGER, INTEGER)
        .withColumn("123abcd_id", DataTypes.StringType)
        .withColumn("123abcd", DataTypes.IntegerType)
        .withRow("abc1", 1)
        .withRow("abc2", 2)
        .withRow("abc3", 1)
        .withRow("abc4", null)
        .withRow("abc5", null)
        .build();
    right.setSingular(true);
    literal = literalInteger(1);
  }

  private void buildDecimalExpressions() {
    left = new PrimitiveExpressionBuilder(FHIRDefinedType.DECIMAL, DECIMAL)
        .withColumn("123abcd_id", DataTypes.StringType)
        .withColumn("123abcd", DataTypes.createDecimalType())
        .withRow("abc1", new BigDecimal("1.0"))
        .withRow("abc2", new BigDecimal("1.0"))
        .withRow("abc3", null)
        .withRow("abc4", new BigDecimal("1.0"))
        .withRow("abc5", null)
        .build();
    left.setSingular(true);
    right = new PrimitiveExpressionBuilder(FHIRDefinedType.DECIMAL, DECIMAL)
        .withColumn("123abcd_id", DataTypes.StringType)
        .withColumn("123abcd", DataTypes.createDecimalType())
        .withRow("abc1", new BigDecimal("1.0"))
        .withRow("abc2", new BigDecimal("2.0"))
        .withRow("abc3", new BigDecimal("1.0"))
        .withRow("abc4", null)
        .withRow("abc5", null)
        .build();
    right.setSingular(true);
    literal = literalDecimal(new BigDecimal("1.0"));
  }

  private void buildBooleanExpressions() {
    left = new PrimitiveExpressionBuilder(FHIRDefinedType.BOOLEAN, BOOLEAN)
        .withColumn("123abcd_id", DataTypes.StringType)
        .withColumn("123abcd", DataTypes.BooleanType)
        .withRow("abc1", true)
        .withRow("abc2", true)
        .withRow("abc3", null)
        .withRow("abc4", true)
        .withRow("abc5", null)
        .build();
    left.setSingular(true);
    right = new PrimitiveExpressionBuilder(FHIRDefinedType.BOOLEAN, BOOLEAN)
        .withColumn("123abcd_id", DataTypes.StringType)
        .withColumn("123abcd", DataTypes.BooleanType)
        .withRow("abc1", true)
        .withRow("abc2", false)
        .withRow("abc3", true)
        .withRow("abc4", null)
        .withRow("abc5", null)
        .build();
    right.setSingular(true);
    literal = literalBoolean(true);
  }

  @Test
  public void equals() {
    BinaryOperatorInput input = new BinaryOperatorInput();
    input.setLeft(left);
    input.setRight(right);

    EqualityOperator equalityOperator = new EqualityOperator(
        EqualityOperator.EQUALS);
    ParsedExpression result = equalityOperator.invoke(input);

    assertThat(result).selectResult().hasRows(
        RowFactory.create("abc1", true),
        RowFactory.create("abc2", false),
        RowFactory.create("abc3", null),
        RowFactory.create("abc4", null),
        RowFactory.create("abc5", null)
    );
  }

  @Test
  public void notEquals() {
    BinaryOperatorInput input = new BinaryOperatorInput();
    input.setLeft(left);
    input.setRight(right);

    EqualityOperator equalityOperator = new EqualityOperator(
        EqualityOperator.NOT_EQUALS);
    ParsedExpression result = equalityOperator.invoke(input);

    assertThat(result).selectResult().hasRows(
        RowFactory.create("abc1", false),
        RowFactory.create("abc2", true),
        RowFactory.create("abc3", null),
        RowFactory.create("abc4", null),
        RowFactory.create("abc5", null)
    );
  }

  @Test
  public void literalEquals() {
    BinaryOperatorInput input = new BinaryOperatorInput();
    input.setLeft(literal);
    input.setRight(right);

    EqualityOperator equalityOperator = new EqualityOperator(
        EqualityOperator.EQUALS);
    ParsedExpression result = equalityOperator.invoke(input);

    assertThat(result).selectResult().hasRows(
        RowFactory.create("abc1", true),
        RowFactory.create("abc2", false),
        RowFactory.create("abc3", true),
        RowFactory.create("abc4", null),
        RowFactory.create("abc5", null)
    );
  }

  @Test
  public void equalsLiteral() {
    BinaryOperatorInput input = new BinaryOperatorInput();
    input.setLeft(left);
    input.setRight(literal);

    EqualityOperator equalityOperator = new EqualityOperator(
        EqualityOperator.EQUALS);
    ParsedExpression result = equalityOperator.invoke(input);

    assertThat(result).selectResult().hasRows(
        RowFactory.create("abc1", true),
        RowFactory.create("abc2", true),
        RowFactory.create("abc3", null),
        RowFactory.create("abc4", true),
        RowFactory.create("abc5", null)
    );
  }

  @Test
  public void literalNotEquals() {
    BinaryOperatorInput input = new BinaryOperatorInput();
    input.setLeft(literal);
    input.setRight(right);

    EqualityOperator equalityOperator = new EqualityOperator(
        EqualityOperator.NOT_EQUALS);
    ParsedExpression result = equalityOperator.invoke(input);

    assertThat(result).selectResult().hasRows(
        RowFactory.create("abc1", false),
        RowFactory.create("abc2", true),
        RowFactory.create("abc3", false),
        RowFactory.create("abc4", null),
        RowFactory.create("abc5", null)
    );
  }

  @Test
  public void notEqualsLiteral() {
    BinaryOperatorInput input = new BinaryOperatorInput();
    input.setLeft(left);
    input.setRight(literal);

    EqualityOperator equalityOperator = new EqualityOperator(
        EqualityOperator.NOT_EQUALS);
    ParsedExpression result = equalityOperator.invoke(input);

    assertThat(result).selectResult().hasRows(
        RowFactory.create("abc1", false),
        RowFactory.create("abc2", false),
        RowFactory.create("abc3", null),
        RowFactory.create("abc4", false),
        RowFactory.create("abc5", null)
    );
  }

}
