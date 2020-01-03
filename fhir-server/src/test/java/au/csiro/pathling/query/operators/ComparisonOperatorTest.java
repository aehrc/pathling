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
public class ComparisonOperatorTest {

  @Parameters(name = "{0}")
  public static Object[] parameters() {
    return new Object[]{
        "String",
        "Integer",
        "Decimal",
        "DateTime",
        "Date",
        "Date (YYYY-MM)",
        "Date (YYYY)"
    };
  }

  private final String dataType;
  private ParsedExpression left;
  private ParsedExpression right;
  private ParsedExpression literal;

  public ComparisonOperatorTest(String dataType) {
    this.dataType = dataType;
  }

  @Before
  public void setUp() {
    switch (dataType) {
      case "String":
        buildStringExpressions();
        break;
      case "Integer":
        buildIntegerExpressions();
        break;
      case "Decimal":
        buildDecimalExpressions();
        break;
      case "DateTime":
        buildDateTimeExpressions(
            "2015-02-07T13:28:17-05:00",
            "2015-02-08T13:28:17-05:00",
            DATE_TIME, FHIRDefinedType.DATETIME);
        break;
      case "Date":
        buildDateTimeExpressions(
            "2015-02-07",
            "2015-02-08",
            DATE, FHIRDefinedType.DATE);
        break;
      case "Date (YYYY-MM)":
        buildDateTimeExpressions(
            "2015-02",
            "2015-03",
            DATE, FHIRDefinedType.DATE);
        break;
      case "Date (YYYY)":
        buildDateTimeExpressions(
            "2015",
            "2016",
            DATE, FHIRDefinedType.DATE);
        break;
      default:
        throw new RuntimeException("Invalid data type");
    }
  }

  private void buildStringExpressions() {
    left = new PrimitiveExpressionBuilder(FHIRDefinedType.STRING, STRING)
        .withColumn("123abcd_id", DataTypes.StringType)
        .withColumn("123abcd", DataTypes.StringType)
        .withRow("abc1", "Evelyn")
        .withRow("abc2", "Evelyn")
        .withRow("abc3", "Jude")
        .withRow("abc4", null)
        .withRow("abc5", "Evelyn")
        .withRow("abc6", null)
        .build();
    left.setSingular(true);
    right = new PrimitiveExpressionBuilder(FHIRDefinedType.STRING, STRING)
        .withColumn("123abcd_id", DataTypes.StringType)
        .withColumn("123abcd", DataTypes.StringType)
        .withRow("abc1", "Evelyn")
        .withRow("abc2", "Jude")
        .withRow("abc3", "Evelyn")
        .withRow("abc4", "Evelyn")
        .withRow("abc5", null)
        .withRow("abc6", null)
        .build();
    right.setSingular(true);
    literal = literalString("Evelyn");
  }

  private void buildIntegerExpressions() {
    left = new PrimitiveExpressionBuilder(FHIRDefinedType.INTEGER, INTEGER)
        .withColumn("123abcd_id", DataTypes.StringType)
        .withColumn("123abcd", DataTypes.IntegerType)
        .withRow("abc1", 1)
        .withRow("abc2", 1)
        .withRow("abc3", 2)
        .withRow("abc4", null)
        .withRow("abc5", 1)
        .withRow("abc6", null)
        .build();
    left.setSingular(true);
    right = new PrimitiveExpressionBuilder(FHIRDefinedType.INTEGER, INTEGER)
        .withColumn("123abcd_id", DataTypes.StringType)
        .withColumn("123abcd", DataTypes.IntegerType)
        .withRow("abc1", 1)
        .withRow("abc2", 2)
        .withRow("abc3", 1)
        .withRow("abc4", 1)
        .withRow("abc5", null)
        .withRow("abc6", null)
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
        .withRow("abc3", new BigDecimal("2.0"))
        .withRow("abc4", null)
        .withRow("abc5", new BigDecimal("1.0"))
        .withRow("abc6", null)
        .build();
    left.setSingular(true);
    right = new PrimitiveExpressionBuilder(FHIRDefinedType.DECIMAL, DECIMAL)
        .withColumn("123abcd_id", DataTypes.StringType)
        .withColumn("123abcd", DataTypes.createDecimalType())
        .withRow("abc1", new BigDecimal("1.0"))
        .withRow("abc2", new BigDecimal("2.0"))
        .withRow("abc3", new BigDecimal("1.0"))
        .withRow("abc4", new BigDecimal("1.0"))
        .withRow("abc5", null)
        .withRow("abc6", null)
        .build();
    right.setSingular(true);
    literal = literalDecimal(new BigDecimal("1.0"));
  }

  private void buildDateTimeExpressions(String lesserDate, String greaterDate,
      FhirPathType fhirPathType, FHIRDefinedType fhirType) {
    left = new PrimitiveExpressionBuilder(fhirType, fhirPathType)
        .withColumn("123abcd_id", DataTypes.StringType)
        .withColumn("123abcd", DataTypes.StringType)
        .withRow("abc1", lesserDate)
        .withRow("abc2", lesserDate)
        .withRow("abc3", greaterDate)
        .withRow("abc4", null)
        .withRow("abc5", lesserDate)
        .withRow("abc6", null)
        .build();
    left.setSingular(true);
    right = new PrimitiveExpressionBuilder(fhirType, fhirPathType)
        .withColumn("123abcd_id", DataTypes.StringType)
        .withColumn("123abcd", DataTypes.StringType)
        .withRow("abc1", lesserDate)
        .withRow("abc2", greaterDate)
        .withRow("abc3", lesserDate)
        .withRow("abc4", lesserDate)
        .withRow("abc5", null)
        .withRow("abc6", null)
        .build();
    right.setSingular(true);
    literal = (fhirPathType == DATE_TIME)
        ? literalDateTime(lesserDate)
        : literalDate(lesserDate);
  }

  @Test
  public void lessThanOrEqualTo() {
    BinaryOperatorInput input = new BinaryOperatorInput();
    input.setLeft(left);
    input.setRight(right);

    ComparisonOperator comparisonOperator = new ComparisonOperator(
        ComparisonOperator.LESS_THAN_OR_EQUAL_TO);
    ParsedExpression result = comparisonOperator.invoke(input);

    assertThat(result).selectResult().hasRows(
        RowFactory.create("abc1", true),
        RowFactory.create("abc2", true),
        RowFactory.create("abc3", false),
        RowFactory.create("abc4", null),
        RowFactory.create("abc5", null),
        RowFactory.create("abc6", null)
    );
  }

  @Test
  public void lessThan() {
    BinaryOperatorInput input = new BinaryOperatorInput();
    input.setLeft(left);
    input.setRight(right);

    ComparisonOperator comparisonOperator = new ComparisonOperator(
        ComparisonOperator.LESS_THAN);
    ParsedExpression result = comparisonOperator.invoke(input);

    assertThat(result).selectResult().hasRows(
        RowFactory.create("abc1", false),
        RowFactory.create("abc2", true),
        RowFactory.create("abc3", false),
        RowFactory.create("abc4", null),
        RowFactory.create("abc5", null),
        RowFactory.create("abc6", null)
    );
  }

  @Test
  public void greaterThanOrEqualTo() {
    BinaryOperatorInput input = new BinaryOperatorInput();
    input.setLeft(left);
    input.setRight(right);

    ComparisonOperator comparisonOperator = new ComparisonOperator(
        ComparisonOperator.GREATER_THAN_OR_EQUAL_TO);
    ParsedExpression result = comparisonOperator.invoke(input);

    assertThat(result).selectResult().hasRows(
        RowFactory.create("abc1", true),
        RowFactory.create("abc2", false),
        RowFactory.create("abc3", true),
        RowFactory.create("abc4", null),
        RowFactory.create("abc5", null),
        RowFactory.create("abc6", null)
    );
  }

  @Test
  public void greaterThan() {
    BinaryOperatorInput input = new BinaryOperatorInput();
    input.setLeft(left);
    input.setRight(right);

    ComparisonOperator comparisonOperator = new ComparisonOperator(
        ComparisonOperator.GREATER_THAN);
    ParsedExpression result = comparisonOperator.invoke(input);

    assertThat(result).selectResult().hasRows(
        RowFactory.create("abc1", false),
        RowFactory.create("abc2", false),
        RowFactory.create("abc3", true),
        RowFactory.create("abc4", null),
        RowFactory.create("abc5", null),
        RowFactory.create("abc6", null)
    );
  }

  @Test
  public void literalLessThanOrEqualTo() {
    BinaryOperatorInput input = new BinaryOperatorInput();
    input.setLeft(literal);
    input.setRight(right);

    ComparisonOperator comparisonOperator = new ComparisonOperator(
        ComparisonOperator.LESS_THAN_OR_EQUAL_TO);
    ParsedExpression result = comparisonOperator.invoke(input);

    assertThat(result).selectResult().hasRows(
        RowFactory.create("abc1", true),
        RowFactory.create("abc2", true),
        RowFactory.create("abc3", true),
        RowFactory.create("abc4", true),
        RowFactory.create("abc5", null),
        RowFactory.create("abc6", null)
    );
  }

  @Test
  public void literalLessThan() {
    BinaryOperatorInput input = new BinaryOperatorInput();
    input.setLeft(literal);
    input.setRight(right);

    ComparisonOperator comparisonOperator = new ComparisonOperator(
        ComparisonOperator.LESS_THAN);
    ParsedExpression result = comparisonOperator.invoke(input);

    assertThat(result).selectResult().hasRows(
        RowFactory.create("abc1", false),
        RowFactory.create("abc2", true),
        RowFactory.create("abc3", false),
        RowFactory.create("abc4", false),
        RowFactory.create("abc5", null),
        RowFactory.create("abc6", null)
    );
  }

  @Test
  public void literalGreaterThanOrEqualTo() {
    BinaryOperatorInput input = new BinaryOperatorInput();
    input.setLeft(literal);
    input.setRight(right);

    ComparisonOperator comparisonOperator = new ComparisonOperator(
        ComparisonOperator.GREATER_THAN_OR_EQUAL_TO);
    ParsedExpression result = comparisonOperator.invoke(input);

    assertThat(result).selectResult().hasRows(
        RowFactory.create("abc1", true),
        RowFactory.create("abc2", false),
        RowFactory.create("abc3", true),
        RowFactory.create("abc4", true),
        RowFactory.create("abc5", null),
        RowFactory.create("abc6", null)
    );
  }

  @Test
  public void literalGreaterThan() {
    BinaryOperatorInput input = new BinaryOperatorInput();
    input.setLeft(literal);
    input.setRight(right);

    ComparisonOperator comparisonOperator = new ComparisonOperator(
        ComparisonOperator.GREATER_THAN);
    ParsedExpression result = comparisonOperator.invoke(input);

    assertThat(result).selectResult().hasRows(
        RowFactory.create("abc1", false),
        RowFactory.create("abc2", false),
        RowFactory.create("abc3", false),
        RowFactory.create("abc4", false),
        RowFactory.create("abc5", null),
        RowFactory.create("abc6", null)
    );
  }

  @Test
  public void lessThanOrEqualToLiteral() {
    BinaryOperatorInput input = new BinaryOperatorInput();
    input.setLeft(left);
    input.setRight(literal);

    ComparisonOperator comparisonOperator = new ComparisonOperator(
        ComparisonOperator.LESS_THAN_OR_EQUAL_TO);
    ParsedExpression result = comparisonOperator.invoke(input);

    assertThat(result).selectResult().hasRows(
        RowFactory.create("abc1", true),
        RowFactory.create("abc2", true),
        RowFactory.create("abc3", false),
        RowFactory.create("abc4", null),
        RowFactory.create("abc5", true),
        RowFactory.create("abc6", null)
    );
  }

  @Test
  public void lessThanLiteral() {
    BinaryOperatorInput input = new BinaryOperatorInput();
    input.setLeft(left);
    input.setRight(literal);

    ComparisonOperator comparisonOperator = new ComparisonOperator(
        ComparisonOperator.LESS_THAN);
    ParsedExpression result = comparisonOperator.invoke(input);

    assertThat(result).selectResult().hasRows(
        RowFactory.create("abc1", false),
        RowFactory.create("abc2", false),
        RowFactory.create("abc3", false),
        RowFactory.create("abc4", null),
        RowFactory.create("abc5", false),
        RowFactory.create("abc6", null)
    );
  }

  @Test
  public void greaterThanOrEqualToLiteral() {
    BinaryOperatorInput input = new BinaryOperatorInput();
    input.setLeft(left);
    input.setRight(literal);

    ComparisonOperator comparisonOperator = new ComparisonOperator(
        ComparisonOperator.GREATER_THAN_OR_EQUAL_TO);
    ParsedExpression result = comparisonOperator.invoke(input);

    assertThat(result).selectResult().hasRows(
        RowFactory.create("abc1", true),
        RowFactory.create("abc2", true),
        RowFactory.create("abc3", true),
        RowFactory.create("abc4", null),
        RowFactory.create("abc5", true),
        RowFactory.create("abc6", null)
    );
  }

  @Test
  public void greaterThanLiteral() {
    BinaryOperatorInput input = new BinaryOperatorInput();
    input.setLeft(left);
    input.setRight(literal);

    ComparisonOperator comparisonOperator = new ComparisonOperator(
        ComparisonOperator.GREATER_THAN);
    ParsedExpression result = comparisonOperator.invoke(input);

    assertThat(result).selectResult().hasRows(
        RowFactory.create("abc1", false),
        RowFactory.create("abc2", false),
        RowFactory.create("abc3", true),
        RowFactory.create("abc4", null),
        RowFactory.create("abc5", false),
        RowFactory.create("abc6", null)
    );
  }

}

