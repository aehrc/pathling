/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.pathling.query.operators;

import static au.csiro.pathling.test.Assertions.assertThat;

import au.csiro.pathling.query.parsing.ParsedExpression;
import au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType;
import au.csiro.pathling.test.DatasetBuilder;
import au.csiro.pathling.test.PrimitiveExpressionBuilder;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * @author John Grimes
 */
@Category(au.csiro.pathling.UnitTest.class)
public class EqualityOperatorTest {

  @Test
  public void stringEqualsLiteralString() {
    ParsedExpression left = new PrimitiveExpressionBuilder(FHIRDefinedType.STRING,
        FhirPathType.STRING)
        .withColumn("123abcd_id", DataTypes.StringType)
        .withColumn("123abcd", DataTypes.StringType)
        .withRow("abc1", "female")
        .withRow("abc2", "male")
        .withRow("abc3", null)
        .build();
    left.setSingular(true);
    ParsedExpression right = PrimitiveExpressionBuilder.literalString("female");

    BinaryOperatorInput equalityInput = new BinaryOperatorInput();
    equalityInput.setLeft(left);
    equalityInput.setRight(right);
    equalityInput.setExpression("gender = 'female'");

    // Execute the equality operator function.
    EqualityOperator equalityOperator = new EqualityOperator("=");
    ParsedExpression result = equalityOperator.invoke(equalityInput);

    // Check the result.
    assertThat(result).hasFhirPath("gender = 'female'");
    assertThat(result).isOfType(FHIRDefinedType.BOOLEAN, FhirPathType.BOOLEAN);
    assertThat(result).isPrimitive();
    assertThat(result).isSingular();
    assertThat(result).isSelection();

    // Check that collecting the dataset result yields the correct values.
    Dataset<Row> expectedDataset = new DatasetBuilder()
        .withColumn("123abcd_id", DataTypes.StringType)
        .withColumn("123abcd", DataTypes.BooleanType)
        .withRow("abc1", true)
        .withRow("abc2", false)
        .withRow("abc3", false)
        .build();
    assertThat(result)
        .selectResult()
        .hasRows(expectedDataset);
  }

}
