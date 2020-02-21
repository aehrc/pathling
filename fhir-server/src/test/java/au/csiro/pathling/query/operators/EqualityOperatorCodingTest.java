/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.query.operators;

import static au.csiro.pathling.TestUtilities.LOINC_URL;
import static au.csiro.pathling.TestUtilities.SNOMED_URL;
import static au.csiro.pathling.TestUtilities.codingStructType;
import static au.csiro.pathling.TestUtilities.rowFromCoding;
import static au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType.CODING;
import static au.csiro.pathling.test.Assertions.assertThat;
import static au.csiro.pathling.test.PrimitiveExpressionBuilder.literalCoding;

import au.csiro.pathling.query.parsing.ParsedExpression;
import au.csiro.pathling.test.PrimitiveExpressionBuilder;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * @author John Grimes
 */
@Category(au.csiro.pathling.UnitTest.class)
public class EqualityOperatorCodingTest {

  private ParsedExpression left;
  private ParsedExpression right;
  private ParsedExpression literal;

  @Before
  public void setUp() {
    Coding coding1 = new Coding(SNOMED_URL, "56459004", null);
    coding1.setVersion("http://snomed.info/sct/32506021000036107/version/20191231");
    Coding coding2 = new Coding(SNOMED_URL, "56459004", null);
    Coding coding3 = new Coding(LOINC_URL, "57711-4", null);
    coding3.setVersion("2.67");
    Coding coding4 = new Coding(LOINC_URL, "57711-4", null);
    coding4.setVersion("2.29");

    left = new PrimitiveExpressionBuilder(FHIRDefinedType.CODING, CODING)
        .withColumn("123abcd_id", DataTypes.StringType)
        .withStructTypeColumns(codingStructType())
        .withRow("abc1", rowFromCoding(coding1))
        .withRow("abc2", rowFromCoding(coding2))
        .withRow("abc3", rowFromCoding(coding3))
        .withRow("abc4", rowFromCoding(coding3))
        .withRow("abc5", null)
        .withRow("abc6", rowFromCoding(coding1))
        .withRow("abc7", null)
        .buildWithStructValue("123abcd");
    left.setSingular(true);
    right = new PrimitiveExpressionBuilder(FHIRDefinedType.CODING, CODING)
        .withColumn("123abcd_id", DataTypes.StringType)
        .withStructTypeColumns(codingStructType())
        .withRow("abc1", rowFromCoding(coding1))
        .withRow("abc2", rowFromCoding(coding1))
        .withRow("abc3", rowFromCoding(coding4))
        .withRow("abc4", rowFromCoding(coding2))
        .withRow("abc5", rowFromCoding(coding1))
        .withRow("abc6", null)
        .withRow("abc7", null)
        .buildWithStructValue("123abcd");
    right.setSingular(true);
    literal = literalCoding(coding1);
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
        RowFactory.create("abc2", true),
        RowFactory.create("abc3", false),
        RowFactory.create("abc4", false),
        RowFactory.create("abc5", null),
        RowFactory.create("abc6", null),
        RowFactory.create("abc7", null)
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
        RowFactory.create("abc2", false),
        RowFactory.create("abc3", true),
        RowFactory.create("abc4", true),
        RowFactory.create("abc5", null),
        RowFactory.create("abc6", null),
        RowFactory.create("abc7", null)
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
        RowFactory.create("abc2", true),
        RowFactory.create("abc3", false),
        RowFactory.create("abc4", true),
        RowFactory.create("abc5", true),
        RowFactory.create("abc6", null),
        RowFactory.create("abc7", null)
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
        RowFactory.create("abc3", false),
        RowFactory.create("abc4", false),
        RowFactory.create("abc5", null),
        RowFactory.create("abc6", true),
        RowFactory.create("abc7", null)
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
        RowFactory.create("abc2", false),
        RowFactory.create("abc3", true),
        RowFactory.create("abc4", false),
        RowFactory.create("abc5", false),
        RowFactory.create("abc6", null),
        RowFactory.create("abc7", null)
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
        RowFactory.create("abc3", true),
        RowFactory.create("abc4", true),
        RowFactory.create("abc5", null),
        RowFactory.create("abc6", false),
        RowFactory.create("abc7", null)
    );
  }

}

