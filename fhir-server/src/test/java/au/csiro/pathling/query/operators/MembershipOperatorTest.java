/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.query.operators;

import static au.csiro.pathling.TestUtilities.getSparkSession;
import static au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType.CODING;
import static au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType.STRING;
import static au.csiro.pathling.test.Assertions.assertThat;
import static au.csiro.pathling.test.PrimitiveExpressionBuilder.literalBoolean;
import static au.csiro.pathling.test.PrimitiveExpressionBuilder.literalCoding;
import static au.csiro.pathling.test.PrimitiveExpressionBuilder.literalString;
import static au.csiro.pathling.test.fixtures.StringPrimitiveRowFixture.*;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import au.csiro.pathling.query.parsing.ParsedExpression;
import au.csiro.pathling.test.CodingRowFixture;
import au.csiro.pathling.test.PrimitiveExpressionBuilder;
import au.csiro.pathling.test.fixtures.StringPrimitiveRowFixture;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * @author Piotr Szul
 */
@Category(au.csiro.pathling.UnitTest.class)
@RunWith(Parameterized.class)
public class MembershipOperatorTest {

  private final SparkSession spark;

  @Parameters(name = "{index}:{0}()")
  public static Object[] data() {
    return new Object[]{"in", "contains"};
  }

  private final String operator;
  private final BinaryOperatorInput operatorInput = new BinaryOperatorInput();

  public MembershipOperatorTest(String operator) {
    spark = getSparkSession();
    this.operator = operator;
  }

  private String resolveOperator(String string) {
    return string.replace("%op%", operator);
  }

  protected ParsedExpression testOperator(ParsedExpression collection, ParsedExpression element) {
    if ("in".equals(operator)) {
      operatorInput.setLeft(element);
      operatorInput.setRight(collection);
    } else if ("contains".equals(operator)) {
      operatorInput.setLeft(collection);
      operatorInput.setRight(element);
    } else {
      throw new IllegalArgumentException("Membership operator '" + operator + "' cannot be tested");
    }

    operatorInput.setExpression(operatorInput.getLeft().getFhirPath() + " " + operator + " "
        + operatorInput.getRight().getFhirPath());

    ParsedExpression result = new MembershipOperator(operator).invoke(operatorInput);
    assertThat(result).isResultFor(operatorInput).isOfBooleanType().isSingular().isSelection();
    return result;
  }

  @Test
  public void returnsCorrectResultWhenElementIsLiteral() {
    ParsedExpression collection = new PrimitiveExpressionBuilder(FHIRDefinedType.STRING, STRING)
        .withDataset(StringPrimitiveRowFixture.createCompleteDataset(spark))
        .build();
    ParsedExpression element = literalString("Samuel");

    // "name.family.%op%('Samuel')
    ParsedExpression result = testOperator(collection, element);

    assertThat(result).selectResult().hasRows(RowFactory.create(ROW_ID_1, false),
        RowFactory.create(ROW_ID_2, true), RowFactory.create(ROW_ID_3, false),
        RowFactory.create(ROW_ID_4, false), RowFactory.create(ROW_ID_5, false));
  }

  @Test
  public void returnsCorrectResultWhenElementIsExpression() {
    ParsedExpression collection = new PrimitiveExpressionBuilder(FHIRDefinedType.STRING, STRING)
        .withDataset(StringPrimitiveRowFixture.createCompleteDataset(spark))
        .build();
    ParsedExpression element = new PrimitiveExpressionBuilder(FHIRDefinedType.STRING, STRING)
        .withDataset(StringPrimitiveRowFixture.createDataset(spark,
            RowFactory.create(ROW_ID_1, "Eva"), STRING_2_SAMUEL, STRING_3_NULL,
            STRING_4_ADAM, STRING_5_NULL))
        .build();
    element.setSingular(true);
    element.setFhirPath("name.family.first()");

    // name.family.%op%(name.family.first())
    ParsedExpression result = testOperator(collection, element);

    assertThat(result).selectResult().hasRows(RowFactory.create(ROW_ID_1, false),
        RowFactory.create(ROW_ID_2, true), RowFactory.create(ROW_ID_3, null),
        RowFactory.create(ROW_ID_4, true), RowFactory.create(ROW_ID_5, null));
  }

  @Test
  public void resultIsFalseWhenCollectionIsEmpty() {
    ParsedExpression collection = new PrimitiveExpressionBuilder(FHIRDefinedType.STRING, STRING)
        .withDataset(StringPrimitiveRowFixture.createNullRowsDataset(spark))
        .build();
    ParsedExpression element = literalString("Samuel");

    // name.family.%op%('Samuel')
    ParsedExpression result = testOperator(collection, element);

    assertThat(result).selectResult().hasRows(RowFactory.create(ROW_ID_3, false),
        RowFactory.create(ROW_ID_5, false));
  }

  @Test
  public void returnsEmptyWhenElementIsEmpty() {
    ParsedExpression collection = new PrimitiveExpressionBuilder(FHIRDefinedType.STRING, STRING)
        .withDataset(StringPrimitiveRowFixture.createCompleteDataset(spark))
        .build();
    ParsedExpression element = new PrimitiveExpressionBuilder(FHIRDefinedType.STRING,
        STRING)
        .withDataset(StringPrimitiveRowFixture.createAllRowsNullDataset(spark))
        .build();
    element.setSingular(true);
    element.setFhirPath("name.family.first()");

    // name.family.%op%(name.family.first())
    ParsedExpression result = testOperator(collection, element);

    assertThat(result).selectResult().hasRows(RowFactory.create(ROW_ID_1, null),
        RowFactory.create(ROW_ID_2, null), RowFactory.create(ROW_ID_3, null),
        RowFactory.create(ROW_ID_4, null), RowFactory.create(ROW_ID_5, null));
  }

  @Test
  public void worksForUnversionedCodingLiterals() {
    ParsedExpression collection = new PrimitiveExpressionBuilder(FHIRDefinedType.CODING, CODING)
        .withDataset(CodingRowFixture.createCompleteDataset(spark))
        .build();
    ParsedExpression element = literalCoding(CodingRowFixture.SYSTEM_1, null,
        CodingRowFixture.CODE_1);

    // maritalStatus.coding.%op%(uri:SYSTEM_1|CODE_1))
    ParsedExpression result = testOperator(collection, element);

    assertThat(result).selectResult().hasRows(RowFactory.create(ROW_ID_1, true),
        RowFactory.create(ROW_ID_2, false), RowFactory.create(ROW_ID_3, true),
        RowFactory.create(ROW_ID_4, false));
  }

  @Test
  public void worksForVersionedCodingLiterals() {
    ParsedExpression collection = new PrimitiveExpressionBuilder(FHIRDefinedType.CODING, CODING)
        .withDataset(CodingRowFixture.createCompleteDataset(spark))
        .build();
    ParsedExpression element = literalCoding(CodingRowFixture.SYSTEM_2, CodingRowFixture.VERSION_2,
        CodingRowFixture.CODE_2);

    // maritalStatus.coding.%op%(uri:SYSTEM_2|version-2|CODE_2))
    ParsedExpression result = testOperator(collection, element);

    assertThat(result).selectResult().hasRows(RowFactory.create(ROW_ID_1, false),
        RowFactory.create(ROW_ID_2, true), RowFactory.create(ROW_ID_3, true),
        RowFactory.create(ROW_ID_4, false));
  }

  @Test
  public void throwExceptionWhenElementIsNotSingular() {
    ParsedExpression collection = new PrimitiveExpressionBuilder(FHIRDefinedType.STRING, STRING)
        .withDataset(StringPrimitiveRowFixture.createNullRowsDataset(spark))
        .build();
    ParsedExpression element = new PrimitiveExpressionBuilder(FHIRDefinedType.STRING, STRING)
        .withDataset(StringPrimitiveRowFixture.createNullRowsDataset(spark))
        .build();
    element.setFhirPath("name.given");

    // name.family.%op%(name.given)
    assertThatExceptionOfType(InvalidRequestException.class)
        .isThrownBy(() -> testOperator(collection, element)).withMessage(
        resolveOperator("Element operand to %op% operator is not singular: name.given"));
  }


  @Test
  public void throwExceptionWhenIncompatibleTypes() {
    ParsedExpression collection = new PrimitiveExpressionBuilder(FHIRDefinedType.STRING, STRING)
        .withDataset(StringPrimitiveRowFixture.createNullRowsDataset(spark))
        .build();
    ParsedExpression element = literalBoolean(true);
    element.setFhirPath("true");

    // name.family.%op%(true)
    String message = operator.equals("in") ? "true in " + collection.getFhirPath()
        : collection.getFhirPath() + " contains true";
    assertThatExceptionOfType(InvalidRequestException.class)
        .isThrownBy(() -> testOperator(collection, element))
        .withMessage("Operands are of incompatible types: " + message);
  }

}
