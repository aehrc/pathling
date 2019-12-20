/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.pathling.query.operators;

import static au.csiro.pathling.test.Assertions.assertThat;
import static au.csiro.pathling.test.StringPrimitiveRowFixture.*;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import au.csiro.pathling.query.parsing.ParsedExpression;
import au.csiro.pathling.test.FunctionTest;
import au.csiro.pathling.test.StringPrimitiveRowFixture;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import org.apache.spark.sql.RowFactory;
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
public class MembershipOperatorTest extends FunctionTest {

  @Parameters(name = "{index}:{0}()")
  public static Object[] data() {
    return new Object[]{"in", "contains"};
  }

  private final String operator;
  private final BinaryOperatorInput operatorInput = new BinaryOperatorInput();

  public MembershipOperatorTest(String operator) {
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
    ParsedExpression collection =
        createPrimitiveParsedExpression(StringPrimitiveRowFixture.createCompleteDataset(spark));
    ParsedExpression element = createLiteralExpression("Samuel");

    // "name.family.%op%('Samuel')
    ParsedExpression result = testOperator(collection, element);

    assertThat(result).selectResult().hasRows(RowFactory.create(STRING_ROW_ID_1, false),
        RowFactory.create(STRING_ROW_ID_2, true), RowFactory.create(STRING_ROW_ID_3, false),
        RowFactory.create(STRING_ROW_ID_4, false), RowFactory.create(STRING_ROW_ID_5, false));
  }

  @Test
  public void returnsCorrectResultWhenElementIsExpression() {
    ParsedExpression collection =
        createPrimitiveParsedExpression(StringPrimitiveRowFixture.createCompleteDataset(spark));
    ParsedExpression element =
        createPrimitiveParsedExpression(StringPrimitiveRowFixture
            .createDataset(spark, RowFactory.create(STRING_ROW_ID_1, "Eva"), STRING_2_SAMUEL,
                STRING_3_NULL, STRING_4_ADAM, STRING_5_NULL));
    element.setSingular(true);
    element.setFhirPath("name.family.first()");

    // name.family.%op%(name.family.first())
    ParsedExpression result = testOperator(collection, element);

    assertThat(result).selectResult().hasRows(RowFactory.create(STRING_ROW_ID_1, false),
        RowFactory.create(STRING_ROW_ID_2, true), RowFactory.create(STRING_ROW_ID_3, null),
        RowFactory.create(STRING_ROW_ID_4, true), RowFactory.create(STRING_ROW_ID_5, null));
  }

  @Test
  public void resultIsFalseWhenCollectionIsEmpty() {
    ParsedExpression collection =
        createPrimitiveParsedExpression(StringPrimitiveRowFixture.createNullRowsDataset(spark));
    ParsedExpression element = createLiteralExpression("Samuel");

    // name.family.%op%('Samuel')
    ParsedExpression result = testOperator(collection, element);

    assertThat(result).selectResult().hasRows(
        RowFactory.create(STRING_ROW_ID_3, false), RowFactory.create(STRING_ROW_ID_5, false));
  }

  @Test
  public void returnsEmptyWhenElementIsEmpty() {
    ParsedExpression collection =
        createPrimitiveParsedExpression(StringPrimitiveRowFixture.createCompleteDataset(spark));
    ParsedExpression element =
        createPrimitiveParsedExpression(StringPrimitiveRowFixture.createAllRowsNullDataset(spark));
    element.setSingular(true);
    element.setFhirPath("name.family.first()");

    // name.family.%op%(name.family.first())
    ParsedExpression result = testOperator(collection, element);

    assertThat(result).selectResult().hasRows(RowFactory.create(STRING_ROW_ID_1, null),
        RowFactory.create(STRING_ROW_ID_2, null), RowFactory.create(STRING_ROW_ID_3, null),
        RowFactory.create(STRING_ROW_ID_4, null), RowFactory.create(STRING_ROW_ID_5, null));
  }

  @Test
  public void throwExceptionWhenElementIsNotSingular() {
    ParsedExpression collection =
        createPrimitiveParsedExpression(StringPrimitiveRowFixture.createNullRowsDataset(spark));
    ParsedExpression element =
        createPrimitiveParsedExpression(StringPrimitiveRowFixture.createNullRowsDataset(spark));
    element.setFhirPath("name.given");

    // name.family.%op%(name.given)
    assertThatExceptionOfType(InvalidRequestException.class)
        .isThrownBy(() -> testOperator(collection, element)).withMessage(
        resolveOperator("Element operand to %op% operator is not singular: name.given"));
  }


  @Test
  public void throwExceptionWhenIncompatibleTypes() {
    ParsedExpression collection =
        createPrimitiveParsedExpression(StringPrimitiveRowFixture.createNullRowsDataset(spark));
    ParsedExpression element = createLiteralExpression(true);
    element.setFhirPath("true");

    // name.family.%op%(true)
    String message = operator.equals("in")
        ? "true in " + collection.getFhirPath()
        : collection.getFhirPath() + " contains true";
    assertThatExceptionOfType(InvalidRequestException.class)
        .isThrownBy(() -> testOperator(collection, element))
        .withMessage("Operands are of incompatible types: " + message);
  }

}
