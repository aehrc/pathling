/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.operator;

import static au.csiro.pathling.test.assertions.Assertions.assertThat;
import static au.csiro.pathling.test.helpers.SparkHelpers.getSparkSession;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.element.BooleanPath;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.literal.BooleanLiteralPath;
import au.csiro.pathling.fhirpath.literal.CodingLiteralPath;
import au.csiro.pathling.fhirpath.literal.StringLiteralPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.test.builders.ElementPathBuilder;
import au.csiro.pathling.test.builders.ParserContextBuilder;
import au.csiro.pathling.test.fixtures.CodingRowFixture;
import au.csiro.pathling.test.fixtures.StringPrimitiveRowFixture;
import java.util.stream.Stream;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * @author Piotr Szul
 */
@Tag("UnitTest")
@Disabled
public class MembershipOperatorTest {

  private final SparkSession spark;
  private ParserContext parserContext;

  public MembershipOperatorTest() {
    spark = getSparkSession();
  }

  @BeforeEach
  void setUp() {
    parserContext = new ParserContextBuilder().build();
  }

  public static Stream<String> parameters() {
    return Stream.of("in", "contains");
  }

  private FhirPath testOperator(final String operator, final FhirPath collection,
      final FhirPath element) {
    final OperatorInput operatorInput;
    if ("in".equals(operator)) {
      operatorInput = new OperatorInput(parserContext, element, collection);
    } else if ("contains".equals(operator)) {
      operatorInput = new OperatorInput(parserContext, collection, element);
    } else {
      throw new IllegalArgumentException("Membership operator '" + operator + "' cannot be tested");
    }

    final FhirPath result = Operator.getInstance(operator).invoke(operatorInput);
    assertThat(result)
        .isElementPath(BooleanPath.class)
        .isSingular();
    return result;
  }

  @ParameterizedTest
  @MethodSource("parameters")
  public void returnsCorrectResultWhenElementIsLiteral(final String operator) {
    final ElementPath collection = new ElementPathBuilder()
        .fhirType(FHIRDefinedType.STRING)
        .dataset(StringPrimitiveRowFixture.createCompleteDataset(spark))
        .idAndValueColumns()
        .build();
    final StringLiteralPath element = StringLiteralPath.fromString("'Samuel'", collection);
    parserContext = new ParserContextBuilder()
        .inputContext(collection)
        .build();

    final FhirPath result = testOperator(operator, collection, element);
    assertThat(result)
        .selectResult()
        .hasRows(
            RowFactory.create(StringPrimitiveRowFixture.ROW_ID_1, false),
            RowFactory.create(StringPrimitiveRowFixture.ROW_ID_2, true),
            RowFactory.create(StringPrimitiveRowFixture.ROW_ID_3, false),
            RowFactory.create(StringPrimitiveRowFixture.ROW_ID_4, false),
            RowFactory.create(StringPrimitiveRowFixture.ROW_ID_5, false));
  }

  @ParameterizedTest
  @MethodSource("parameters")
  public void returnsCorrectResultWhenElementIsExpression(final String operator) {
    final ElementPath collection = new ElementPathBuilder()
        .fhirType(FHIRDefinedType.STRING)
        .dataset(StringPrimitiveRowFixture.createCompleteDataset(spark))
        .idAndValueColumns()
        .build();
    final ElementPath element = new ElementPathBuilder()
        .fhirType(FHIRDefinedType.STRING)
        .dataset(StringPrimitiveRowFixture.createDataset(spark,
            RowFactory.create(StringPrimitiveRowFixture.ROW_ID_1, "Eva"),
            StringPrimitiveRowFixture.STRING_2_SAMUEL,
            StringPrimitiveRowFixture.STRING_3_NULL,
            StringPrimitiveRowFixture.STRING_4_ADAM,
            StringPrimitiveRowFixture.STRING_5_NULL))
        .idAndValueColumns()
        .singular(true)
        .expression("name.family.first()")
        .build();
    parserContext = new ParserContextBuilder()
        .inputContext(collection)
        .build();

    final FhirPath result = testOperator(operator, collection, element);
    assertThat(result)
        .selectResult()
        .hasRows(
            RowFactory.create(StringPrimitiveRowFixture.ROW_ID_1, false),
            RowFactory.create(StringPrimitiveRowFixture.ROW_ID_2, true),
            RowFactory.create(StringPrimitiveRowFixture.ROW_ID_3, null),
            RowFactory.create(StringPrimitiveRowFixture.ROW_ID_4, true),
            RowFactory.create(StringPrimitiveRowFixture.ROW_ID_5, null));
  }

  @ParameterizedTest
  @MethodSource("parameters")
  public void resultIsFalseWhenCollectionIsEmpty(final String operator) {
    final ElementPath collection = new ElementPathBuilder()
        .fhirType(FHIRDefinedType.STRING)
        .dataset(StringPrimitiveRowFixture.createNullRowsDataset(spark))
        .idAndValueColumns()
        .build();
    final StringLiteralPath element = StringLiteralPath.fromString("'Samuel'", collection);
    parserContext = new ParserContextBuilder()
        .inputContext(collection)
        .build();

    final FhirPath result = testOperator(operator, collection, element);
    assertThat(result)
        .selectResult()
        .hasRows(
            RowFactory.create(StringPrimitiveRowFixture.ROW_ID_3, false),
            RowFactory.create(StringPrimitiveRowFixture.ROW_ID_5, false));
  }

  @ParameterizedTest
  @MethodSource("parameters")
  public void returnsEmptyWhenElementIsEmpty(final String operator) {
    final ElementPath collection = new ElementPathBuilder()
        .fhirType(FHIRDefinedType.STRING)
        .dataset(StringPrimitiveRowFixture.createCompleteDataset(spark))
        .idAndValueColumns()
        .build();
    final ElementPath element = new ElementPathBuilder()
        .fhirType(FHIRDefinedType.STRING)
        .dataset(StringPrimitiveRowFixture.createAllRowsNullDataset(spark))
        .idAndValueColumns()
        .singular(true)
        .expression("name.family.first()")
        .build();
    parserContext = new ParserContextBuilder()
        .inputContext(collection)
        .build();

    final FhirPath result = testOperator(operator, collection, element);
    assertThat(result)
        .selectResult()
        .hasRows(
            RowFactory.create(StringPrimitiveRowFixture.ROW_ID_1, null),
            RowFactory.create(StringPrimitiveRowFixture.ROW_ID_2, null),
            RowFactory.create(StringPrimitiveRowFixture.ROW_ID_3, null),
            RowFactory.create(StringPrimitiveRowFixture.ROW_ID_4, null),
            RowFactory.create(StringPrimitiveRowFixture.ROW_ID_5, null));
  }

  @ParameterizedTest
  @MethodSource("parameters")
  public void worksForUnversionedCodingLiterals(final String operator) {
    final ElementPath collection = new ElementPathBuilder()
        .fhirType(FHIRDefinedType.CODING)
        .dataset(CodingRowFixture.createCompleteDataset(spark))
        .idAndValueColumns()
        .expression("code")
        .build();
    final CodingLiteralPath element = CodingLiteralPath
        .fromString(CodingRowFixture.SYSTEM_1 + "|" + CodingRowFixture.CODE_1, collection);
    parserContext = new ParserContextBuilder()
        .inputContext(collection)
        .build();

    final FhirPath result = testOperator(operator, collection, element);
    assertThat(result)
        .selectResult()
        .hasRows(
            RowFactory.create(StringPrimitiveRowFixture.ROW_ID_1, true),
            RowFactory.create(StringPrimitiveRowFixture.ROW_ID_2, false),
            RowFactory.create(StringPrimitiveRowFixture.ROW_ID_3, true),
            RowFactory.create(StringPrimitiveRowFixture.ROW_ID_4, false));
  }

  @ParameterizedTest
  @MethodSource("parameters")
  public void worksForVersionedCodingLiterals(final String operator) {
    final ElementPath collection = new ElementPathBuilder()
        .fhirType(FHIRDefinedType.CODING)
        .dataset(CodingRowFixture.createCompleteDataset(spark))
        .idAndValueColumns()
        .build();
    final CodingLiteralPath element = CodingLiteralPath
        .fromString(CodingRowFixture.SYSTEM_2 + "|" + CodingRowFixture.VERSION_2 + "|"
            + CodingRowFixture.CODE_2, collection);
    parserContext = new ParserContextBuilder()
        .inputContext(collection)
        .build();

    final FhirPath result = testOperator(operator, collection, element);
    assertThat(result)
        .selectResult()
        .hasRows(
            RowFactory.create(StringPrimitiveRowFixture.ROW_ID_1, false),
            RowFactory.create(StringPrimitiveRowFixture.ROW_ID_2, true),
            RowFactory.create(StringPrimitiveRowFixture.ROW_ID_3, true),
            RowFactory.create(StringPrimitiveRowFixture.ROW_ID_4, false));
  }

  @ParameterizedTest
  @MethodSource("parameters")
  public void throwExceptionWhenElementIsNotSingular(final String operator) {
    final ElementPath collection = new ElementPathBuilder()
        .singular(false)
        .build();
    final ElementPath element = new ElementPathBuilder()
        .singular(false)
        .expression("name.given")
        .build();

    final InvalidUserInputError error = assertThrows(
        InvalidUserInputError.class,
        () -> testOperator(operator, collection, element));
    assertEquals("Element operand used with " + operator + " operator is not singular: name.given",
        error.getMessage());
  }


  @ParameterizedTest
  @MethodSource("parameters")
  public void throwExceptionWhenIncompatibleTypes(final String operator) {
    final ElementPath collection = new ElementPathBuilder()
        .fhirType(FHIRDefinedType.STRING)
        .expression("foo")
        .build();
    final BooleanLiteralPath element = BooleanLiteralPath.fromString("true", collection);

    final InvalidUserInputError error = assertThrows(
        InvalidUserInputError.class,
        () -> testOperator(operator, collection, element));
    assertEquals(
        "Left operand to " + operator + " operator is not comparable to right operand: "
            + (operator.equals("in")
               ? "true in foo"
               : "foo contains true"),
        error.getMessage());
  }

}
