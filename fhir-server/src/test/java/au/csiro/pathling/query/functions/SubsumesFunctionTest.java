/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.pathling.query.functions;

import static au.csiro.pathling.TestUtilities.codeableConceptStructType;
import static au.csiro.pathling.TestUtilities.codingStructType;
import static au.csiro.pathling.TestUtilities.rowFromCodeableConcept;
import static au.csiro.pathling.TestUtilities.rowFromCoding;
import static au.csiro.pathling.test.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.ConceptMap;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import au.csiro.pathling.encoding.Mapping;
import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.query.parsing.ParsedExpression;
import au.csiro.pathling.query.parsing.parser.ExpressionParserContext;
import au.csiro.pathling.test.ComplexExpressionBuilder;
import au.csiro.pathling.test.DatasetAssert;
import au.csiro.pathling.test.DatasetBuilder;
import au.csiro.pathling.test.ParsedExpressionAssert;
import au.csiro.pathling.test.PrimitiveExpressionBuilder;
import au.csiro.pathling.test.fixtures.ConceptMapFixtures;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;

/**
 * @author Piotr Szul
 */
@Category(au.csiro.pathling.UnitTest.class)
public class SubsumesFunctionTest {

  private TerminologyClient terminologyClient = mock(TerminologyClient.class);


  private static final String TEST_SYSTEM = "uuid:1";

  private static final Coding CODING_SMALL = new Coding(TEST_SYSTEM, "SMALL", null);
  private static final Coding CODING_MEDIUM = new Coding(TEST_SYSTEM, "MEDIUM", null);
  private static final Coding CODING_LARGE = new Coding(TEST_SYSTEM, "LARGE", null);
  private static final Coding CODING_OTHER1 = new Coding(TEST_SYSTEM, "OTHER1", null);
  private static final Coding CODING_OTHER2 = new Coding(TEST_SYSTEM, "OTHER2", null);
  private static final Coding CODING_OTHER3 = new Coding(TEST_SYSTEM, "OTHER3", null);
  private static final Coding CODING_OTHER4 = new Coding(TEST_SYSTEM, "OTHER4", null);
  private static final Coding CODING_OTHER5 = new Coding(TEST_SYSTEM, "OTHER5", null);

  private static final String RES_ID1 = "Encounter/xyz1";
  private static final String RES_ID2 = "Encounter/xyz2";
  private static final String RES_ID3 = "Encounter/xyz3";
  private static final String RES_ID4 = "Encounter/xyz4";
  private static final String RES_ID5 = "Encounter/xyz5";

  // coding_large -- subsumes --> coding_medium --> subsumes --> coding_small
  private static final ConceptMap MAP_LARGE_MEDIUM_SMALL = ConceptMapFixtures.createConceptMap(
      Mapping.of(CODING_LARGE, CODING_MEDIUM), Mapping.of(CODING_MEDIUM, CODING_SMALL));

  public static final List<String> ALL_RES_IDS =
      Arrays.asList(RES_ID1, RES_ID2, RES_ID3, RES_ID4, RES_ID5);

  private static Row codeableConceptRowFromCoding(Coding coding) {
    return codeableConceptRowFromCoding(coding, CODING_OTHER4);
  }

  private static Row codeableConceptRowFromCoding(Coding coding, Coding otherCoding) {
    return rowFromCodeableConcept(new CodeableConcept(coding).addCoding(otherCoding));
  }

  @Before
  public void setUp() {
    when(terminologyClient.closure(any(), any(), any())).thenReturn(MAP_LARGE_MEDIUM_SMALL);
  }

  private static ParsedExpression createCodingInput() {
    ParsedExpression inputExpression = new ComplexExpressionBuilder(FHIRDefinedType.CODING)
        .withColumn("789wxyz_id", DataTypes.StringType).withStructTypeColumns(codingStructType())
        .withRow(RES_ID1, rowFromCoding(CODING_SMALL))
        .withRow(RES_ID2, rowFromCoding(CODING_MEDIUM))
        .withRow(RES_ID3, rowFromCoding(CODING_LARGE))
        .withRow(RES_ID4, rowFromCoding(CODING_OTHER1))
        .withRow(RES_ID5, null /* NULL coding value */)
        .withRow(RES_ID1, rowFromCoding(CODING_OTHER2))
        .withRow(RES_ID2, rowFromCoding(CODING_OTHER2))
        .withRow(RES_ID3, rowFromCoding(CODING_OTHER2))
        .withRow(RES_ID4, rowFromCoding(CODING_OTHER2)).buildWithStructValue("789wxyz");
    inputExpression.setSingular(false);
    return inputExpression;
  }

  private static ParsedExpression createCodeableConceptInput() {
    ParsedExpression inputExpression = new ComplexExpressionBuilder(FHIRDefinedType.CODEABLECONCEPT)
        .withColumn("789wxyz_id", DataTypes.StringType)
        .withStructTypeColumns(codeableConceptStructType())
        .withRow(RES_ID1, codeableConceptRowFromCoding(CODING_SMALL))
        .withRow(RES_ID2, codeableConceptRowFromCoding(CODING_MEDIUM))
        .withRow(RES_ID3, codeableConceptRowFromCoding(CODING_LARGE))
        .withRow(RES_ID4, codeableConceptRowFromCoding(CODING_OTHER1))
        .withRow(RES_ID5, null /* NULL codeable cocept value */)
        .withRow(RES_ID1, codeableConceptRowFromCoding(CODING_OTHER2))
        .withRow(RES_ID2, codeableConceptRowFromCoding(CODING_OTHER2))
        .withRow(RES_ID3, codeableConceptRowFromCoding(CODING_OTHER2))
        .withRow(RES_ID4, codeableConceptRowFromCoding(CODING_OTHER2))
        .buildWithStructValue("789wxyz");
    inputExpression.setSingular(false);
    return inputExpression;
  }

  private static ParsedExpression createLiteralArg() {
    ParsedExpression argumentExpression = PrimitiveExpressionBuilder.literalCoding(CODING_MEDIUM);
    return argumentExpression;
  }

  private static ParsedExpression createCodingArg() {
    return ComplexExpressionBuilder.of(FHIRDefinedType.CODING)
        .withColumn("123wxyz_id", DataTypes.StringType).withStructTypeColumns(codingStructType())
        .withIdValueRows(ALL_RES_IDS, id -> rowFromCoding(CODING_MEDIUM))
        .withIdValueRows(ALL_RES_IDS, id -> rowFromCoding(CODING_OTHER3))
        .buildWithStructValue("123wxyz");
  }

  private static ParsedExpression createCodeableConceptArg() {
    return ComplexExpressionBuilder.of(FHIRDefinedType.CODEABLECONCEPT)
        .withColumn("123wxyz_id", DataTypes.StringType)
        .withStructTypeColumns(codeableConceptStructType())
        .withIdValueRows(ALL_RES_IDS, id -> codeableConceptRowFromCoding(CODING_MEDIUM))
        .withIdValueRows(ALL_RES_IDS,
            id -> codeableConceptRowFromCoding(CODING_OTHER3, CODING_OTHER5))
        .buildWithStructValue("123wxyz");
  }

  private static ParsedExpression createNullCodingArg() {
    return ComplexExpressionBuilder.of(FHIRDefinedType.CODING)
        .withColumn("123wxyz_id", DataTypes.StringType).withStructTypeColumns(codingStructType())
        .withIdValueRows(ALL_RES_IDS, id -> null).buildWithStructValue("123wxyz");
  }

  private static DatasetBuilder allFalse() {
    return new DatasetBuilder().withIdsAndValue(false, ALL_RES_IDS);
  }

  private static DatasetBuilder allTrue() {
    return new DatasetBuilder().withIdsAndValue(true, ALL_RES_IDS);
  }

  private static DatasetBuilder expectedSubsumes() {
    return allFalse().changeValues(true, Arrays.asList(RES_ID2, RES_ID3));
  }

  private static DatasetBuilder expectedSubsumedBy() {
    return allFalse().changeValues(true, Arrays.asList(RES_ID2, RES_ID1));
  }

  private ParsedExpressionAssert assertCallSuccess(Function function,
      ParsedExpression inputExpression, ParsedExpression argumentExpression) {
    // Prepare the inputs to the function.
    ExpressionParserContext parserContext = new ExpressionParserContext();
    parserContext.setTerminologyClient(terminologyClient);

    FunctionInput functionInput = new FunctionInput();
    // TODO: change to some random string - does not really matter here what it is
    String inputFhirPath = "subsumes(" + TEST_SYSTEM + "|" + "MEDIUM" + ")";
    functionInput.setExpression(inputFhirPath);
    functionInput.setContext(parserContext);
    functionInput.setInput(inputExpression);
    functionInput.getArguments().add(argumentExpression);

    ParsedExpression result = function.invoke(functionInput);
    return assertThat(result).isSelection().isOfBooleanType().isSingular()
        .isResultFor(functionInput);
  }

  private DatasetAssert assertSubsumesSuccess(ParsedExpression inputExpression,
      ParsedExpression argumentExpression) {
    return assertCallSuccess(new SubsumesFunction(), inputExpression, argumentExpression)
        .selectResult();
  }

  private DatasetAssert assertSubsumedBySuccess(ParsedExpression inputExpression,
      ParsedExpression argumentExpression) {
    return assertCallSuccess(new SubsumesFunction(true), inputExpression, argumentExpression)
        .selectResult();
  }

  //
  // Test subsumes on selected pairs of argument types
  // (Coding, CodingLiteral) && (CodeableConcept, Coding) && (Literal, CodeableConcept)
  //
  @Test
  public void testSubsumesCodingWithLiteralCorrectly() throws Exception {
    assertSubsumesSuccess(createCodingInput(), createLiteralArg()).hasRows(expectedSubsumes());
  }

  @Test
  public void testSubsumesCodeableConceptWithCodingCorrectly() throws Exception {
    assertSubsumesSuccess(createCodeableConceptInput(), createCodingArg())
        .hasRows(expectedSubsumes());
  }

  @Test
  public void testSubsumesLiteralWithCodeableConcepCorrectly() throws Exception {
    // call subsumes but expect subsumedBy result
    // because input is switched with argument
    assertSubsumesSuccess(createLiteralArg(), createCodeableConceptInput())
        .hasRows(expectedSubsumedBy());
  }
  //
  // Test subsumedBy on selected pairs of argument types
  // (Coding, CodeableConcept) && (CodeableConcept, Literal) && (Literal, Coding)
  //

  @Test
  public void testSubsumedByCodingWithCodeableConceptCorrectly() throws Exception {
    assertSubsumedBySuccess(createCodingInput(), createCodeableConceptArg())
        .hasRows(expectedSubsumedBy());
  }

  @Test
  public void testSubsumedByCodeableConceptWithLiteralCorrectly() throws Exception {
    assertSubsumedBySuccess(createCodeableConceptInput(), createLiteralArg())
        .hasRows(expectedSubsumedBy());
  }

  @Test
  public void testSubsumedByLiteralWithCodingtCorrectly() throws Exception {
    // call subsumedBy but expect subsumes result
    // because input is switched with argument
    assertSubsumedBySuccess(createLiteralArg(), createCodingInput()).hasRows(expectedSubsumes());
  }

  //
  // Test agains nulls
  //

  @Test
  public void testAllFalseWhenSubsumesNullCoding() throws Exception {
    // call subsumedBy but expect subsumes result
    // because input is switched with argument
    assertSubsumesSuccess(createCodingInput(), createNullCodingArg()).hasRows(allFalse());
    assertSubsumedBySuccess(createCodeableConceptInput(), createNullCodingArg())
        .hasRows(allFalse());
  }



  @Test
  public void testAllNonNullTrueWhenSubsumesItself() throws Exception {
    assertSubsumesSuccess(createCodingInput(), createCodeableConceptInput())
        .hasRows(allTrue().changeValue(RES_ID5, false));
    assertSubsumedBySuccess(createCodingInput(), createCodeableConceptInput())
        .hasRows(allTrue().changeValue(RES_ID5, false));
  }

  //
  // Test for various validation errors
  //

  @Test
  public void throwsErrorIfInputTypeIsUnsupprted() {
    ParsedExpression input = PrimitiveExpressionBuilder.literalString("stringLiteral");
    ParsedExpression argument =
        new ComplexExpressionBuilder(FHIRDefinedType.CODEABLECONCEPT).build();

    FunctionInput functionInput = new FunctionInput();
    functionInput.setInput(input);
    functionInput.getArguments().add(argument);
    functionInput.setExpression("'stringLiteral'.subsumes(Some.coding)");

    SubsumesFunction subsumesFunction = new SubsumesFunction(true);
    assertThatExceptionOfType(InvalidRequestException.class)
        .isThrownBy(() -> subsumesFunction.invoke(functionInput)).withMessage(
            "subsumedBy function accepts input of type Coding or CodeableConcept: 'stringLiteral'");
  }

  @Test
  public void throwsErrorIfArgumentTypeIsUnsupprted() {
    ParsedExpression input = new ComplexExpressionBuilder(FHIRDefinedType.CODEABLECONCEPT).build();
    ParsedExpression argument = PrimitiveExpressionBuilder.literalString("str");

    FunctionInput functionInput = new FunctionInput();
    functionInput.setInput(input);
    functionInput.getArguments().add(argument);
    functionInput.setExpression("subsumes('str')");

    SubsumesFunction subsumesFunction = new SubsumesFunction();
    assertThatExceptionOfType(InvalidRequestException.class)
        .isThrownBy(() -> subsumesFunction.invoke(functionInput))
        .withMessage("subsumes function accepts argument of type Coding or CodeableConcept: 'str'");
  }

  @Test
  public void throwsErrorIfBothArgumentsAreLiterals() {
    ParsedExpression input = PrimitiveExpressionBuilder.literalCoding(new Coding());
    ParsedExpression argument = PrimitiveExpressionBuilder.literalCoding(new Coding());

    FunctionInput functionInput = new FunctionInput();
    functionInput.setInput(input);
    functionInput.getArguments().add(argument);
    functionInput.setExpression("subsumedBy(uuid:1|a)");

    SubsumesFunction subsumesFunction = new SubsumesFunction(true);
    assertThatExceptionOfType(InvalidRequestException.class)
        .isThrownBy(() -> subsumesFunction.invoke(functionInput)).withMessage(
            "Input and argument cannot be both literals for subsumedBy function: subsumedBy(uuid:1|a)");
  }

  @Test
  public void throwsErrorIfMoreThanOneArgument() {
    ParsedExpression input = new ComplexExpressionBuilder(FHIRDefinedType.CODEABLECONCEPT).build();
    ParsedExpression argument1 = PrimitiveExpressionBuilder.literalCoding(new Coding()),
        argument2 = PrimitiveExpressionBuilder.literalCoding(new Coding());

    FunctionInput functionInput = new FunctionInput();
    functionInput.setInput(input);
    functionInput.getArguments().add(argument1);
    functionInput.getArguments().add(argument2);
    functionInput.setExpression("subsumes(uuid:1|a,uuid:1|b)");

    SubsumesFunction subsumesFunction = new SubsumesFunction();
    assertThatExceptionOfType(InvalidRequestException.class)
        .isThrownBy(() -> subsumesFunction.invoke(functionInput)).withMessage(
            "subsumes function accepts one argument of type Coding|CodeableConcept: subsumes(uuid:1|a,uuid:1|b)");
  }


}
