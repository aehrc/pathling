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
import au.csiro.pathling.test.DatasetBuilder;
import au.csiro.pathling.test.ExpressionBuilder;
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

  private static DatasetBuilder allFalse() {
    return new DatasetBuilder().withIdsAndValue(false, ALL_RES_IDS);
  }

  private static Row codeableConceptRowFromCoding(Coding coding) {
    return rowFromCodeableConcept(new CodeableConcept(coding).addCoding(CODING_OTHER3));
  }

  @Before
  public void setUp() {
    when(terminologyClient.closure(any(), any(), any())).thenReturn(MAP_LARGE_MEDIUM_SMALL);
  }

  private ParsedExpressionAssert assertCallSuccess(Function function,
      ParsedExpression inputExpression, ParsedExpression argumentExpression) {
    // Prepare the inputs to the function.
    ExpressionParserContext parserContext = new ExpressionParserContext();
    parserContext.setTerminologyClient(terminologyClient);
    // parserContext.setTerminologyClientFactory(terminologyClientFactory);

    FunctionInput functionInput = new FunctionInput();
    String inputFhirPath = "subsumes(" + TEST_SYSTEM + "|" + "MEDIUM" + ")";
    functionInput.setExpression(inputFhirPath);
    functionInput.setContext(parserContext);
    functionInput.setInput(inputExpression);
    functionInput.getArguments().add(argumentExpression);

    ParsedExpression result = function.invoke(functionInput);
    return assertThat(result).isSelection().isOfBooleanType().isSingular();
  }

  @Test
  public void testSubsumesCodingWithLiteralCorrectly() throws Exception {

    ParsedExpression inputExpression = new ComplexExpressionBuilder(FHIRDefinedType.CODING)
        .withColumn("789wxyz_id", DataTypes.StringType).withStructTypeColumns(codingStructType())
        .withRow(RES_ID1, rowFromCoding(CODING_SMALL))
        .withRow(RES_ID2, rowFromCoding(CODING_MEDIUM))
        .withRow(RES_ID3, rowFromCoding(CODING_LARGE))
        .withRow(RES_ID4, rowFromCoding(CODING_OTHER1)).withRow(RES_ID5, null)
        .withRow(RES_ID1, rowFromCoding(CODING_OTHER2))
        .withRow(RES_ID2, rowFromCoding(CODING_OTHER2))
        .withRow(RES_ID3, rowFromCoding(CODING_OTHER2))
        .withRow(RES_ID4, rowFromCoding(CODING_OTHER2)).buildWithStructValue("789wxyz");
    inputExpression.setSingular(false);

    ParsedExpression argumentExpression = PrimitiveExpressionBuilder.literalCoding(CODING_MEDIUM);

    assertCallSuccess(new SubsumesFunction(), inputExpression, argumentExpression).selectResult()
        .hasRows(allFalse().changeValues(true, Arrays.asList(RES_ID2, RES_ID3)));

    assertCallSuccess(new SubsumesFunction(true), inputExpression, argumentExpression)
        .selectResult().hasRows(allFalse().changeValues(true, Arrays.asList(RES_ID1, RES_ID2)));

  }

  @Test
  public void testSubsumesCodingWithCodingCorrectly() throws Exception {

    ParsedExpression inputExpression = new ComplexExpressionBuilder(FHIRDefinedType.CODING)
        .withColumn("789wxyz_id", DataTypes.StringType).withStructTypeColumns(codingStructType())
        .withRow(RES_ID1, rowFromCoding(CODING_SMALL))
        .withRow(RES_ID2, rowFromCoding(CODING_MEDIUM))
        .withRow(RES_ID3, rowFromCoding(CODING_LARGE))
        .withRow(RES_ID4, rowFromCoding(CODING_OTHER1)).withRow(RES_ID5, null)
        .withRow(RES_ID1, rowFromCoding(CODING_OTHER2))
        .withRow(RES_ID2, rowFromCoding(CODING_OTHER2))
        .withRow(RES_ID3, rowFromCoding(CODING_OTHER2))
        .withRow(RES_ID4, rowFromCoding(CODING_OTHER2)).buildWithStructValue("789wxyz");
    inputExpression.setSingular(false);


    final ExpressionBuilder argExpressionBuilder =
        new ComplexExpressionBuilder(FHIRDefinedType.CODING)
            .withColumn("123wxyz_id", DataTypes.StringType)
            .withStructTypeColumns(codingStructType());

    ALL_RES_IDS.forEach(id -> {
      argExpressionBuilder.withRow(id, rowFromCoding(CODING_MEDIUM)).withRow(id,
          rowFromCoding(CODING_OTHER3));
    });

    ParsedExpression argumentExpression = argExpressionBuilder.buildWithStructValue("123wxyz");

    assertCallSuccess(new SubsumesFunction(), inputExpression, argumentExpression).selectResult()
        .hasRows(allFalse().changeValues(true, Arrays.asList(RES_ID2, RES_ID3)));

    assertCallSuccess(new SubsumesFunction(true), inputExpression, argumentExpression)
        .selectResult().hasRows(allFalse().changeValues(true, Arrays.asList(RES_ID1, RES_ID2)));
  }

  @Test
  public void testSubsumesCodeableConceptWithLiteralCorrectly() throws Exception {
    ParsedExpression inputExpression = new ComplexExpressionBuilder(FHIRDefinedType.CODEABLECONCEPT)
        .withColumn("789wxyz_id", DataTypes.StringType)
        .withStructTypeColumns(codeableConceptStructType())
        .withRow(RES_ID1, codeableConceptRowFromCoding(CODING_SMALL))
        .withRow(RES_ID2, codeableConceptRowFromCoding(CODING_MEDIUM))
        .withRow(RES_ID3, codeableConceptRowFromCoding(CODING_LARGE))
        .withRow(RES_ID4, codeableConceptRowFromCoding(CODING_OTHER1)).withRow(RES_ID5, null)
        .withRow(RES_ID1, codeableConceptRowFromCoding(CODING_OTHER2))
        .withRow(RES_ID2, codeableConceptRowFromCoding(CODING_OTHER2))
        .withRow(RES_ID3, codeableConceptRowFromCoding(CODING_OTHER2))
        .withRow(RES_ID4, codeableConceptRowFromCoding(CODING_OTHER2))
        .buildWithStructValue("789wxyz");
    inputExpression.setSingular(false);

    ParsedExpression argumentExpression = PrimitiveExpressionBuilder.literalCoding(CODING_MEDIUM);

    assertCallSuccess(new SubsumesFunction(), inputExpression, argumentExpression).selectResult()
        .hasRows(allFalse().changeValues(true, Arrays.asList(RES_ID2, RES_ID3)));

    assertCallSuccess(new SubsumesFunction(true), inputExpression, argumentExpression)
        .selectResult().hasRows(allFalse().changeValues(true, Arrays.asList(RES_ID1, RES_ID2)));
  }

  // @Test
  // public void throwsErrorIfInputTypeIsUnsupported() {
  // ParsedExpression input = new PrimitiveExpressionBuilder(FHIRDefinedType.STRING,
  // FhirPathType.STRING)
  // .build();
  // input.setFhirPath("onsetString");
  // ParsedExpression argument = PrimitiveExpressionBuilder.literalString(MY_VALUE_SET_URL);
  //
  // FunctionInput memberOfInput = new FunctionInput();
  // memberOfInput.setInput(input);
  // memberOfInput.getArguments().add(argument);
  //
  // MemberOfFunction memberOfFunction = new MemberOfFunction();
  // assertThatExceptionOfType(InvalidRequestException.class)
  // .isThrownBy(() -> memberOfFunction.invoke(memberOfInput))
  // .withMessage("Input to memberOf function is of unsupported type: onsetString");
  // }
  //
  // @Test
  // public void throwsErrorIfArgumentIsNotString() {
  // ParsedExpression input = new ComplexExpressionBuilder(FHIRDefinedType.CODEABLECONCEPT)
  // .build();
  // ParsedExpression argument = PrimitiveExpressionBuilder.literalInteger(4);
  //
  // FunctionInput memberOfInput = new FunctionInput();
  // memberOfInput.setInput(input);
  // memberOfInput.getArguments().add(argument);
  // memberOfInput.setExpression("memberOf(4)");
  //
  // MemberOfFunction memberOfFunction = new MemberOfFunction();
  // assertThatExceptionOfType(InvalidRequestException.class)
  // .isThrownBy(() -> memberOfFunction.invoke(memberOfInput))
  // .withMessage("memberOf function accepts one argument of type String: memberOf(4)");
  // }

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
