/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function.memberof;

import static au.csiro.pathling.test.assertions.Assertions.assertThat;
import static au.csiro.pathling.test.builders.DatasetBuilder.makeEid;
import static au.csiro.pathling.test.helpers.SparkHelpers.codeableConceptStructType;
import static au.csiro.pathling.test.helpers.SparkHelpers.codingStructType;
import static au.csiro.pathling.test.helpers.SparkHelpers.rowFromCodeableConcept;
import static au.csiro.pathling.test.helpers.SparkHelpers.rowFromCoding;
import static au.csiro.pathling.test.helpers.TestHelpers.LOINC_URL;
import static au.csiro.pathling.test.helpers.TestHelpers.SNOMED_URL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.fhir.TerminologyClientFactory;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.element.BooleanPath;
import au.csiro.pathling.fhirpath.element.CodingPath;
import au.csiro.pathling.fhirpath.element.ElementDefinition;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.function.NamedFunctionInput;
import au.csiro.pathling.fhirpath.literal.IntegerLiteralPath;
import au.csiro.pathling.fhirpath.literal.StringLiteralPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.test.builders.DatasetBuilder;
import au.csiro.pathling.test.builders.ElementPathBuilder;
import au.csiro.pathling.test.builders.ParserContextBuilder;
import au.csiro.pathling.test.helpers.FhirHelpers;
import au.csiro.pathling.test.helpers.FhirHelpers.MemberOfTxAnswerer;
import ca.uhn.fhir.rest.param.UriParam;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.*;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

/**
 * @author John Grimes
 */
@Tag("UnitTest")
class MemberOfFunctionTest {

  private static final String MY_VALUE_SET_URL = "https://csiro.au/fhir/ValueSet/my-value-set";
  private static final String TERMINOLOGY_SERVICE_URL = "https://r4.ontoserver.csiro.au/fhir";

  @Test
  public void memberOfCoding() {
    final Coding coding1 = new Coding(MY_VALUE_SET_URL, "AMB", "ambulatory");
    final Coding coding2 = new Coding(MY_VALUE_SET_URL, "EMER", null);
    final Coding coding3 = new Coding(MY_VALUE_SET_URL, "IMP", "inpatient encounter");
    final Coding coding4 = new Coding(MY_VALUE_SET_URL, "IMP", null);
    final Coding coding5 = new Coding(MY_VALUE_SET_URL, "ACUTE", "inpatient acute");

    final Optional<ElementDefinition> optionalDefinition = FhirHelpers
        .getChildOfResource("Encounter", "class");
    assertTrue(optionalDefinition.isPresent());
    final ElementDefinition definition = optionalDefinition.get();

    final Dataset<Row> inputDataset = new DatasetBuilder()
        .withIdColumn()
        .withEidColumn()
        .withStructTypeColumns(codingStructType())
        .withRow("Encounter/1", makeEid(1), rowFromCoding(coding1))
        .withRow("Encounter/1", makeEid(0), rowFromCoding(coding5))
        .withRow("Encounter/2", makeEid(0), rowFromCoding(coding2))
        .withRow("Encounter/3", makeEid(0), rowFromCoding(coding3))
        .withRow("Encounter/4", makeEid(0), rowFromCoding(coding4))
        .withRow("Encounter/5", makeEid(0), rowFromCoding(coding5))
        .withRow("Encounter/6", null, null)
        .buildWithStructValue().coalesce(1);
    final CodingPath inputExpression = (CodingPath) new ElementPathBuilder()
        .dataset(inputDataset)
        .idAndEidAndValueColumns()
        .expression("Encounter.class")
        .singular(false)
        .definition(definition)
        .buildDefined();

    final StringLiteralPath argumentExpression = StringLiteralPath
        .fromString("'" + MY_VALUE_SET_URL + "'", inputExpression);

    // Create a mock terminology client.
    final TerminologyClient terminologyClient = mock(TerminologyClient.class,
        withSettings().serializable());
    final Answer<ValueSet> memberOfTxAnswerer = new MemberOfTxAnswerer(coding2, coding5);
    when(terminologyClient.getServerBase()).thenReturn(TERMINOLOGY_SERVICE_URL);
    when(terminologyClient.expand(any(ValueSet.class), any(IntegerType.class)))
        .thenAnswer(memberOfTxAnswerer);
    // setup all systems as known
    when(terminologyClient.searchCodeSystems(any(UriParam.class), any()))
        .thenReturn(Collections.singletonList(new CodeSystem()));
    // Create a mock TerminologyClientFactory, and make it return the mock terminology client.

    final TerminologyClientFactory terminologyClientFactory = mock(TerminologyClientFactory.class,
        withSettings().serializable());
    when(terminologyClientFactory.build(any())).thenReturn(terminologyClient);

    // Prepare the inputs to the function.
    final ParserContext parserContext = new ParserContextBuilder()
        .idColumn(inputExpression.getIdColumn())
        .terminologyClient(terminologyClient)
        .terminologyClientFactory(terminologyClientFactory)
        .build();

    final NamedFunctionInput memberOfInput = new NamedFunctionInput(parserContext, inputExpression,
        Collections.singletonList(argumentExpression));

    // Invoke the function.
    final FhirPath result = new MemberOfFunction().invoke(memberOfInput);

    // The outcome is somehow random with regard to the sequence passed to MemberOfMapperAnswerer.
    final Dataset<Row> expectedResult = new DatasetBuilder()
        .withIdColumn()
        .withEidColumn()
        .withColumn(DataTypes.BooleanType)
        .withRow("Encounter/1", makeEid(0), true)
        .withRow("Encounter/1", makeEid(1), false)
        .withRow("Encounter/2", makeEid(0), true)
        .withRow("Encounter/3", makeEid(0), false)
        .withRow("Encounter/4", makeEid(0), false)
        .withRow("Encounter/5", makeEid(0), true)
        .withRow("Encounter/6", null, null)
        .build();

    // Check the result.
    assertThat(result)
        .hasExpression("Encounter.class.memberOf('" + MY_VALUE_SET_URL + "')")
        .isElementPath(BooleanPath.class)
        .hasFhirType(FHIRDefinedType.BOOLEAN)
        .isNotSingular()
        .selectOrderedResultWithEid()
        .hasRows(expectedResult);
  }

  @Test
  public void memberOfCodeableConcept() {
    final Coding coding1 = new Coding(LOINC_URL, "10337-4",
        "Procollagen type I [Mass/volume] in Serum");
    final Coding coding2 = new Coding(LOINC_URL, "10428-1",
        "Varicella zoster virus immune globulin given [Volume]");
    final Coding coding3 = new Coding(LOINC_URL, "10555-1", null);
    final Coding coding4 = new Coding(LOINC_URL, "10665-8",
        "Fungus colony count [#/volume] in Unspecified specimen by Environmental culture");
    final Coding coding5 = new Coding(SNOMED_URL, "416399002",
        "Procollagen type I amino-terminal propeptide level");

    final CodeableConcept codeableConcept1 = new CodeableConcept(coding1);
    codeableConcept1.addCoding(coding5);
    final CodeableConcept codeableConcept2 = new CodeableConcept(coding2);
    final CodeableConcept codeableConcept3 = new CodeableConcept(coding3);
    final CodeableConcept codeableConcept4 = new CodeableConcept(coding3);
    final CodeableConcept codeableConcept5 = new CodeableConcept(coding4);
    final CodeableConcept codeableConcept6 = new CodeableConcept(coding1);

    final Optional<ElementDefinition> optionalDefinition = FhirHelpers
        .getChildOfResource("DiagnosticReport", "code");
    assertTrue(optionalDefinition.isPresent());
    final ElementDefinition definition = optionalDefinition.get();

    final Dataset<Row> inputDataset = new DatasetBuilder()
        .withIdColumn()
        .withStructTypeColumns(codeableConceptStructType())
        .withRow("DiagnosticReport/1", rowFromCodeableConcept(codeableConcept1))
        .withRow("DiagnosticReport/2", rowFromCodeableConcept(codeableConcept2))
        .withRow("DiagnosticReport/3", rowFromCodeableConcept(codeableConcept3))
        .withRow("DiagnosticReport/4", rowFromCodeableConcept(codeableConcept4))
        .withRow("DiagnosticReport/5", rowFromCodeableConcept(codeableConcept5))
        .withRow("DiagnosticReport/6", rowFromCodeableConcept(codeableConcept6))
        .withRow("DiagnosticReport/7", null)
        .buildWithStructValue()
        .coalesce(1);
    final ElementPath inputExpression = new ElementPathBuilder()
        .dataset(inputDataset)
        .idAndValueColumns()
        .expression("DiagnosticReport.code")
        .singular(true)
        .definition(definition)
        .buildDefined();

    final StringLiteralPath argumentExpression = StringLiteralPath
        .fromString("'" + MY_VALUE_SET_URL + "'", inputExpression);

    // Create a mock terminology client.
    final TerminologyClient terminologyClient = mock(TerminologyClient.class,
        withSettings().serializable());
    final Answer<ValueSet> memberOfTxAnswerer = new MemberOfTxAnswerer(codeableConcept1,
        codeableConcept3, codeableConcept4);
    when(terminologyClient.getServerBase()).thenReturn(TERMINOLOGY_SERVICE_URL);
    when(terminologyClient.expand(any(ValueSet.class), any(IntegerType.class)))
        .thenAnswer(memberOfTxAnswerer);
    // setup all systems as known
    when(terminologyClient.searchCodeSystems(any(UriParam.class), any()))
        .thenReturn(Collections.singletonList(new CodeSystem()));

    // Create a mock TerminologyClientFactory, and make it return the mock terminology client.
    final TerminologyClientFactory terminologyClientFactory = mock(TerminologyClientFactory.class,
        withSettings().serializable());
    when(terminologyClientFactory.build(any())).thenReturn(terminologyClient);

    // Prepare the inputs to the function.
    final ParserContext parserContext = new ParserContextBuilder()
        .terminologyClient(terminologyClient)
        .terminologyClientFactory(terminologyClientFactory)
        .build();

    final NamedFunctionInput memberOfInput = new NamedFunctionInput(parserContext, inputExpression,
        Collections.singletonList(argumentExpression));

    // Invoke the function.
    final FhirPath result = new MemberOfFunction().invoke(memberOfInput);

    // The outcome is somehow random with regard to
    // the sequence passed to MemberOfMapperAnswerer
    final Dataset<Row> expectedResult = new DatasetBuilder()
        .withIdColumn()
        .withColumn(DataTypes.BooleanType)
        .withRow("DiagnosticReport/1", true)
        .withRow("DiagnosticReport/2", false)
        .withRow("DiagnosticReport/3", true)
        .withRow("DiagnosticReport/4", true)
        .withRow("DiagnosticReport/5", false)
        .withRow("DiagnosticReport/6", true)
        .withRow("DiagnosticReport/7", null)
        .build();

    // Check the result.
    assertTrue(result instanceof BooleanPath);
    assertThat((BooleanPath) result)
        .hasExpression("DiagnosticReport.code.memberOf('" + MY_VALUE_SET_URL + "')")
        .isSingular()
        .hasFhirType(FHIRDefinedType.BOOLEAN)
        .isElementPath(BooleanPath.class)
        .selectOrderedResult()
        .hasRows(expectedResult);
  }

  @Test
  public void throwsErrorIfInputTypeIsUnsupported() {
    final FhirPath mockContext = new ElementPathBuilder().build();
    final ElementPath input = new ElementPathBuilder()
        .fhirType(FHIRDefinedType.STRING)
        .expression("name.given")
        .build();
    final FhirPath argument = StringLiteralPath.fromString(MY_VALUE_SET_URL, mockContext);

    final ParserContext parserContext = new ParserContextBuilder()
        .terminologyClient(mock(TerminologyClient.class))
        .terminologyClientFactory(mock(TerminologyClientFactory.class))
        .build();

    final NamedFunctionInput memberOfInput = new NamedFunctionInput(parserContext, input,
        Collections.singletonList(argument));

    final InvalidUserInputError error = assertThrows(InvalidUserInputError.class,
        () -> new MemberOfFunction().invoke(memberOfInput));
    assertEquals("Input to memberOf function is of unsupported type: name.given",
        error.getMessage());
  }

  @Test
  public void throwsErrorIfArgumentIsNotString() {
    final ElementPath input = new ElementPathBuilder()
        .fhirType(FHIRDefinedType.CODEABLECONCEPT)
        .build();
    final IntegerLiteralPath argument = IntegerLiteralPath.fromString("4", input);

    final ParserContext context = new ParserContextBuilder()
        .terminologyClient(mock(TerminologyClient.class))
        .terminologyClientFactory(mock(TerminologyClientFactory.class))
        .build();

    final NamedFunctionInput memberOfInput = new NamedFunctionInput(context, input,
        Collections.singletonList(argument));

    final InvalidUserInputError error = assertThrows(InvalidUserInputError.class,
        () -> new MemberOfFunction().invoke(memberOfInput));
    assertEquals("memberOf function accepts one argument of type String literal",
        error.getMessage());
  }

  @Test
  public void throwsErrorIfMoreThanOneArgument() {
    final ElementPath input = new ElementPathBuilder()
        .fhirType(FHIRDefinedType.CODEABLECONCEPT)
        .build();
    final StringLiteralPath argument1 = StringLiteralPath.fromString("'foo'", input),
        argument2 = StringLiteralPath.fromString("'bar'", input);

    final ParserContext context = new ParserContextBuilder()
        .terminologyClient(mock(TerminologyClient.class))
        .terminologyClientFactory(mock(TerminologyClientFactory.class))
        .build();

    final NamedFunctionInput memberOfInput = new NamedFunctionInput(context, input,
        Arrays.asList(argument1, argument2));

    final InvalidUserInputError error = assertThrows(InvalidUserInputError.class,
        () -> new MemberOfFunction().invoke(memberOfInput));
    assertEquals("memberOf function accepts one argument of type String",
        error.getMessage());
  }

  @Test
  public void throwsErrorIfTerminologyServiceNotConfigured() {
    final ElementPath input = new ElementPathBuilder()
        .fhirType(FHIRDefinedType.CODEABLECONCEPT)
        .build();
    final FhirPath argument = StringLiteralPath.fromString("some string", input);

    final ParserContext context = new ParserContextBuilder()
        .build();

    final NamedFunctionInput memberOfInput = new NamedFunctionInput(context, input,
        Collections.singletonList(argument));

    final InvalidUserInputError error = assertThrows(InvalidUserInputError.class,
        () -> new MemberOfFunction().invoke(memberOfInput));
    assertEquals(
        "Attempt to call terminology function memberOf when terminology service has not been configured",
        error.getMessage());
  }

}