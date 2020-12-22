/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.parser;

import static au.csiro.pathling.test.assertions.Assertions.assertThat;
import static au.csiro.pathling.test.fixtures.PatientListBuilder.*;
import static au.csiro.pathling.test.helpers.SparkHelpers.getSparkSession;
import static au.csiro.pathling.test.helpers.TestHelpers.getResourceAsStream;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.fhir.TerminologyClientFactory;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.element.BooleanPath;
import au.csiro.pathling.fhirpath.element.IntegerPath;
import au.csiro.pathling.fhirpath.literal.CodingLiteralPath;
import au.csiro.pathling.fhirpath.literal.DateLiteralPath;
import au.csiro.pathling.fhirpath.literal.DateTimeLiteralPath;
import au.csiro.pathling.fhirpath.literal.TimeLiteralPath;
import au.csiro.pathling.io.ResourceReader;
import au.csiro.pathling.test.TimingExtension;
import au.csiro.pathling.test.assertions.FhirPathAssertion;
import au.csiro.pathling.test.builders.DatasetBuilder;
import au.csiro.pathling.test.builders.ParserContextBuilder;
import au.csiro.pathling.test.fixtures.ConceptMapFixtures;
import au.csiro.pathling.test.helpers.FhirHelpers;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.param.UriParam;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Date;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.*;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;

/**
 * @author Piotr Szul
 */
@Tag("UnitTest")
@ExtendWith(TimingExtension.class)
public class ParserTest {

  private SparkSession spark;
  private ResourceReader mockReader;
  private Parser parser;
  private TerminologyClient terminologyClient;

  @BeforeEach
  public void setUp() throws IOException {
    spark = getSparkSession();

    terminologyClient = mock(TerminologyClient.class, Mockito.withSettings().serializable());
    final TerminologyClientFactory terminologyClientFactory =
        mock(TerminologyClientFactory.class, Mockito.withSettings().serializable());
    when(terminologyClientFactory.build(any())).thenReturn(terminologyClient);

    mockReader = mock(ResourceReader.class);
    mockResourceReader(ResourceType.PATIENT, ResourceType.CONDITION, ResourceType.ENCOUNTER,
        ResourceType.PROCEDURE, ResourceType.MEDICATIONREQUEST, ResourceType.OBSERVATION,
        ResourceType.DIAGNOSTICREPORT);
    final FhirContext fhirContext = FhirHelpers.getFhirContext();

    final ResourcePath subjectResource = ResourcePath
        .build(fhirContext, mockReader, ResourceType.PATIENT, "%resource", true);

    final ParserContext parserContext = new ParserContextBuilder()
        .fhirContext(fhirContext)
        .terminologyClientFactory(terminologyClientFactory)
        .terminologyClient(terminologyClient)
        .sparkSession(spark)
        .resourceReader(mockReader)
        .inputContext(subjectResource)
        .build();
    parser = new Parser(parserContext);
  }


  private void mockResourceReader(final ResourceType... resourceTypes)
      throws MalformedURLException {
    for (final ResourceType resourceType : resourceTypes) {
      final File parquetFile =
          new File("src/test/resources/test-data/parquet/" + resourceType.toCode() + ".parquet");
      final URL parquetUrl = parquetFile.getAbsoluteFile().toURI().toURL();
      assertNotNull(parquetUrl);
      final Dataset<Row> dataset = spark.read().parquet(parquetUrl.toString());
      when(mockReader.read(resourceType)).thenReturn(dataset);
      when(mockReader.getAvailableResourceTypes())
          .thenReturn(new HashSet<>(Arrays.asList(resourceTypes)));
    }
  }

  private FhirPathAssertion assertThatResultOf(final String expression) {
    return assertThat(parser.parse(expression));
  }

  @Test
  public void testContainsOperator() {
    assertThatResultOf("name.family contains 'Wuckert783'")
        .isElementPath(BooleanPath.class)
        .selectOrderedResult()
        .hasRows(allPatientsWithValue(false).changeValue(PATIENT_ID_9360820c, true));

    assertThatResultOf("name.suffix contains 'MD'")
        .isElementPath(BooleanPath.class)
        .selectOrderedResult()
        .hasRows(allPatientsWithValue(false).changeValue(PATIENT_ID_8ee183e2, true));
  }

  @Test
  public void testInOperator() {
    assertThatResultOf("'Wuckert783' in name.family")
        .isElementPath(BooleanPath.class)
        .selectOrderedResult()
        .hasRows(allPatientsWithValue(false).changeValue(PATIENT_ID_9360820c, true));

    assertThatResultOf("'MD' in name.suffix")
        .isElementPath(BooleanPath.class)
        .selectOrderedResult()
        .hasRows(allPatientsWithValue(false).changeValue(PATIENT_ID_8ee183e2, true));
  }

  @Test
  public void testCodingOperations() {
    // test unversioned
    assertThatResultOf(
        "maritalStatus.coding contains http://terminology.hl7.org/CodeSystem/v3-MaritalStatus|S")
        .isElementPath(BooleanPath.class)
        .selectOrderedResult()
        .hasRows(allPatientsWithValue(true)
            .changeValue(PATIENT_ID_8ee183e2, false)
            .changeValue(PATIENT_ID_9360820c, false)
            .changeValue(PATIENT_ID_beff242e, false));

    // test versioned
    assertThatResultOf(
        "http://terminology.hl7.org/CodeSystem/v2-0203|v2.0.3|PPN in identifier.type.coding")
        .isElementPath(BooleanPath.class)
        .selectOrderedResult()
        .hasRows(allPatientsWithValue(true).changeValue(PATIENT_ID_bbd33563, false));
  }

  @Test
  public void testDateTimeLiterals() {
    // Full DateTime.
    assertThatResultOf("@2015-02-04T14:34:28Z")
        .isLiteralPath(DateTimeLiteralPath.class)
        .hasExpression("@2015-02-04T14:34:28Z")
        .hasJavaValue(new Date(1423060468000L));

    // Date with no time component.
    assertThatResultOf("@2015-02-04")
        .isLiteralPath(DateLiteralPath.class)
        .hasExpression("@2015-02-04")
        .hasJavaValue(new Date(1423008000000L));
  }

  @Test
  public void testTimeLiterals() {
    // Full Time.
    assertThatResultOf("@T14:34:28")
        .isLiteralPath(TimeLiteralPath.class)
        .hasExpression("@T14:34:28")
        .hasJavaValue("14:34:28");

    // Hour only.
    assertThatResultOf("@T14")
        .isLiteralPath(TimeLiteralPath.class)
        .hasExpression("@T14")
        .hasJavaValue("14");
  }

  @Test
  public void testCodingLiterals() {
    // Coding literal form [system]|[code]
    final Coding expectedCoding =
        new Coding("http://terminology.hl7.org/CodeSystem/v3-MaritalStatus", "S", null);
    assertThatResultOf("http://terminology.hl7.org/CodeSystem/v3-MaritalStatus|S")
        .isLiteralPath(CodingLiteralPath.class)
        .hasExpression("http://terminology.hl7.org/CodeSystem/v3-MaritalStatus|S")
        .hasCodingValue(expectedCoding);

    // Coding literal form [system]|[version]|[code]
    final Coding expectedCodingWithVersion =
        new Coding("http://terminology.hl7.org/CodeSystem/v3-MaritalStatus", "S", null);
    expectedCodingWithVersion.setVersion("v1");
    assertThatResultOf("http://terminology.hl7.org/CodeSystem/v3-MaritalStatus|v1|S")
        .isLiteralPath(CodingLiteralPath.class)
        .hasExpression("http://terminology.hl7.org/CodeSystem/v3-MaritalStatus|v1|S")
        .hasCodingValue(expectedCodingWithVersion);
  }

  @Test
  public void testCountWithReverseResolve() {
    assertThatResultOf("reverseResolve(Condition.subject).code.coding.count()")
        .isElementPath(IntegerPath.class)
        .isSingular()
        .selectOrderedResult()
        .hasRows(allPatientsWithValue(8L).changeValue(PATIENT_ID_121503c8, 10L)
            .changeValue(PATIENT_ID_2b36c1e2, 3L).changeValue(PATIENT_ID_7001ad9c, 5L)
            .changeValue(PATIENT_ID_9360820c, 16L).changeValue(PATIENT_ID_beff242e, 3L)
            .changeValue(PATIENT_ID_bbd33563, 10L));
  }

  @Test
  public void testCount() {
    final DatasetBuilder expectedCountResult =
        allPatientsWithValue(1L).changeValue(PATIENT_ID_9360820c, 2L);
    assertThatResultOf("name.count()")
        .selectOrderedResult()
        .hasRows(expectedCountResult);

    assertThatResultOf("name.family.count()")
        .selectOrderedResult()
        .hasRows(expectedCountResult);

    assertThatResultOf("name.given.count()")
        .selectOrderedResult()
        .hasRows(expectedCountResult);

    assertThatResultOf("name.prefix.count()")
        .selectOrderedResult()
        .hasRows(expectedCountResult.changeValue(PATIENT_ID_bbd33563, 0L));
  }

  @Test
  public void testSubsumesAndSubsumedBy() {
    // Setup mock terminology client
    when(terminologyClient.closure(any(), any())).thenReturn(ConceptMapFixtures.CM_EMPTY);

    // Viral sinusitis (disorder) = http://snomed.info/sct|444814009 not in (PATIENT_ID_2b36c1e2,
    // PATIENT_ID_bbd33563, PATIENT_ID_7001ad9c)
    // Chronic sinusitis (disorder) = http://snomed.info/sct|40055000 in (PATIENT_ID_7001ad9c)

    // With empty concept map subsume should work as member of
    assertThatResultOf(
        "reverseResolve(Condition.subject).code.subsumes(http://snomed.info/sct|40055000)")
        .isElementPath(BooleanPath.class)
        .selectOrderedResult()
        .hasRows("responses/ParserTest/testSubsumesAndSubsumedBy-subsumes-empty.csv");

    assertThatResultOf(
        "reverseResolve(Condition.subject).code.subsumedBy(http://snomed.info/sct|40055000)")
        .isElementPath(BooleanPath.class)
        .selectOrderedResult()
        .hasRows("responses/ParserTest/testSubsumesAndSubsumedBy-subsumedBy-empty.csv");

    // on the same collection should return all True (even though one is CodeableConcept)
    assertThatResultOf(
        "reverseResolve(Condition.subject).code.coding.subsumes(%resource.reverseResolve(Condition.subject).code)")
        .selectOrderedResult()
        .hasRows("responses/ParserTest/testSubsumesAndSubsumedBy-subsumes-self.csv");

    // http://snomed.info/sct|444814009 -- subsumes --> http://snomed.info/sct|40055000
    when(terminologyClient.closure(any(), any()))
        .thenReturn(ConceptMapFixtures.CM_SNOMED_444814009_SUBSUMES_40055000_VERSIONED);
    assertThatResultOf(
        "reverseResolve(Condition.subject).code.subsumes(http://snomed.info/sct|40055000)")
        .selectOrderedResult()
        .hasRows("responses/ParserTest/testSubsumesAndSubsumedBy-subsumes.csv");

    assertThatResultOf("reverseResolve(Condition.subject).code.subsumedBy"
        + "(http://snomed.info/sct|http://snomed.info/sct/32506021000036107/version/20200229|40055000)")
        .selectOrderedResult()
        .hasRows("responses/ParserTest/testSubsumesAndSubsumedBy-subsumedBy.csv");
  }

  @Test
  public void testWhereWithAggregateFunction() {
    assertThatResultOf("where($this.name.given.first() = 'Karina848').gender")
        .selectOrderedResult()
        .hasRows(allPatientsWithValue(DataTypes.StringType, null)
            .changeValue(PATIENT_ID_9360820c, "female"));
  }

  /**
   * This tests that the value from the `$this` context gets preserved successfully, when used in
   * the "element" operand to the membership operator.
   */
  @Test
  public void testWhereWithContainsOperator() {
    assertThatResultOf("where($this.name.given contains 'Karina848').gender")
        .selectOrderedResult()
        .hasRows(allPatientsWithValue((String) null).changeValue(PATIENT_ID_9360820c, "female"));
  }

  @Test
  public void testWhereWithSubsumes() {
    when(terminologyClient.closure(any(), any()))
        .thenReturn(ConceptMapFixtures.CM_SNOMED_444814009_SUBSUMES_40055000_VERSIONED);

    assertThatResultOf(
        "where($this.reverseResolve(Condition.subject).code"
            + ".subsumedBy(http://snomed.info/sct|40055000) contains true).gender")
        .selectOrderedResult()
        .hasRows(allPatientsWithValue((String) null)
            .changeValue(PATIENT_ID_7001ad9c, "female"));
  }

  @Test
  public void testWhereWithMemberOf() {
    final Bundle mockSearch = (Bundle) FhirHelpers.getJsonParser().parseResource(
        getResourceAsStream("txResponses/AggregateQueryTest/queryWithMemberOf.Bundle.json"));
    final List<CodeSystem> codeSystems = mockSearch.getEntry().stream()
        .map(entry -> (CodeSystem) entry.getResource())
        .collect(Collectors.toList());
    final ValueSet mockExpansion = (ValueSet) FhirHelpers.getJsonParser().parseResource(
        getResourceAsStream("txResponses/AggregateQueryTest/queryWithMemberOf.ValueSet.json"));

    //noinspection unchecked
    when(terminologyClient.searchCodeSystems(any(UriParam.class), any(Set.class)))
        .thenReturn(codeSystems);
    when(terminologyClient.expand(any(ValueSet.class), any(IntegerType.class)))
        .thenReturn(mockExpansion);

    assertThatResultOf(
        "reverseResolve(Condition.subject).where("
            + "$this.code.memberOf('http://snomed.info/sct?fhir_vs=refset/32570521000036109'))"
            + ".recordedDate")
        .selectOrderedResult()
        .hasRows("responses/ParserTest/testWhereWithMemberOf.csv");
  }

  /**
   * This tests that the value from the `$this` context gets preserved successfully, when used in
   * the "collection" operand to the membership operator. It also tests that aggregation can be
   * applied successfully following a nested where invocation.
   */
  @Test
  public void testAggregationFollowingNestedWhere() {
    assertThatResultOf(
        "where(name.where(use = 'official').first().given.first() in "
            + "name.where(use = 'maiden').first().given).gender")
        .selectOrderedResult()
        .hasRows("responses/ParserTest/testAggregationFollowingNestedWhere.csv");
  }

  @Test
  public void testNestedWhereWithAggregationOnElement() {
    assertThatResultOf(
        "name.where('Karina848' in where(use contains 'maiden').given).family")
        .selectOrderedResult()
        .hasRows("responses/ParserTest/testNestedWhereWithAggregationOnElement.csv");
  }

  @Test
  public void testBooleanOperatorWithTwoLiterals() {
    assertThatResultOf("true and false")
        .selectOrderedResult();
  }

  @Test
  public void parserErrorThrows() {
    final InvalidUserInputError error = assertThrows(InvalidUserInputError.class,
        () -> parser.parse(
            "(reasonCode.coding.display contains 'Viral pneumonia') and (class.code = 'AMB'"));
    assertEquals("Error parsing FHIRPath expression: missing ')' at '<EOF>'", error.getMessage());
  }
}
