/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.aggregate;

import static au.csiro.pathling.test.assertions.Assertions.assertJson;
import static au.csiro.pathling.test.helpers.FhirHelpers.getJsonParser;
import static au.csiro.pathling.test.helpers.TestHelpers.getResourceAsStream;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.fhir.TerminologyClientFactory;
import au.csiro.pathling.io.ResourceReader;
import au.csiro.pathling.test.helpers.FhirHelpers;
import au.csiro.pathling.test.helpers.TestHelpers;
import javax.annotation.Nonnull;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.Parameters;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

/**
 * @author John Grimes
 */
@SpringBootTest
@Tag("IntegrationTest")
@Execution(ExecutionMode.SAME_THREAD)
class AggregateExecutorTest extends QueryExecutorTest {

  @Autowired
  private AggregateExecutor executor;

  @Autowired
  private SparkSession spark;

  @MockBean
  private ResourceReader resourceReader;

  @MockBean
  private TerminologyClient terminologyClient;

  @MockBean
  private TerminologyClientFactory terminologyClientFactory;

  @Test
  void simpleQuery() {
    final ResourceType subjectResource = ResourceType.PATIENT;
    mockResourceReader(subjectResource);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("Number of patients", "count()")
        .withGrouping("Gender", "gender")
        .build();

    assertResponse("responses/AggregateExecutorTest-simpleQuery.Parameters.json",
        executor.execute(request));
  }

  @Test
  void multipleGroupingsAndAggregations() {
    final ResourceType subjectResource = ResourceType.ENCOUNTER;
    mockResourceReader(subjectResource, ResourceType.ORGANIZATION);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("Number of encounters", "count()")
        .withAggregation("Number of reasons", "reasonCode.count()")
        .withGrouping("Class", "class.code")
        .withGrouping("Reason", "reasonCode.coding.display")
        .withFilter("status = 'finished'")
        .withFilter("serviceProvider.resolve().name = 'ST ELIZABETH\\'S MEDICAL CENTER'")
        .build();

    assertResponse(
        "responses/AggregateExecutorTest-multipleGroupingsAndAggregations.Parameters.json",
        executor.execute(request));
  }

  @Test
  void queryWithIntegerGroupings() {
    final ResourceType subjectResource = ResourceType.CLAIM;
    mockResourceReader(subjectResource);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("Number of claims", "count()")
        .withGrouping("Claim item sequence", "item.sequence")
        .build();

    assertResponse("responses/AggregateExecutorTest-queryWithIntegerGroupings.Parameters.json",
        executor.execute(request));
  }

  @Test
  @Disabled
  void queryWithMathExpression() {
    final ResourceType subjectResource = ResourceType.CLAIM;
    mockResourceReader(subjectResource);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("Number of claims", "count()")
        .withGrouping("First claim item sequence + 1", "item.sequence.first() + 1")
        .build();

    assertResponse("responses/AggregateExecutorTest-queryWithMathExpression.Parameters.json",
        executor.execute(request));
  }

  @Test
  void queryWithChoiceElement() {
    final ResourceType subjectResource = ResourceType.PATIENT;
    mockResourceReader(subjectResource);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("Number of patients", "count()")
        .withGrouping("Multiple birth?", "multipleBirthBoolean")
        .build();

    assertResponse("responses/AggregateExecutorTest-queryWithChoiceElement.Parameters.json",
        executor.execute(request));
  }

  @Test
  void queryWithDateComparison() {
    final ResourceType subjectResource = ResourceType.PATIENT;
    mockResourceReader(subjectResource);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("Number of patients", "count()")
        .withFilter("birthDate > @1980 and birthDate < @1990")
        .build();

    assertResponse("responses/AggregateExecutorTest-queryWithDateComparison.Parameters.json",
        executor.execute(request));
  }

  @Test
  void queryWithResolve() {
    final ResourceType subjectResource = ResourceType.ALLERGYINTOLERANCE;
    mockResourceReader(subjectResource, ResourceType.PATIENT);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("Number of allergies", "count()")
        .withGrouping("Patient gender", "patient.resolve().gender")
        .build();

    assertResponse("responses/AggregateExecutorTest-queryWithResolve.Parameters.json",
        executor.execute(request));
  }

  @Test
  void queryWithPolymorphicResolve() {
    final ResourceType subjectResource = ResourceType.DIAGNOSTICREPORT;
    mockResourceReader(subjectResource, ResourceType.PATIENT);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("Number of reports", "count()")
        .withGrouping("Patient active status", "subject.resolve().ofType(Patient).gender")
        .build();

    assertResponse("responses/AggregateExecutorTest-queryWithPolymorphicResolve.Parameters.json",
        executor.execute(request));
  }

  @Test
  void queryWithReverseResolve() {
    final ResourceType subjectResource = ResourceType.PATIENT;
    mockResourceReader(ResourceType.CONDITION, subjectResource);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("Number of patients", "count()")
        .withGrouping("Condition", "reverseResolve(Condition.subject).code.coding.display")
        .build();

    assertResponse("responses/AggregateExecutorTest-queryWithReverseResolve.Parameters.json",
        executor.execute(request));
  }

  @Test
  void queryWithReverseResolveAndCounts() {
    final ResourceType subjectResource = ResourceType.PATIENT;
    mockResourceReader(ResourceType.CONDITION, subjectResource);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("Number of patients", "count()")
        .withGrouping("Condition", "reverseResolve(Condition.subject).code.coding.count()")
        .build();

    assertResponse(
        "responses/AggregateExecutorTest-queryWithReverseResolveAndCounts.Parameters.json",
        executor.execute(request));
  }

  @Test
  void queryMultipleGroupingCounts() {
    final ResourceType subjectResource = ResourceType.PATIENT;
    mockResourceReader(ResourceType.CONDITION, subjectResource);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("Number of patients", "count()")
        .withGrouping("Given name", "name.given")
        .withGrouping("Name prefix", "name.prefix")
        .build();

    assertResponse(
        "responses/AggregateExecutorTest-queryMultipleGroupingCounts.Parameters.json",
        executor.execute(request));
  }

  @Test
  void queryMultipleCountAggregations() {
    final ResourceType subjectResource = ResourceType.PATIENT;
    mockResourceReader(ResourceType.CONDITION, subjectResource);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("Number of patient given names", "name.given.count()")
        .withAggregation("Number of patient prefixes", "name.prefix.count()")
        .withGrouping("Gender", "gender")
        .build();

    assertResponse(
        "responses/AggregateExecutorTest-queryMultipleCountAggregations.Parameters.json",
        executor.execute(request));
  }

  @Test
  @Disabled
  void queryWithWhere() {
    final ResourceType subjectResource = ResourceType.PATIENT;
    mockResourceReader(ResourceType.CONDITION, subjectResource);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("Number of patients", "count()")
        .withGrouping("2010 condition verification status",
            "reverseResolve(Condition.subject).where($this.onsetDateTime > @2010 and "
                + "$this.onsetDateTime < @2011).verificationStatus.coding.code")
        .build();

    assertResponse(
        "responses/AggregateExecutorTest-queryWithWhere.Parameters.json",
        executor.execute(request));
  }

  @Test
  void queryWithMemberOf() {
    final ResourceType subjectResource = ResourceType.PATIENT;
    mockResourceReader(ResourceType.CONDITION, subjectResource);
    final Bundle mockResponse = (Bundle) FhirHelpers.getJsonParser()
        .parseResource(getResourceAsStream(
            "txResponses/MemberOfFunctionTest-memberOfCoding-validate-code-positive.Bundle.json"));

    when(terminologyClient.batch(any(Bundle.class))).thenReturn(mockResponse);

    final String valueSetUrl = "http://snomed.info/sct?fhir_vs=refset/32570521000036109";
    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("Number of patients", "count()")
        .withGrouping("Condition in ED diagnosis reference set",
            "reverseResolve(Condition.subject)" + ".code" + ".memberOf('" + valueSetUrl + "')")
        .build();

    assertResponse(
        "responses/AggregateExecutorTest-queryWithMemberOf.Parameters.json",
        executor.execute(request));
  }

  @Test
  void queryWithDateTimeGrouping() {
    final ResourceType subjectResource = ResourceType.MEDICATIONREQUEST;
    mockResourceReader(subjectResource);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("Number of prescriptions", "count()")
        .withGrouping("Authored on", "authoredOn")
        .build();

    assertResponse(
        "responses/AggregateExecutorTest-queryWithDateTimeGrouping.Parameters.json",
        executor.execute(request));
  }

  @Test
  @Disabled
  void queryWithWhereAsComparisonOperand() {
    final ResourceType subjectResource = ResourceType.PATIENT;
    mockResourceReader(subjectResource, ResourceType.MEDICATIONREQUEST);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("Number of patients", "count()")
        .withGrouping("First prescription falls before 2018-05-06",
            "@2018-05-06 > reverseResolve(MedicationRequest.subject).where("
                + "$this.medicationCodeableConcept.coding contains "
                + "http://www.nlm.nih.gov/research/umls/rxnorm|243670).first().authoredOn")
        .build();

    assertResponse(
        "responses/AggregateExecutorTest-queryWithWhereAsComparisonOperand.Parameters.json",
        executor.execute(request));
  }

  private static void assertResponse(@Nonnull final String expectedPath,
      @Nonnull final AggregateResponse response) {
    final Parameters parameters = response.toParameters();
    final String actualJson = getJsonParser().encodeResourceToString(parameters);
    assertJson(expectedPath, actualJson);
  }

  private void mockResourceReader(final ResourceType... resourceTypes) {
    TestHelpers.mockResourceReader(resourceReader, spark, resourceTypes);
  }

}