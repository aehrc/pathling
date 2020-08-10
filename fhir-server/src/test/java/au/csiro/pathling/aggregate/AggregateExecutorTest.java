/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.aggregate;

import static au.csiro.pathling.test.assertions.Assertions.assertJson;
import static au.csiro.pathling.test.helpers.FhirHelpers.getJsonParser;
import static au.csiro.pathling.test.helpers.TestHelpers.getResourceAsStream;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.Configuration;
import au.csiro.pathling.aggregate.AggregateResponse.Grouping;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.fhir.TerminologyClientFactory;
import au.csiro.pathling.io.ResourceReader;
import au.csiro.pathling.search.SearchExecutor;
import au.csiro.pathling.test.helpers.FhirHelpers;
import au.csiro.pathling.test.helpers.TestHelpers;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.api.server.IBundleProvider;
import ca.uhn.fhir.rest.param.StringAndListParam;
import ca.uhn.fhir.rest.param.StringParam;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.IntegerType;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.ValueSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

/**
 * @author John Grimes
 */
@SpringBootTest
@Tag("IntegrationTest")
@ActiveProfiles("test")
@Slf4j
class AggregateExecutorTest extends QueryExecutorTest {

  private final AggregateExecutor executor;
  private final SparkSession spark;
  private final ResourceReader resourceReader;
  private final TerminologyClient terminologyClient;
  private final TerminologyClientFactory terminologyClientFactory;
  private final Configuration configuration;
  private final FhirContext fhirContext;
  private final FhirEncoders fhirEncoders;
  private AggregateResponse response;
  private ResourceType subjectResource;

  @Autowired
  public AggregateExecutorTest(final Configuration configuration, final FhirContext fhirContext,
      final SparkSession spark, final FhirEncoders fhirEncoders) {
    this.configuration = configuration;
    this.fhirContext = fhirContext;
    this.spark = spark;
    this.fhirEncoders = fhirEncoders;
    resourceReader = mock(ResourceReader.class);
    terminologyClient = mock(TerminologyClient.class, Mockito.withSettings().serializable());

    terminologyClientFactory =
        mock(TerminologyClientFactory.class, Mockito.withSettings().serializable());
    when(terminologyClientFactory.build(any())).thenReturn(terminologyClient);

    executor = new FreshAggregateExecutor(configuration, fhirContext, spark,
        resourceReader, Optional.of(terminologyClient), Optional.of(terminologyClientFactory));
  }

  /**
   * Test that the drill down expression from the first grouping from each aggregate result can be
   * successfully executed using the FHIRPath search.
   */
  @AfterEach
  public void runFirstGroupingThroughSearch() {
    final Optional<Grouping> firstGroupingOptional = response.getGroupings()
        .stream()
        .findFirst();
    assertTrue(firstGroupingOptional.isPresent());
    final Grouping firstGrouping = firstGroupingOptional.get();
    final String drillDown = firstGrouping.getDrillDown();

    final StringAndListParam filters = new StringAndListParam();
    filters.addAnd(new StringParam(drillDown));
    final IBundleProvider searchExecutor = new SearchExecutor(configuration, fhirContext, spark,
        resourceReader, Optional.of(terminologyClient), Optional.of(terminologyClientFactory),
        fhirEncoders, subjectResource, Optional.of(filters));
    final List<IBaseResource> resources = searchExecutor.getResources(0, 100);
    assertTrue(resources.size() > 0);
  }

  @Test
  void simpleQuery() {
    subjectResource = ResourceType.PATIENT;
    mockResourceReader(subjectResource);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("Number of patients", "count()")
        .withGrouping("Gender", "gender")
        .build();

    response = executor.execute(request);
    assertResponse("responses/AggregateExecutorTest-simpleQuery.Parameters.json", response);
  }

  @Test
  void multipleGroupingsAndAggregations() {
    subjectResource = ResourceType.ENCOUNTER;
    mockResourceReader(subjectResource, ResourceType.ORGANIZATION);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("Number of encounters", "count()")
        .withAggregation("Number of reasons", "reasonCode.count()")
        .withGrouping("Class", "class.code")
        .withGrouping("Reason", "reasonCode.coding.display")
        .withFilter("status = 'finished'")
        .withFilter("serviceProvider.resolve().name = 'ST ELIZABETH\\'S MEDICAL CENTER'")
        .build();

    response = executor.execute(request);
    assertResponse(
        "responses/AggregateExecutorTest-multipleGroupingsAndAggregations.Parameters.json",
        response);
  }

  @Test
  void queryWithIntegerGroupings() {
    subjectResource = ResourceType.CLAIM;
    mockResourceReader(subjectResource);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("Number of claims", "count()")
        .withGrouping("Claim item sequence", "item.sequence")
        .build();

    response = executor.execute(request);
    assertResponse("responses/AggregateExecutorTest-queryWithIntegerGroupings.Parameters.json",
        response);
  }

  @Test
  @Disabled
  void queryWithMathExpression() {
    subjectResource = ResourceType.CLAIM;
    mockResourceReader(subjectResource);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("Number of claims", "count()")
        .withGrouping("First claim item sequence + 1", "item.sequence.first() + 1")
        .build();

    response = executor.execute(request);
    assertResponse("responses/AggregateExecutorTest-queryWithMathExpression.Parameters.json",
        response);
  }

  @Test
  void queryWithChoiceElement() {
    subjectResource = ResourceType.PATIENT;
    mockResourceReader(subjectResource);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("Number of patients", "count()")
        .withGrouping("Multiple birth?", "multipleBirthBoolean")
        .build();

    response = executor.execute(request);
    assertResponse("responses/AggregateExecutorTest-queryWithChoiceElement.Parameters.json",
        response);
  }

  @Test
  void queryWithDateComparison() {
    subjectResource = ResourceType.PATIENT;
    mockResourceReader(subjectResource);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("Number of patients", "count()")
        .withFilter("birthDate > @1980 and birthDate < @1990")
        .build();

    response = executor.execute(request);
    assertResponse("responses/AggregateExecutorTest-queryWithDateComparison.Parameters.json",
        response);
  }

  @Test
  void queryWithResolve() {
    subjectResource = ResourceType.ALLERGYINTOLERANCE;
    mockResourceReader(subjectResource, ResourceType.PATIENT);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("Number of allergies", "count()")
        .withGrouping("Patient gender", "patient.resolve().gender")
        .build();

    response = executor.execute(request);
    assertResponse("responses/AggregateExecutorTest-queryWithResolve.Parameters.json", response);
  }

  @Test
  void queryWithPolymorphicResolve() {
    subjectResource = ResourceType.DIAGNOSTICREPORT;
    mockResourceReader(subjectResource, ResourceType.PATIENT);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("Number of reports", "count()")
        .withGrouping("Patient active status", "subject.resolve().ofType(Patient).gender")
        .build();

    response = executor.execute(request);
    assertResponse("responses/AggregateExecutorTest-queryWithPolymorphicResolve.Parameters.json",
        response);
  }

  @Test
  void queryWithReverseResolve() {
    subjectResource = ResourceType.PATIENT;
    mockResourceReader(ResourceType.CONDITION, subjectResource);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("Number of patients", "count()")
        .withGrouping("Condition", "reverseResolve(Condition.subject).code.coding.display")
        .build();

    response = executor.execute(request);
    assertResponse("responses/AggregateExecutorTest-queryWithReverseResolve.Parameters.json",
        response);
  }

  @Test
  void queryWithReverseResolveAndCounts() {
    subjectResource = ResourceType.PATIENT;
    mockResourceReader(ResourceType.CONDITION, subjectResource);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("Number of patients", "count()")
        .withGrouping("Condition", "reverseResolve(Condition.subject).code.coding.count()")
        .build();

    response = executor.execute(request);
    assertResponse(
        "responses/AggregateExecutorTest-queryWithReverseResolveAndCounts.Parameters.json",
        response);
  }

  @Test
  void queryMultipleGroupingCounts() {
    subjectResource = ResourceType.PATIENT;
    mockResourceReader(ResourceType.CONDITION, subjectResource);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("Number of patients", "count()")
        .withGrouping("Given name", "name.given")
        .withGrouping("Name prefix", "name.prefix")
        .build();

    response = executor.execute(request);
    assertResponse("responses/AggregateExecutorTest-queryMultipleGroupingCounts.Parameters.json",
        response);
  }

  @Test
  void queryMultipleCountAggregations() {
    subjectResource = ResourceType.PATIENT;
    mockResourceReader(ResourceType.CONDITION, subjectResource);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("Number of patient given names", "name.given.count()")
        .withAggregation("Number of patient prefixes", "name.prefix.count()")
        .withGrouping("Gender", "gender")
        .build();

    response = executor.execute(request);
    assertResponse("responses/AggregateExecutorTest-queryMultipleCountAggregations.Parameters.json",
        response);
  }

  @Test
  @Disabled
  void queryWithWhere() {
    subjectResource = ResourceType.PATIENT;
    mockResourceReader(ResourceType.CONDITION, subjectResource);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("Number of patients", "count()")
        .withGrouping("2010 condition verification status",
            "reverseResolve(Condition.subject).where($this.onsetDateTime > @2010 and "
                + "$this.onsetDateTime < @2011).verificationStatus.coding.code")
        .build();

    response = executor.execute(request);
    assertResponse("responses/AggregateExecutorTest-queryWithWhere.Parameters.json", response);
  }

  @Test
  void queryWithMemberOf() {
    subjectResource = ResourceType.PATIENT;
    mockResourceReader(ResourceType.CONDITION, subjectResource);
    final ValueSet mockResponse = (ValueSet) FhirHelpers.getJsonParser()
        .parseResource(getResourceAsStream(
            "txResponses/AggregateExecutorTest-queryWithMemberOf.ValueSet.json"));

    when(terminologyClient.expand(any(ValueSet.class), any(IntegerType.class)))
        .thenReturn(mockResponse);

    final String valueSetUrl = "http://snomed.info/sct?fhir_vs=refset/32570521000036109";
    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("Number of patients", "count()")
        .withGrouping("Condition in ED diagnosis reference set",
            "reverseResolve(Condition.subject)" + ".code" + ".memberOf('" + valueSetUrl + "')")
        .build();

    response = executor.execute(request);
    assertResponse("responses/AggregateExecutorTest-queryWithMemberOf.Parameters.json", response);
  }

  @Test
  void queryWithDateTimeGrouping() {
    subjectResource = ResourceType.MEDICATIONREQUEST;
    mockResourceReader(subjectResource);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("Number of prescriptions", "count()")
        .withGrouping("Authored on", "authoredOn")
        .build();

    response = executor.execute(request);
    assertResponse("responses/AggregateExecutorTest-queryWithDateTimeGrouping.Parameters.json",
        response);
  }

  @Test
  @Disabled
  void queryWithWhereAsComparisonOperand() {
    subjectResource = ResourceType.PATIENT;
    mockResourceReader(subjectResource, ResourceType.MEDICATIONREQUEST);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("Number of patients", "count()")
        .withGrouping("First prescription falls before 2018-05-06",
            "@2018-05-06 > reverseResolve(MedicationRequest.subject).where("
                + "$this.medicationCodeableConcept.coding contains "
                + "http://www.nlm.nih.gov/research/umls/rxnorm|243670).first().authoredOn")
        .build();

    response = executor.execute(request);
    assertResponse(
        "responses/AggregateExecutorTest-queryWithWhereAsComparisonOperand.Parameters.json",
        response);
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