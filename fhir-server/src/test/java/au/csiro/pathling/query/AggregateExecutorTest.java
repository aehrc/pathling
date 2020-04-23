/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.query;

import static au.csiro.pathling.TestUtilities.checkExpectedJson;
import static au.csiro.pathling.TestUtilities.getJsonParser;
import static au.csiro.pathling.TestUtilities.getResourceAsStream;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.TestUtilities;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.fhir.TerminologyClientFactory;
import au.csiro.pathling.query.AggregateRequest.Aggregation;
import au.csiro.pathling.query.AggregateRequest.Grouping;
import ca.uhn.fhir.rest.param.StringAndListParam;
import ca.uhn.fhir.rest.param.StringParam;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.ConceptMap;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.hl7.fhir.r4.model.StringType;
import org.json.JSONException;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

/**
 * @author John Grimes
 */
@Category(au.csiro.pathling.UnitTest.class)
public class AggregateExecutorTest extends ExecutorTest {

  private AggregateExecutor executor;
  private TerminologyClient terminologyClient;
  private Parameters response;
  private ExecutorConfiguration configuration;
  private ResourceType subjectResource;

  @Before
  public void setUp() throws Exception {
    super.setUp();

    terminologyClient = mock(TerminologyClient.class, Mockito.withSettings().serializable());
    TerminologyClientFactory terminologyClientFactory =
        mock(TerminologyClientFactory.class, Mockito.withSettings().serializable());
    when(terminologyClientFactory.build(any())).thenReturn(terminologyClient);

    // Create and configure a new AggregateExecutor.
    Path warehouseDirectory = Files.createTempDirectory("pathling-test-");
    configuration = new ExecutorConfiguration(spark,
        TestUtilities.getFhirContext(), terminologyClientFactory, terminologyClient, mockReader);
    configuration.setWarehouseUrl(warehouseDirectory.toString());
    configuration.setDatabaseName("test");
    configuration.setFhirEncoders(FhirEncoders.forR4().getOrCreate());

    executor = new AggregateExecutor(configuration);
  }

  /**
   * Test that the drill down expression from the first grouping from each aggregate result can be
   * successfully executed using the FHIRPath search.
   */
  @After
  public void runFirstGroupingThroughSearch() {
    Optional<ParametersParameterComponent> firstGroupingOptional = response.getParameter().stream()
        .filter(param -> param.getName().equals("grouping"))
        .findFirst();
    assertThat(firstGroupingOptional).isPresent();
    ParametersParameterComponent firstGrouping = firstGroupingOptional.get();

    Optional<ParametersParameterComponent> drillDownOptional = firstGrouping.getPart().stream()
        .filter(param -> param.getName().equals("drillDown"))
        .findFirst();
    assertThat(drillDownOptional).isPresent();
    ParametersParameterComponent drillDown = drillDownOptional.get();
    String drillDownString = ((StringType) drillDown.getValue()).getValue();

    StringAndListParam filters = new StringAndListParam();
    filters.addAnd(new StringParam(drillDownString));
    SearchExecutor searchExecutor = new SearchExecutor(configuration, subjectResource,
        filters);
    List<IBaseResource> resources = searchExecutor.getResources(0, 100);
    assertThat(resources.size()).isGreaterThan(0);
  }

  @Test
  public void queryWithMultipleGroupings() throws IOException, JSONException {
    subjectResource = ResourceType.ENCOUNTER;
    mockResourceReader(subjectResource);

    // Build a AggregateRequest to pass to the executor.
    AggregateRequest request = new AggregateRequest();
    request.setSubjectResource(subjectResource);

    Aggregation aggregation = new Aggregation();
    aggregation.setLabel("Number of encounters");
    aggregation.setExpression("count()");
    request.getAggregations().add(aggregation);

    Grouping grouping1 = new Grouping();
    grouping1.setLabel("Class");
    grouping1.setExpression("class.code");
    request.getGroupings().add(grouping1);

    Grouping grouping2 = new Grouping();
    grouping2.setLabel("Reason");
    grouping2.setExpression("reasonCode.coding.display");
    request.getGroupings().add(grouping2);

    // Execute the query.
    AggregateResponse response = executor.execute(request);

    // Check the response against an expected response.
    this.response = response.toParameters();
    String actualJson = getJsonParser().encodeResourceToString(this.response);
    checkExpectedJson(actualJson,
        "responses/AggregateExecutorTest-queryWithMultipleGroupings.Parameters.json");
  }

  @Test
  public void queryWithFilter() throws IOException, JSONException {
    subjectResource = ResourceType.PATIENT;
    mockResourceReader(subjectResource);

    // Build a AggregateRequest to pass to the executor.
    AggregateRequest request = new AggregateRequest();
    request.setSubjectResource(ResourceType.PATIENT);

    Aggregation aggregation = new Aggregation();
    aggregation.setLabel("Number of patients");
    aggregation.setExpression("count()");
    request.getAggregations().add(aggregation);

    Grouping grouping1 = new Grouping();
    grouping1.setLabel("Gender");
    grouping1.setExpression("gender");
    request.getGroupings().add(grouping1);

    request.getFilters().add("gender = 'female'");

    // Execute the query.
    AggregateResponse response = executor.execute(request);

    // Check the response against an expected response.
    this.response = response.toParameters();
    String actualJson = getJsonParser().encodeResourceToString(this.response);
    checkExpectedJson(actualJson,
        "responses/AggregateExecutorTest-queryWithFilter.Parameters.json");
  }

  @Test
  public void queryWithIntegerGroupings() throws IOException, JSONException {
    subjectResource = ResourceType.CLAIM;
    mockResourceReader(subjectResource);

    // Build a AggregateRequest to pass to the executor.
    AggregateRequest request = new AggregateRequest();
    request.setSubjectResource(subjectResource);

    Aggregation aggregation = new Aggregation();
    aggregation.setLabel("Number of claims");
    aggregation.setExpression("count()");
    request.getAggregations().add(aggregation);

    Grouping grouping = new Grouping();
    grouping.setLabel("Claim item sequence");
    grouping.setExpression("item.sequence");
    request.getGroupings().add(grouping);

    // Execute the query.
    AggregateResponse response = executor.execute(request);

    // Check the response against an expected response.
    this.response = response.toParameters();
    String actualJson = getJsonParser().encodeResourceToString(this.response);
    checkExpectedJson(actualJson,
        "responses/AggregateExecutorTest-queryWithIntegerGroupings.Parameters.json");
  }

  @Test
  public void queryWithMathExpression() throws IOException, JSONException {
    subjectResource = ResourceType.CLAIM;
    mockResourceReader(subjectResource);

    // Build a AggregateRequest to pass to the executor.
    AggregateRequest request = new AggregateRequest();
    request.setSubjectResource(subjectResource);

    Aggregation aggregation = new Aggregation();
    aggregation.setLabel("Number of claims");
    aggregation.setExpression("count()");
    request.getAggregations().add(aggregation);

    Grouping grouping = new Grouping();
    grouping.setLabel("First claim item sequence + 1");
    grouping.setExpression("item.sequence.first() + 1");
    request.getGroupings().add(grouping);

    // Execute the query.
    AggregateResponse response = executor.execute(request);

    // Check the response against an expected response.
    this.response = response.toParameters();
    String actualJson = getJsonParser().encodeResourceToString(this.response);
    checkExpectedJson(actualJson,
        "responses/AggregateExecutorTest-queryWithMathExpression.Parameters.json");
  }

  @Test
  public void queryWithChoiceElement() throws IOException, JSONException {
    subjectResource = ResourceType.PATIENT;
    mockResourceReader(subjectResource);

    // Build a AggregateRequest to pass to the executor.
    AggregateRequest request = new AggregateRequest();
    request.setSubjectResource(subjectResource);

    Aggregation aggregation = new Aggregation();
    aggregation.setLabel("Number of patients");
    aggregation.setExpression("count()");
    request.getAggregations().add(aggregation);

    Grouping grouping = new Grouping();
    grouping.setLabel("Multiple birth?");
    grouping.setExpression("multipleBirthBoolean");
    request.getGroupings().add(grouping);

    // Execute the query.
    AggregateResponse response = executor.execute(request);

    // Check the response against an expected response.
    this.response = response.toParameters();
    String actualJson = getJsonParser().encodeResourceToString(this.response);
    checkExpectedJson(actualJson,
        "responses/AggregateExecutorTest-queryWithChoiceElement.Parameters.json");
  }

  @Test
  public void queryWithDateComparison() throws IOException, JSONException {
    subjectResource = ResourceType.PATIENT;
    mockResourceReader(subjectResource);

    // Build a AggregateRequest to pass to the executor.
    AggregateRequest request = new AggregateRequest();
    request.setSubjectResource(subjectResource);

    Aggregation aggregation = new Aggregation();
    aggregation.setLabel("Number of patients");
    aggregation.setExpression("count()");
    request.getAggregations().add(aggregation);

    request.getFilters().add("birthDate > @1980 and birthDate < @1990");

    // Execute the query.
    AggregateResponse response = executor.execute(request);

    // Check the response against an expected response.
    this.response = response.toParameters();
    String actualJson = getJsonParser().encodeResourceToString(this.response);
    checkExpectedJson(actualJson,
        "responses/AggregateExecutorTest-queryWithDateComparison.Parameters.json");
  }

  @Test
  public void queryWithResolve() throws IOException, JSONException {
    subjectResource = ResourceType.ALLERGYINTOLERANCE;
    mockResourceReader(subjectResource, ResourceType.PATIENT);

    // Build a AggregateRequest to pass to the executor.
    AggregateRequest request = new AggregateRequest();
    request.setSubjectResource(subjectResource);

    Aggregation aggregation = new Aggregation();
    aggregation.setLabel("Number of allergies");
    aggregation.setExpression("count()");
    request.getAggregations().add(aggregation);

    Grouping grouping1 = new Grouping();
    grouping1.setLabel("Patient gender");
    grouping1.setExpression("patient.resolve().gender");
    request.getGroupings().add(grouping1);

    // Execute the query.
    AggregateResponse response = executor.execute(request);

    // Check the response against an expected response.
    this.response = response.toParameters();
    String actualJson = getJsonParser().encodeResourceToString(this.response);
    checkExpectedJson(actualJson,
        "responses/AggregateExecutorTest-queryWithResolve.Parameters.json");
  }

  @Test
  public void queryWithPolymorphicResolve() throws IOException, JSONException {
    subjectResource = ResourceType.DIAGNOSTICREPORT;
    mockResourceReader(subjectResource, ResourceType.PATIENT);

    // Build a AggregateRequest to pass to the executor.
    AggregateRequest request = new AggregateRequest();
    request.setSubjectResource(subjectResource);

    Aggregation aggregation = new Aggregation();
    aggregation.setLabel("Number of reports");
    aggregation.setExpression("count()");
    request.getAggregations().add(aggregation);

    Grouping grouping1 = new Grouping();
    grouping1.setLabel("Patient active status");
    grouping1.setExpression("subject.resolve().ofType(Patient).gender");
    request.getGroupings().add(grouping1);

    // Execute the query.
    AggregateResponse response = executor.execute(request);

    // Check the response against an expected response.
    this.response = response.toParameters();
    String actualJson = getJsonParser().encodeResourceToString(this.response);
    checkExpectedJson(actualJson,
        "responses/AggregateExecutorTest-queryWithPolymorphicResolve.Parameters.json");
  }

  @Test
  public void queryWithReverseResolve() throws IOException, JSONException {
    subjectResource = ResourceType.PATIENT;
    mockResourceReader(ResourceType.CONDITION, subjectResource);

    // Build a AggregateRequest to pass to the executor.
    AggregateRequest request = new AggregateRequest();
    request.setSubjectResource(subjectResource);

    Aggregation aggregation = new Aggregation();
    aggregation.setLabel("Number of patients");
    aggregation.setExpression("count()");
    request.getAggregations().add(aggregation);

    Grouping grouping1 = new Grouping();
    grouping1.setLabel("Condition");
    grouping1.setExpression("reverseResolve(Condition.subject).code.coding.display");
    request.getGroupings().add(grouping1);

    // Execute the query.
    AggregateResponse response = executor.execute(request);

    // Check the response against an expected response.
    this.response = response.toParameters();
    String actualJson = getJsonParser().encodeResourceToString(this.response);
    checkExpectedJson(actualJson,
        "responses/AggregateExecutorTest-queryWithReverseResolve.Parameters.json");
  }


  @Test
  public void queryWithReverseResolveAndCounts() throws IOException, JSONException {
    subjectResource = ResourceType.PATIENT;
    mockResourceReader(ResourceType.CONDITION, subjectResource);

    // Build a AggregateRequest to pass to the executor.
    AggregateRequest request = new AggregateRequest();
    request.setSubjectResource(subjectResource);

    Aggregation aggregation = new Aggregation();
    aggregation.setLabel("Number of patients");
    aggregation.setExpression("count()");
    request.getAggregations().add(aggregation);

    Grouping grouping1 = new Grouping();
    grouping1.setLabel("Condition");
    grouping1.setExpression("reverseResolve(Condition.subject).code.coding.count()");
    request.getGroupings().add(grouping1);

    // Execute the query.
    AggregateResponse response = executor.execute(request);

    // Check the response against an expected response.
    this.response = response.toParameters();
    String actualJson = getJsonParser().encodeResourceToString(this.response);
    checkExpectedJson(actualJson,
        "responses/AggregateExecutorTest-queryWithReverseResolveAndCounts.Parameters.json");
  }

  @Test
  public void queryMultipleGroupingCounts() throws IOException, JSONException {
    subjectResource = ResourceType.PATIENT;
    mockResourceReader(ResourceType.CONDITION, subjectResource);

    // Build a AggregateRequest to pass to the executor.
    AggregateRequest request = new AggregateRequest();
    request.setSubjectResource(subjectResource);

    Aggregation aggregation = new Aggregation();
    aggregation.setLabel("Number of patients");
    aggregation.setExpression("count()");
    request.getAggregations().add(aggregation);

    Grouping grouping1 = new Grouping();
    grouping1.setLabel("Given name");
    grouping1.setExpression("name.given");
    request.getGroupings().add(grouping1);

    Grouping grouping2 = new Grouping();
    grouping2.setLabel("Name prefix");
    grouping2.setExpression("name.prefix");
    request.getGroupings().add(grouping2);

    // Execute the query.
    AggregateResponse response = executor.execute(request);

    // Check the response against an expected response.
    this.response = response.toParameters();
    String actualJson = getJsonParser().encodeResourceToString(this.response);
    checkExpectedJson(actualJson,
        "responses/AggregateExecutorTest-queryMultipleGroupingCounts.Parameters.json");
  }


  @Test
  public void queryMultipleCountAggregations() throws IOException, JSONException {
    subjectResource = ResourceType.PATIENT;
    mockResourceReader(ResourceType.CONDITION, subjectResource);

    // Build a AggregateRequest to pass to the executor.
    AggregateRequest request = new AggregateRequest();
    request.setSubjectResource(subjectResource);

    Aggregation aggregation1 = new Aggregation();
    aggregation1.setLabel("Number of patient given names");
    aggregation1.setExpression("name.given.count()");
    request.getAggregations().add(aggregation1);

    Aggregation aggregation2 = new Aggregation();
    aggregation2.setLabel("Number of patient prefixes");
    aggregation2.setExpression("name.prefix.count()");
    request.getAggregations().add(aggregation2);

    Grouping grouping1 = new Grouping();
    grouping1.setLabel("Gender");
    grouping1.setExpression("gender");
    request.getGroupings().add(grouping1);

    // Execute the query.
    AggregateResponse response = executor.execute(request);

    // Check the response against an expected response.
    this.response = response.toParameters();
    String actualJson = getJsonParser().encodeResourceToString(this.response);

    checkExpectedJson(actualJson,
        "responses/AggregateExecutorTest-queryMultipleCountAggregations.Parameters.json");
  }

  @Test
  public void queryWithWhere() throws IOException, JSONException {
    subjectResource = ResourceType.PATIENT;
    mockResourceReader(ResourceType.CONDITION, subjectResource);

    // Build a AggregateRequest to pass to the executor.
    AggregateRequest request = new AggregateRequest();
    request.setSubjectResource(subjectResource);

    Aggregation aggregation = new Aggregation();
    aggregation.setLabel("Number of patients");
    aggregation.setExpression("count()");
    request.getAggregations().add(aggregation);

    Grouping grouping = new Grouping();
    grouping.setLabel("2010 condition verification status");
    grouping.setExpression(
        "reverseResolve(Condition.subject).where($this.onsetDateTime > @2010 and $this.onsetDateTime < @2011).verificationStatus.coding.code");
    request.getGroupings().add(grouping);

    // Execute the query.
    AggregateResponse response = executor.execute(request);

    // Check the response against an expected response.
    this.response = response.toParameters();
    String actualJson = getJsonParser().encodeResourceToString(this.response);
    checkExpectedJson(actualJson,
        "responses/AggregateExecutorTest-queryWithWhere.Parameters.json");
  }

  @Test
  public void queryWithMemberOf() throws IOException, JSONException {
    subjectResource = ResourceType.PATIENT;
    mockResourceReader(ResourceType.CONDITION, subjectResource);
    Bundle mockResponse = (Bundle) TestUtilities.getJsonParser().parseResource(getResourceAsStream(
        "txResponses/MemberOfFunctionTest-memberOfCoding-validate-code-positive.Bundle.json"));

    // Mock out responses from the terminology server.
    when(terminologyClient.batch(any(Bundle.class))).thenReturn(mockResponse);

    // Build a AggregateRequest to pass to the executor.
    AggregateRequest request = new AggregateRequest();
    request.setSubjectResource(subjectResource);

    Aggregation aggregation = new Aggregation();
    aggregation.setLabel("Number of patients");
    aggregation.setExpression("count()");
    request.getAggregations().add(aggregation);

    Grouping grouping1 = new Grouping();
    grouping1.setLabel("Condition in ED diagnosis reference set?");
    String valueSetUrl = "http://snomed.info/sct?fhir_vs=refset/32570521000036109";
    grouping1.setExpression(
        "reverseResolve(Condition.subject)" + ".code" + ".memberOf('" + valueSetUrl + "')");
    request.getGroupings().add(grouping1);

    // Execute the query.
    AggregateResponse response = executor.execute(request);

    // Check the response against an expected response.
    this.response = response.toParameters();
    String actualJson = getJsonParser().encodeResourceToString(this.response);
    checkExpectedJson(actualJson,
        "responses/AggregateExecutorTest-queryWithMemberOf.Parameters.json");
  }

  /**
   * Patient/121503c8-9564-4b48-9086-a22df717948e has Condition with Coding:
   * http://snomed.info/sct|363406005 (Malignant tumor of colon)
   * <p>
   * Patient/9360820c-8602-4335-8b50-c88d627a0c20 has Condition with Coding:
   * http://snomed.info/sct|94260004 (Secondary malignant neoplasm of colon)
   * <p>
   * http://snomed.info/sct|363406005 -- subsumes --> http://snomed.info/sct|94260004
   */
  @Test
  public void queryWithSubsumes() throws IOException, JSONException {
    subjectResource = ResourceType.PATIENT;
    mockResourceReader(ResourceType.CONDITION, ResourceType.PATIENT);
    ConceptMap mockResponse = (ConceptMap) TestUtilities.getJsonParser().parseResource(
        getResourceAsStream("txResponses//SubsumesFunctionTest-closure.ConceptMap.json"));

    // Mock out responses from the terminology server.
    when(terminologyClient.closure(any(), any(), any())).thenReturn(mockResponse);

    // Build a AggregateRequest to pass to the executor.
    AggregateRequest request = new AggregateRequest();
    request.setSubjectResource(ResourceType.PATIENT);

    Aggregation aggregation = new Aggregation();
    aggregation.setLabel("Number of patients");
    aggregation.setExpression("count()");
    request.getAggregations().add(aggregation);

    Grouping grouping1 = new Grouping();
    grouping1.setLabel("Condition subsumes http://snomed.info/sct|94260004");
    grouping1.setExpression(
        "reverseResolve(Condition.subject).code.subsumes(http://snomed.info/sct|94260004)");
    request.getGroupings().add(grouping1);

    // Execute the query.
    AggregateResponse response = executor.execute(request);

    // Check the response against an expected response.
    this.response = response.toParameters();
    String actualJson = getJsonParser().encodeResourceToString(this.response);
    System.out.println(actualJson);
    checkExpectedJson(actualJson,
        "responses/AggregateExecutorTest-queryWithSubsumes.Parameters.json");
  }

}
