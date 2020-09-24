/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.aggregate;

import static au.csiro.pathling.test.helpers.TestHelpers.getResourceAsStream;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import au.csiro.pathling.Configuration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.test.helpers.FhirHelpers;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.param.UriParam;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.CodeSystem;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.IntegerType;
import org.hl7.fhir.r4.model.ValueSet;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author John Grimes
 */
@Slf4j
class AggregateQueryTest extends AggregateExecutorTest {

  @Autowired
  public AggregateQueryTest(final Configuration configuration, final FhirContext fhirContext,
      final SparkSession spark, final FhirEncoders fhirEncoders) {
    super(configuration, fhirContext, spark, fhirEncoders);
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
    assertResponse("AggregateQueryTest/simpleQuery.Parameters.json", response);
  }

  @Test
  // empty function
  @Disabled
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
        "AggregateQueryTest/multipleGroupingsAndAggregations.Parameters.json",
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
    assertResponse("AggregateQueryTest/queryWithIntegerGroupings.Parameters.json",
        response);
  }

  @Test
  // first function
  @Disabled
  void queryWithMathExpression() {
    subjectResource = ResourceType.CLAIM;
    mockResourceReader(subjectResource);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("Number of claims", "count()")
        .withGrouping("First claim item sequence + 1", "item.sequence.first() + 1")
        .build();

    response = executor.execute(request);
    assertResponse("AggregateQueryTest/queryWithMathExpression.Parameters.json",
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
    assertResponse("AggregateQueryTest/queryWithChoiceElement.Parameters.json",
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
    assertResponse("AggregateQueryTest/queryWithDateComparison.Parameters.json",
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
    assertResponse("AggregateQueryTest/queryWithResolve.Parameters.json", response);
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
    assertResponse("AggregateQueryTest/queryWithPolymorphicResolve.Parameters.json",
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
    assertResponse("AggregateQueryTest/queryWithReverseResolve.Parameters.json",
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
        "AggregateQueryTest/queryWithReverseResolveAndCounts.Parameters.json",
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
    assertResponse("AggregateQueryTest/queryMultipleGroupingCounts.Parameters.json",
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
    assertResponse("AggregateQueryTest/queryMultipleCountAggregations.Parameters.json",
        response);
  }

  @Test
  // empty function
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
    assertResponse("AggregateQueryTest/queryWithWhere.Parameters.json", response);
  }

  @Test
  void queryWithMemberOf() {
    subjectResource = ResourceType.PATIENT;
    mockResourceReader(ResourceType.CONDITION, subjectResource);
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

    final String valueSetUrl = "http://snomed.info/sct?fhir_vs=refset/32570521000036109";
    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("Number of patients", "count()")
        .withGrouping("Condition in ED diagnosis reference set",
            "reverseResolve(Condition.subject).code.memberOf('" + valueSetUrl + "')")
        .build();

    response = executor.execute(request);
    assertResponse("AggregateQueryTest/queryWithMemberOf.Parameters.json", response);
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
    assertResponse("AggregateQueryTest/queryWithDateTimeGrouping.Parameters.json",
        response);
  }

  @Test
  // first function
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
        "AggregateQueryTest/queryWithWhereAsComparisonOperand.Parameters.json",
        response);
  }

  @Test
  void queryWithAmbiguousSelfJoin() {
    subjectResource = ResourceType.MEDICATIONREQUEST;
    mockResourceReader(subjectResource);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("Number of medication requests", "count()")
        .withGrouping("Status", "status")
        .withFilter("authoredOn < @2018 and authoredOn > @2000")
        .build();

    response = executor.execute(request);
    assertResponse("AggregateQueryTest/queryWithAmbiguousSelfJoin.Parameters.json",
        response);
  }

  @Test
  void queryWithWhereAndMembership() {
    subjectResource = ResourceType.PATIENT;
    mockResourceReader(subjectResource, ResourceType.OBSERVATION);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("Number of patients", "count()")
        .withGrouping(
            "reverseResolve(Observation.subject).where($this.code.coding contains http://loinc.org|8302-2).status")
        .build();

    response = executor.execute(request);
    assertResponse("AggregateQueryTest/queryWithWhereAndMembership.Parameters.json",
        response);
  }

  @Test
  // empty function
  @Disabled
  void queryWithWhereAndBoolean() {
    subjectResource = ResourceType.PATIENT;
    mockResourceReader(subjectResource, ResourceType.OBSERVATION);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("Number of patients", "count()")
        .withGrouping(
            "where($this.gender = 'male' and $this.birthDate > @1990).communication.language.text")
        .build();

    response = executor.execute(request);
    assertResponse("AggregateQueryTest/queryWithWhereAndBoolean.Parameters.json",
        response);
  }

  @Test
  void queryWithWhereAndLiterals() {
    subjectResource = ResourceType.PATIENT;
    mockResourceReader(subjectResource);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("Number of patients", "count()")
        .withGrouping("5.where($this = 6)")
        .build();

    response = executor.execute(request);
    assertResponse("AggregateQueryTest/queryWithAmbiguousSelfJoin.Parameters.json",
        response);
  }

  @Test
  void queryWithWhereAndTrue() {
    subjectResource = ResourceType.PATIENT;
    mockResourceReader(subjectResource);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("Number of patients", "count()")
        .withGrouping("where(true)")
        .build();

    response = executor.execute(request);
    assertResponse("AggregateQueryTest/queryWithAmbiguousSelfJoin.Parameters.json",
        response);
  }

  @Test
  void queryWithNestedAggregation() {
    subjectResource = ResourceType.PATIENT;
    mockResourceReader(subjectResource);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("Number of patients", "name.given contains 'Frank'")
        .withGrouping("gender")
        .build();

    response = executor.execute(request);
    assertResponse("AggregateQueryTest/queryWithAmbiguousSelfJoin.Parameters.json",
        response);
  }

}