/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.aggregate;

import static au.csiro.pathling.test.helpers.TestHelpers.getResourceAsStream;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.test.TimingExtension;
import ca.uhn.fhir.rest.param.UriParam;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.CodeSystem;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.IntegerType;
import org.hl7.fhir.r4.model.ValueSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * @author John Grimes
 */
@Slf4j
@ExtendWith(TimingExtension.class)
class AggregateQueryTest extends AggregateExecutorTest {

  public AggregateQueryTest() {
    super();
  }

  @Test
  void simpleQuery() {
    subjectResource = ResourceType.PATIENT;
    mockResourceReader(subjectResource);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("count()")
        .withGrouping("gender")
        .build();

    response = executor.execute(request);
    assertResponse("AggregateQueryTest/simpleQuery.Parameters.json", response);
  }

  @Test
  void multipleGroupingsAndAggregations() {
    subjectResource = ResourceType.ENCOUNTER;
    mockResourceReader(subjectResource, ResourceType.ORGANIZATION);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("count()")
        .withAggregation("reasonCode.count()")
        .withGrouping("class.code")
        .withGrouping("reasonCode.coding.display")
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
        .withAggregation("count()")
        .withGrouping("item.sequence")
        .build();

    response = executor.execute(request);
    assertResponse("AggregateQueryTest/queryWithIntegerGroupings.Parameters.json",
        response);
  }

  @Test
  void queryWithMathExpression() {
    subjectResource = ResourceType.CLAIM;
    mockResourceReader(subjectResource);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("count()")
        .withGrouping("item.sequence.first() + 1")
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
        .withAggregation("count()")
        .withGrouping("multipleBirthBoolean")
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
        .withAggregation("count()")
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
        .withAggregation("count()")
        .withGrouping("patient.resolve().gender")
        .build();

    response = executor.execute(request);
    assertResponse("AggregateQueryTest/queryWithResolve.Parameters.json", response);
  }

  @Test
  void queryWithPolymorphicResolve() {
    subjectResource = ResourceType.DIAGNOSTICREPORT;
    mockResourceReader(subjectResource, ResourceType.PATIENT);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("count()")
        .withGrouping("subject.resolve().ofType(Patient).gender")
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
        .withAggregation("count()")
        .withGrouping("reverseResolve(Condition.subject).code.coding.display")
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
        .withAggregation("count()")
        .withGrouping("reverseResolve(Condition.subject).code.coding.count()")
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
        .withAggregation("count()")
        .withGrouping("name.given")
        .withGrouping("name.prefix")
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
        .withAggregation("name.given.count()")
        .withAggregation("name.prefix.count()")
        .withGrouping("gender")
        .build();

    response = executor.execute(request);
    assertResponse("AggregateQueryTest/queryMultipleCountAggregations.Parameters.json",
        response);
  }

  @Test
  void queryWithWhere() {
    subjectResource = ResourceType.PATIENT;
    mockResourceReader(ResourceType.CONDITION, subjectResource);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("count()")
        .withGrouping("reverseResolve(Condition.subject).where($this.onsetDateTime > @2010 and "
            + "$this.onsetDateTime < @2011).verificationStatus.coding.code")
        .build();

    response = executor.execute(request);
    assertResponse("AggregateQueryTest/queryWithWhere.Parameters.json", response);
  }

  @Test
  void queryWithMemberOf() {
    subjectResource = ResourceType.PATIENT;
    mockResourceReader(ResourceType.CONDITION, subjectResource);
    final Bundle mockSearch = (Bundle) jsonParser.parseResource(
        getResourceAsStream("txResponses/AggregateQueryTest/queryWithMemberOf.Bundle.json"));
    final List<CodeSystem> codeSystems = mockSearch.getEntry().stream()
        .map(entry -> (CodeSystem) entry.getResource())
        .collect(Collectors.toList());
    final ValueSet mockExpansion = (ValueSet) jsonParser.parseResource(
        getResourceAsStream("txResponses/AggregateQueryTest/queryWithMemberOf.ValueSet.json"));

    //noinspection unchecked
    when(terminologyClient.searchCodeSystems(any(UriParam.class), any(Set.class)))
        .thenReturn(codeSystems);
    when(terminologyClient.expand(any(ValueSet.class), any(IntegerType.class)))
        .thenReturn(mockExpansion);

    final String valueSetUrl = "http://snomed.info/sct?fhir_vs=refset/32570521000036109";
    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("count()")
        .withGrouping("reverseResolve(Condition.subject).code.memberOf('" + valueSetUrl + "')")
        .build();

    response = executor.execute(request);
    assertResponse("AggregateQueryTest/queryWithMemberOf.Parameters.json", response);
  }

  @Test
  void queryWithDateTimeGrouping() {
    subjectResource = ResourceType.MEDICATIONREQUEST;
    mockResourceReader(subjectResource);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("count()")
        .withGrouping("authoredOn")
        .build();

    response = executor.execute(request);
    assertResponse("AggregateQueryTest/queryWithDateTimeGrouping.Parameters.json",
        response);
  }

  @Test
  void queryWithWhereAsComparisonOperand() {
    subjectResource = ResourceType.PATIENT;
    mockResourceReader(subjectResource, ResourceType.MEDICATIONREQUEST);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("count()")
        .withGrouping("reverseResolve(MedicationRequest.subject).where("
            + "$this.medicationCodeableConcept.coding contains "
            + "http://www.nlm.nih.gov/research/umls/rxnorm|313782 "
            + "and $this.authoredOn < @2019-06-21).count() > 0")
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
        .withAggregation("count()")
        .withGrouping("status")
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
        .withAggregation("count()")
        .withGrouping(
            "reverseResolve(Observation.subject).where($this.code.coding contains http://loinc.org|8302-2).status")
        .build();

    response = executor.execute(request);
    assertResponse("AggregateQueryTest/queryWithWhereAndMembership.Parameters.json",
        response);
  }

  @Test
  void queryWithWhereAndBoolean() {
    subjectResource = ResourceType.PATIENT;
    mockResourceReader(subjectResource, ResourceType.OBSERVATION);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("count()")
        .withGrouping(
            "where($this.gender = 'male' and $this.birthDate > @1990).communication.language.text")
        .build();

    response = executor.execute(request);
    assertResponse("AggregateQueryTest/queryWithWhereAndBoolean.Parameters.json",
        response);
  }

  @Test
  void queryWithWhereInAggregation() {
    subjectResource = ResourceType.PATIENT;
    mockResourceReader(subjectResource, ResourceType.OBSERVATION);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("where($this.gender = 'female').count()")
        .withGrouping("gender")
        .withGrouping("maritalStatus.coding")
        .build();

    response = executor.execute(request);
    assertResponse("AggregateQueryTest/queryWithWhereInAggregation.Parameters.json",
        response);
  }

  @Test
  void queryWithNestedAggregation() {
    subjectResource = ResourceType.PATIENT;
    mockResourceReader(subjectResource);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("name.given.count() + name.family.count()")
        .withGrouping("gender")
        .build();

    response = executor.execute(request);
    assertResponse("AggregateQueryTest/queryWithNestedAggregation.Parameters.json",
        response);
  }

  @Test
  void queryWithNestedAggregationAndNoGroupings() {
    subjectResource = ResourceType.PATIENT;
    mockResourceReader(subjectResource);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("name.given.count() + name.family.count()")
        .build();

    response = executor.execute(request);
    assertResponse("AggregateQueryTest/queryWithNestedAggregationAndNoGroupings.Parameters.json",
        response);
  }

  @Test
  void queryWithUriValueInGrouping() {
    subjectResource = ResourceType.ENCOUNTER;
    mockResourceReader(subjectResource, ResourceType.CONDITION);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("count()")
        .withGrouping("reverseResolve(Condition.encounter).code.coding.system")
        .build();

    response = executor.execute(request);
    assertResponse("AggregateQueryTest/queryWithUriValueInGrouping.Parameters.json",
        response);
  }

  /**
   * @see <a href="https://github.com/aehrc/pathling/issues/151">#151</a>
   */
  @Test
  void queryWithComparisonInAggregation() {
    subjectResource = ResourceType.CAREPLAN;
    mockResourceReader(subjectResource);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("count() = 12")
        .withGrouping("status")
        .build();

    response = executor.execute(request);
    assertResponse("AggregateQueryTest/queryWithComparisonInAggregation.Parameters.json",
        response);
  }

  /**
   * @see <a href="https://github.com/aehrc/pathling/issues/151">#151</a>
   */
  @Test
  void queryWithLiteralAggregation() {
    subjectResource = ResourceType.CAREPLAN;
    mockResourceReader(subjectResource);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("true")
        .withGrouping("status")
        .build();

    response = executor.execute(request);
    assertResponse("AggregateQueryTest/queryWithLiteralAggregation.Parameters.json",
        response);
  }

  @Test
  void queryWithWhereAndGroupedData() {
    subjectResource = ResourceType.CAREPLAN;
    mockResourceReader(subjectResource);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("count().where($this >= 12 and $this <= 13)")
        .withGrouping("status")
        .build();

    response = executor.execute(request);
    assertResponse("AggregateQueryTest/queryWithWhereAndGroupedData.Parameters.json",
        response);
  }

  @Test
  void queryWithMultipleGroupingsAndMembership() {
    subjectResource = ResourceType.PATIENT;
    mockResourceReader(subjectResource);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("count()")
        .withGrouping("name.prefix contains 'Mrs.'")
        .withGrouping("name.given contains 'Karina848'")
        .build();

    response = executor.execute(request);
    assertResponse("AggregateQueryTest/queryWithMultipleGroupingsAndMembership.Parameters.json",
        response);
  }

  @Test
  void queryWithNonSingularWhereFollowedByCount() {
    subjectResource = ResourceType.PATIENT;
    mockResourceReader(subjectResource);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("name.where($this.given contains 'Karina848').count()")
        .withFilter("id = '9360820c-8602-4335-8b50-c88d627a0c20'")
        .build();

    response = executor.execute(request);
    assertResponse("AggregateQueryTest/queryWithNonSingularWhereFollowedByCount.Parameters.json",
        response);
  }

  @Test
  void throwsInvalidInputOnEmptyAggregation() {
    subjectResource = ResourceType.PATIENT;

    final InvalidUserInputError error = assertThrows(InvalidUserInputError.class,
        () -> new AggregateRequestBuilder(subjectResource)
            .withAggregation("")
            .build());
    assertEquals("Aggregation expression cannot be blank", error.getMessage());
  }

  @Test
  void throwsInvalidInputOnEmptyGrouping() {
    subjectResource = ResourceType.PATIENT;

    final InvalidUserInputError error = assertThrows(InvalidUserInputError.class,
        () -> new AggregateRequestBuilder(subjectResource)
            .withAggregation("count()")
            .withGrouping("")
            .build());
    assertEquals("Grouping expression cannot be blank", error.getMessage());
  }

  @Test
  void throwsInvalidInputOnEmptyFilter() {
    subjectResource = ResourceType.PATIENT;

    final InvalidUserInputError error = assertThrows(InvalidUserInputError.class,
        () -> new AggregateRequestBuilder(subjectResource)
            .withAggregation("count()")
            .withFilter("")
            .build());
    assertEquals("Filter expression cannot be blank", error.getMessage());
  }

  @Test
  void throwsInvalidInputOnMissingAggregation() {
    subjectResource = ResourceType.PATIENT;

    final InvalidUserInputError error = assertThrows(InvalidUserInputError.class,
        () -> new AggregateRequestBuilder(subjectResource).build());
    assertEquals("Query must have at least one aggregation expression", error.getMessage());
  }

}