/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.aggregate;

import static au.csiro.pathling.test.TestResources.getResourceAsStream;
import static au.csiro.pathling.test.helpers.TerminologyHelpers.setOfSimpleFrom;
import static au.csiro.pathling.test.helpers.TerminologyHelpers.snomedSimple;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.encoding.SimpleCoding;
import au.csiro.pathling.test.TimingExtension;
import au.csiro.pathling.test.fixtures.RelationBuilder;
import au.csiro.pathling.test.helpers.TerminologyHelpers;
import com.google.common.collect.ImmutableSet;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.ValueSet;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;

/**
 * @author John Grimes
 */
@Slf4j
@ExtendWith(TimingExtension.class)
@Tag("Tranche1")
class AggregateQueryTest extends AggregateExecutorTest {

  AggregateQueryTest() {
    super();
  }

  @Test
  void simpleQuery() {
    subjectResource = ResourceType.PATIENT;
    mockResource(subjectResource);

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
    mockResource(subjectResource, ResourceType.ORGANIZATION);

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
  void multipleAggregations() {
    subjectResource = ResourceType.PATIENT;
    mockResource(subjectResource, ResourceType.CONDITION);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("reverseResolve(Condition.subject).count()")
        .withAggregation("count()")
        .build();

    response = executor.execute(request);
    assertResponse(
        "AggregateQueryTest/multipleAggregations.Parameters.json",
        response);
  }

  @Test
  void queryWithIntegerGroupings() {
    subjectResource = ResourceType.CLAIM;
    mockResource(subjectResource);

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
    mockResource(subjectResource);

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
    mockResource(subjectResource);

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
    mockResource(subjectResource);

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
    mockResource(subjectResource, ResourceType.PATIENT);

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
    mockResource(subjectResource, ResourceType.PATIENT);
    mockEmptyResource(ResourceType.GROUP, ResourceType.DEVICE, ResourceType.LOCATION);

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
    mockResource(ResourceType.CONDITION, subjectResource);

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
    mockResource(ResourceType.CONDITION, subjectResource);

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
    mockResource(ResourceType.CONDITION, subjectResource);

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
    mockResource(ResourceType.CONDITION, subjectResource);

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
    mockResource(ResourceType.CONDITION, subjectResource);

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
    mockResource(ResourceType.CONDITION, subjectResource);

    final ValueSet mockExpansion = (ValueSet) jsonParser.parseResource(
        getResourceAsStream("txResponses/AggregateQueryTest/queryWithMemberOf.ValueSet.json"));
    when(terminologyService.intersect(any(), any()))
        .thenReturn(setOfSimpleFrom(mockExpansion));

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
    mockResource(subjectResource);

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
    mockResource(subjectResource, ResourceType.MEDICATIONREQUEST);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("count()")
        .withGrouping("reverseResolve(MedicationRequest.subject).where("
            + "$this.medicationCodeableConcept.coding" + ""
            + ".where(system = 'http://www.nlm.nih.gov/research/umls/rxnorm').code contains '313782' "
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
    mockResource(subjectResource);

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
    mockResource(subjectResource, ResourceType.OBSERVATION);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("count()")
        .withGrouping(
            "reverseResolve(Observation.subject).where($this.code.coding.code contains '8302-2').status")
        .build();

    response = executor.execute(request);
    assertResponse("AggregateQueryTest/queryWithWhereAndMembership.Parameters.json",
        response);
  }

  @Test
  void queryWithWhereAndBoolean() {
    subjectResource = ResourceType.PATIENT;
    mockResource(subjectResource, ResourceType.OBSERVATION);

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
    mockResource(subjectResource, ResourceType.OBSERVATION);

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
    mockResource(subjectResource);

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
    mockResource(subjectResource);

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
    mockResource(subjectResource, ResourceType.CONDITION);

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
    mockResource(subjectResource);

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
    mockResource(subjectResource);

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
    mockResource(subjectResource);

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
    mockResource(subjectResource);

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
    mockResource(subjectResource);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("name.where($this.given contains 'Karina848').count()")
        .withFilter("id = '9360820c-8602-4335-8b50-c88d627a0c20'")
        .build();

    response = executor.execute(request);
    assertResponse("AggregateQueryTest/queryWithNonSingularWhereFollowedByCount.Parameters.json",
        response);
  }


  @Test
  void queryWithNonSingularBooleanGrouping() {
    subjectResource = ResourceType.PATIENT;
    mockResource(ResourceType.CONDITION, subjectResource);
    // Not a real subsumption - just works for this use case.
    // http://snomed.info/sct|284551006 -- subsumes --> http://snomed.info/sct|40055000
    when(terminologyService.getSubsumesRelation(any()))
        .thenReturn(RelationBuilder.empty().add(TerminologyHelpers.CD_SNOMED_VER_284551006,
            TerminologyHelpers.CD_SNOMED_VER_40055000).build());

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("count()")
        .withGrouping(
            "reverseResolve(Condition.subject).code.subsumedBy(http://snomed.info/sct|284551006)")
        .build();

    response = executor.execute(request);
    assertResponse("AggregateQueryTest/queryWithNonSingularBooleanGrouping.Parameters.json",
        response);
  }

  @Test
  void queryWithBracketing() {
    subjectResource = ResourceType.PATIENT;
    mockResource(subjectResource);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("count()")
        .withGrouping("(1 + 3) * 4")
        .build();

    response = executor.execute(request);
    assertResponse("AggregateQueryTest/queryWithBracketing.Parameters.json", response);
  }

  @Test
  void queryWithCanonicalGrouping() {
    subjectResource = ResourceType.QUESTIONNAIRERESPONSE;
    mockResource(subjectResource);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("count()")
        .withGrouping("questionnaire")
        .build();

    response = executor.execute(request);
    assertResponse("AggregateQueryTest/queryWithCanonicalGrouping.Parameters.json", response);
  }

  @Test
  void queryWithLargeScaleDecimalResult() {
    subjectResource = ResourceType.PATIENT;
    mockResource(subjectResource);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("where(gender = 'female').count() / where(gender = 'male').count()")
        .build();

    response = executor.execute(request);
    assertResponse("AggregateQueryTest/queryWithLargeScaleDecimalResult.Parameters.json", response);
  }

  @Test
  void queryWithWhereAggregationAndCount() {
    subjectResource = ResourceType.CONDITION;
    mockResource(subjectResource);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation(
            "where(code.coding contains http://snomed.info/sct|444814009||'Viral sinusitis (disorder)').count()")
        .build();

    response = executor.execute(request);
    assertResponse("AggregateQueryTest/queryWithWhereAggregationAndCount.Parameters.json",
        response);
  }

  @Test
  void queryWithCombineResultInSecondFilter() {
    subjectResource = ResourceType.PATIENT;
    mockResource(subjectResource);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("count()")
        .withFilter("gender = 'male'")
        .withFilter("(name.given combine name.family).empty().not()")
        .build();

    response = executor.execute(request);
    assertResponse("AggregateQueryTest/queryWithCombineResultInSecondFilter.Parameters.json",
        response);
  }

  @Test
  void queryWithMultipleTrivialAggregations() {
    subjectResource = ResourceType.OBSERVATION;
    mockResource(subjectResource);

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("true")
        .withAggregation("true")
        .build();

    response = executor.execute(request);
    assertResponse("AggregateQueryTest/queryWithMultipleTrivialAggregations.Parameters.json",
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


  @Test
  void queryWithTerminologyAndFilterInWhere() {
    subjectResource = ResourceType.CONDITION;
    mockResource(ResourceType.CONDITION, subjectResource);

    final ValueSet mockExpansion = (ValueSet) jsonParser.parseResource(
        getResourceAsStream("txResponses/AggregateQueryTest/queryWithMemberOf.ValueSet.json"));
    when(terminologyService.intersect(any(), any()))
        .thenReturn(setOfSimpleFrom(mockExpansion));

    final String valueSetUrl = "http://snomed.info/sct?fhir_vs=refset/32570521000036109";
    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("count()")
        .withGrouping("where($this.onsetString = 'test').code.memberOf('" + valueSetUrl + "')")
        .build();

    AggregateExecutor.ResultWithExpressions query = executor.buildQuery(request);
    query.getDataset().explain(true);
  }


  @Test
  void queryWithTerminologyAndFilter() {

    subjectResource = ResourceType.CONDITION;
    mockResource(ResourceType.CONDITION, subjectResource);

    final String valueSetUrl = "http://snomed.info/sct?fhir_vs=refset/32570521000036109";

    // We expect only the codings for 'Acute bronchitis (disorder)' and 'Laceration of foot'
    // to be passed to the terminology server. Conditions with 'Prediabetes' have clinicalStatus = 'active'
    // so should be filtered out as well as any other codings not explicitly included in the filter. 
    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("count()")
        .withAggregation("id.count()")
        .withGrouping("code.memberOf('" + valueSetUrl + "')")
        .withGrouping("verificationStatus.coding.code")
        .withFilter(
            "code.text='Acute bronchitis (disorder)' or code.text='Laceration of foot' or code.text='Prediabetes'")
        .withFilter("clinicalStatus.coding.code contains 'resolved'")
        .build();

    final ImmutableSet<SimpleCoding> filteredCodings = ImmutableSet.of(
        snomedSimple("284551006"), // Laceration of foot
        snomedSimple("10509002") // Acute bronchitis (disorder)
    );

    final ValueSet mockExpansion = (ValueSet) jsonParser.parseResource(
        getResourceAsStream("txResponses/AggregateQueryTest/queryWithMemberOf.ValueSet.json"));
    when(terminologyService.intersect(valueSetUrl, filteredCodings))
        .thenReturn(setOfSimpleFrom(mockExpansion));

    //executor.buildQuery(request).getDataset().explain(true);
    
    response = executor.execute(request);

    // We check here that we had exactly one invocation ot the terminology function with the expected
    // arguments (e.g. filtered codings). But that also shows that Spark is able to reuse (implicitly)
    // the filtered and grouped dataset between multiple aggregations.
    verify(terminologyService, Mockito.times(1)).intersect(valueSetUrl, filteredCodings);
    verifyNoMoreInteractions(terminologyService);

    assertResponse("AggregateQueryTest/queryWithTerminologyAndFilter.Parameters.json",
        response);

    // Setup the mock for the drill-down test (run after each)
    when(terminologyService.intersect(any(), any()))
        .thenReturn(setOfSimpleFrom(mockExpansion));
  }

}
