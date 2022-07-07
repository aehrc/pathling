/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.aggregate;

import static au.csiro.pathling.aggregate.AggreateResponseBuilder.agg;
import static au.csiro.pathling.aggregate.AggreateResponseBuilder.get;

import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;


/**
 * @author Piotr Szul
 */
@SpringBootTest
@Tag("UnitTest")
class AggregationTest extends AggregateExecutorTest {

  AggregationTest() {
    super();
  }


  private void assertResult(final AggregateRequest request, AggregateResponse expectedResponse) {
    response = executor.execute(request);
    assertResponse(expectedResponse, response);
  }

  @BeforeEach
  void setupResource() {
    subjectResource = ResourceType.RELATEDPERSON;
    mockResource(subjectResource);

  }

  @Test
  void resourceAggregation() {

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("count()")
        .withAggregation("gender.count()")
        .withAggregation("name.count()")
        .withAggregation("name.family.count()")
        .withAggregation("name.given.count()")
        .build();

    final AggregateResponse expectedResponse = agg()
        .withValues(3, 2, 5, 5, 8).build();

    assertResult(request, expectedResponse);
  }

  @Test
  void resourceAggregationWithConstraint() {

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("name.where(given contains 'Ann').count()")
        .withAggregation("where(gender='female').name.given.count()")
        .build();

    final AggregateResponse expectedResponse = agg()
        .withValues(2, 4).build();
    assertResult(request, expectedResponse);
  }

  @Test
  void simpleFilterOnResource() {

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("count()")
        .withAggregation("name.count()")
        .withFilter("gender='female'")
        .build();

    final AggregateResponse expectedResponse = agg().withValues(1, 2)
        .withDrillDown("gender = 'female'").build();
    assertResult(request, expectedResponse);
  }

  @Test
  void joinAggAndFilterResource() {

    // TODO: how should that work  ???

    // "where(name.family contains 'Brown).name.count()"
    // "name.where(family contains 'Brown).count()"

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("name.count()")
        .withFilter("name.family contains 'Brown'")
        .build();

    final AggregateResponse expectedResponse = agg().withValues(1)
        .withDrillDown("name.family contains 'Brown'").build();
    assertResult(request, expectedResponse);
  }

  @Test
  void andFilterOnResource() {

    // TODO: It's failing becasue filters are not applied contextually
    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("count()")
        .withFilter("name.family contains 'Bond'")
        .withFilter("name.given contains 'Craig'")
        .build();

    final AggregateResponse expectedResponse = agg().withValues(0).withDrillDown(
        "(name.family contains 'Bond') %and% (name.given contains 'Craig')").build();

    assertResult(request, expectedResponse);
  }


  @Test
  void explicitAndFilterOnResource() {
    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation(
            "where(name.where((family contains 'Bond') and (given contains 'Craig')).empty().not()).count()")
        .withAggregation(
            "where(name.where((family contains 'Bond') and (given contains 'James')).empty().not()).count()")
        .build();
    final AggregateResponse expectedResponse = agg().withValues(0, 1).build();
    assertResult(request, expectedResponse);
  }


  @Test
  void complexAggregationExpressions() {

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation(
            "where(gender='female').name.count() / name.count()")
        .build();

    final AggregateResponse expectedResponse = agg()
        .withValues(0.4000).build();
    assertResult(request, expectedResponse);
  }


  @Test
  void basicGroupingExpressions() {

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("count()")
        .withGrouping("gender")
        .build();

    final AggregateResponse expectedResponse = AggreateResponseBuilder.get()
        .newGroup(new CodeType("male"))
        .withValues(1).withDrillDown("(gender) = 'male'")
        .newGroup(new CodeType("female"))
        .withValues(1).withDrillDown("(gender) = 'female'")
        .newEmptyGroup()
        .withValues(1).withDrillDown("(gender).empty()")
        .build();

    assertResult(request, expectedResponse);
  }

  @Test
  void basicGroupingWithFilterExpressions() {

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("count()")
        .withGrouping("gender")
        .withFilter("gender.empty().not()")
        .build();

    final AggregateResponse expectedResponse = AggreateResponseBuilder.get()
        .newGroup(new CodeType("male"))
        .withValues(1)
        .withDrillDown("((gender) = 'male') and (gender.empty().not())")
        .newGroup(new CodeType("female"))
        .withValues(1)
        .withDrillDown("((gender) = 'female') and (gender.empty().not())")
        .build();
    assertResult(request, expectedResponse);
  }


  @Test
  void basicGroupingWithConstraint() {
    subjectResource = ResourceType.RELATEDPERSON;
    mockResource(subjectResource);

    // TODO: How should that work ???
    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("count()")
        .withGrouping("gender.where($this.empty().not())")
        .build();

    final AggregateResponse expectedResponse = AggreateResponseBuilder.get()
        .newGroup(new CodeType("male"))
        .withValues(1)
        .withDrillDown("((gender) = 'male') and (gender.empty().not())")
        .newGroup(new CodeType("female"))
        .withValues(1)
        .withDrillDown("((gender) = 'female') and (gender.empty().not())")
        .build();
    assertResult(request, expectedResponse);
  }


  @Test
  void groupingOnMultivalue() {

    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("name.count()")
        .withGrouping("name.given")
        .withFilter("gender = 'female'")
        .build();

    final AggregateResponse expectedResponse = get()
        .newGroup("Marie").withValues(2)
        .withDrillDown("((name.given) contains 'Marie') and (gender = 'female')")
        .newGroup("Ann").withValues(2)
        .withDrillDown("((name.given) contains 'Ann') and (gender = 'female')")
        .build();
    assertResult(request, expectedResponse);
  }


  @Test
  void andGroupingOnMultivalues() {
    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("count()")
        .withAggregation("name.count()")
        .withGrouping("name.given")
        .withGrouping("name.family")
        .withFilter("gender.empty()")
        .build();

    final AggregateResponse expectedResponse = get()
        .newGroup("Craig", "Jones").withValues(1, 1).withDrillDown(
            "((name.given) contains 'Craig') and ((name.family) contains 'Jones') and (gender.empty())")
        .newGroup("James", "Bond").withValues(1, 1).withDrillDown(
            "((name.given) contains 'James') and ((name.family) contains 'Bond') and (gender.empty())")
        .build();
    assertResult(request, expectedResponse);
  }


  @Test
  void simluateSusceptabilityWithConstainedGrouping() {
    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("count()")
        .withGrouping("gender")
        .withGrouping("name.where(use = 'maiden').family")
        .build();

    final AggregateResponse expectedResponse = get()
        .newGroup(new CodeType("female"), "White").withValues(1).withDrillDown(
            "((gender) = 'female') and ((name.where($this.use = 'maiden').family) contains 'White')")
        .newGroup(new CodeType("male"), null).withValues(1).withDrillDown(
            "((gender) = 'male') and ((name.where($this.use = 'maiden').family).empty())")
        .newGroup(null, null).withValues(1).withDrillDown(
            "((gender).empty()) and ((name.where($this.use = 'maiden').family).empty())"
        )
        .build();
    assertResult(request, expectedResponse);
  }

  @Test
  void simluateSusceptabilityWithExtraGroupingWithValue() {
    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("count()")
        .withGrouping("gender")
        .withGrouping("name.family")
        .withGrouping("name.use")
        .withFilter(
            "gender='female'") // add filter to simplify narrrow down the results only to the 
        // interesting case
        .build();

    final AggregateResponse expectedResponse = get()
        .newGroup(new CodeType("female"), "White", new CodeType("maiden")).withValues(1)
        .withDrillDown(
            "((gender) = 'female') and ((name.family) contains 'White') and ((name.use) contains 'maiden') and (gender = 'female')")
        .newGroup(new CodeType("female"), "Brown", new CodeType("official")).withValues(1)
        .withDrillDown(
            "((gender) = 'female') and ((name.family) contains 'Brown') and ((name.use) contains 'official') and (gender = 'female')")
        .build();
    assertResult(request, expectedResponse);
  }


  @Test
  void simluateSusceptabilityWithExtraGroupingWithCondition() {
    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("count()")
        .withGrouping("gender")
        .withGrouping("name.family")
        .withGrouping("name.use contains 'maiden'")
        .withFilter(
            "gender='female'") // add filter to simplify narrrow down the results only to the 
        // interesting case
        .build();

    final AggregateResponse expectedResponse = get()
        .newGroup(new CodeType("female"), "White", true).withValues(1)
        .withDrillDown(
            "((gender) = 'female') and ((name.family) contains 'White') and (name.use contains 'maiden') and (gender = 'female')")
        .newGroup(new CodeType("female"), "Brown", false).withValues(1)
        .withDrillDown(
            "((gender) = 'female') and ((name.family) contains 'Brown') and (name.use contains 'maiden') and (gender = 'female')")
        .build();
    assertResult(request, expectedResponse);
  }

  @Test
  void simluateSusceptabilityWithContextualAggregation() {
    final AggregateRequest request = new AggregateRequestBuilder(subjectResource)
        .withAggregation("name.where(use contains 'maiden').count()")
        .withAggregation("name.count()")
        .withGrouping("gender")
        .withGrouping("name.family")
        .withFilter(
            "gender='female'") // add filter to simplify narrrow down the results only to the 
        // interesting case
        .build();

    final AggregateResponse expectedResponse = get()
        .newGroup(new CodeType("female"), "Brown").withValues(0, 1).withDrillDown(
            "((gender) = 'female') and ((name.family) contains 'Brown') and (gender = 'female')")
        .newGroup(new CodeType("female"), "White").withValues(1, 1).withDrillDown(
            "((gender) = 'female') and ((name.family) contains 'White') and (gender = 'female')")
        .build();
    assertResult(request, expectedResponse);
  }

}
