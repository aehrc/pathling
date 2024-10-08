/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package au.csiro.pathling.search;

import static au.csiro.pathling.test.TestResources.getResourceAsStream;
import static au.csiro.pathling.test.TestResources.getResourceAsString;
import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import au.csiro.pathling.config.QueryConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import au.csiro.pathling.test.SharedMocks;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.helpers.TerminologyServiceHelpers;
import au.csiro.pathling.test.helpers.TestHelpers;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import ca.uhn.fhir.rest.api.server.IBundleProvider;
import ca.uhn.fhir.rest.param.StringAndListParam;
import ca.uhn.fhir.rest.param.StringParam;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.hl7.fhir.r4.model.Bundle.BundleType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.Resource;
import org.hl7.fhir.r4.model.ValueSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author John Grimes
 */
@SpringBootUnitTest
class SearchExecutorTest {

  @Autowired
  QueryConfiguration configuration;

  @Autowired
  FhirContext fhirContext;

  @Autowired
  IParser jsonParser;

  @Autowired
  SparkSession sparkSession;

  @Autowired
  FhirEncoders fhirEncoders;


  @Autowired
  TerminologyServiceFactory terminologyServiceFactory;

  @Autowired
  TerminologyService terminologyService;

  @BeforeEach
  void setUp() {
    SharedMocks.resetAll();
  }

  @Test
  void simpleSearchWithMemberOf() {
    final StringAndListParam params = new StringAndListParam();
    params.addAnd(new StringParam(
        "reverseResolve(Condition.subject).code.memberOf('http://snomed.info/sct?fhir_vs=ecl/^ 32570581000036105 : << 263502005 = << 90734009')"));
    final SearchExecutorBuilder builder = searchBuilder()
        .withSubjectResource(ResourceType.PATIENT)
        .withFilters(params);
    TestHelpers.mockResource(builder.getDatabase(), sparkSession, ResourceType.CONDITION);

    final ValueSet valueSet = (ValueSet) jsonParser.parseResource(getResourceAsStream(
        "txResponses/SearchExecutorTest/simpleSearchWithMemberOf.ValueSet.json"));

    TerminologyServiceHelpers.setupValidate(terminologyService)
        .fromValueSet(
            "http://snomed.info/sct?fhir_vs=ecl/^ 32570581000036105 : << 263502005 = << 90734009",
            valueSet);

    final SearchExecutor executor = builder.build();
    assertResponse("SearchExecutorTest/simpleSearchWithMemberOf.Bundle.json", executor);
  }

  @Test
  void searchOfQuestionnaire() {
    final SearchExecutorBuilder builder = searchBuilder()
        .withSubjectResource(ResourceType.QUESTIONNAIRE);
    TestHelpers.mockResource(builder.getDatabase(), sparkSession, ResourceType.QUESTIONNAIRE);

    final SearchExecutor executor = builder.build();
    assertResponse("SearchExecutorTest/searchOfQuestionnaire.Bundle.json", executor);
  }

  @Test
  void combineResultInSecondFilter() {
    final StringAndListParam params = new StringAndListParam();
    params.addAnd(new StringParam("gender = 'male'"));
    params.addAnd(new StringParam("(name.given combine name.family).empty().not()"));
    final SearchExecutorBuilder builder = searchBuilder()
        .withSubjectResource(ResourceType.PATIENT)
        .withFilters(params);
    TestHelpers.mockResource(builder.getDatabase(), sparkSession, ResourceType.PATIENT);

    final SearchExecutor executor = builder.build();
    assertResponse("SearchExecutorTest/combineResultInSecondFilter.Bundle.json", executor);
  }

  @Test
  void throwsInvalidInputOnNonBooleanFilter() {
    final StringAndListParam params = new StringAndListParam();
    params.addAnd(new StringParam("category.coding"));

    final InvalidUserInputError error = assertThrows(InvalidUserInputError.class,
        () -> searchBuilder()
            .withSubjectResource(ResourceType.CAREPLAN)
            .withFilters(params)
            .build());
    assertEquals("Filter expression must be of Boolean type: category.coding", error.getMessage());
  }

  @Test
  void throwsInvalidInputOnEmptyFilter() {
    final StringAndListParam params = new StringAndListParam();
    params.addAnd(new StringParam(""));

    final InvalidUserInputError error = assertThrows(InvalidUserInputError.class,
        () -> searchBuilder()
            .withSubjectResource(ResourceType.CAREPLAN)
            .withFilters(params)
            .build());
    assertEquals("Filter expression cannot be blank", error.getMessage());
  }

  @Nonnull
  SearchExecutorBuilder searchBuilder() {
    return new SearchExecutorBuilder(configuration, fhirContext, sparkSession,
        fhirEncoders, terminologyServiceFactory);
  }

  @SuppressWarnings("SameParameterValue")
  void assertResponse(@Nonnull final String expectedPath,
      @Nonnull final IBundleProvider executor) {

    final String expectedJson = getResourceAsString("responses/" + expectedPath);
    final Bundle expectedBundle = (Bundle) jsonParser.parseResource(expectedJson);
    assertEquals(expectedBundle.getTotal(), executor.size());

    final List<IBaseResource> actualResources = executor.getResources(0, expectedBundle.getTotal());
    final Bundle actualBundle = new Bundle();
    actualBundle.setEntry(actualResources.stream().map(resource -> {
      final BundleEntryComponent entry = new BundleEntryComponent();
      entry.setResource((Resource) resource);
      return entry;
    }).collect(Collectors.toList()));
    actualBundle.setTotal(requireNonNull(executor.size()));
    actualBundle.setType(BundleType.SEARCHSET);
    final String actualJson = jsonParser.encodeResourceToString(actualBundle);

    JSONAssert.assertEquals(expectedJson, actualJson, JSONCompareMode.LENIENT);
  }
}
