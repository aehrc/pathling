/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.datasource.ObjectDataSource;
import ca.uhn.fhir.context.FhirContext;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.AdministrativeGender;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.Patient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Tests for {@link ResourceFilterFactory}.
 */
@SpringBootUnitTest
@TestInstance(Lifecycle.PER_CLASS)
@Slf4j
class ResourceFilterFactoryTest {

  @Autowired
  SparkSession spark;

  @Autowired
  FhirEncoders encoders;

  private SearchParameterRegistry registry;
  private ResourceFilterFactory factory;
  private FhirContext fhirContext;

  @BeforeEach
  void setUp() {
    fhirContext = encoders.getContext();
    registry = new TestSearchParameterRegistry();
    factory = new ResourceFilterFactory(fhirContext, registry, new Parser());
  }

  // ========== Factory method tests ==========

  @Test
  void withDefaultRegistry_createsFactoryWithR4Registry() {
    final ResourceFilterFactory defaultFactory = ResourceFilterFactory.withDefaultRegistry(
        fhirContext);

    assertNotNull(defaultFactory);
    assertNotNull(defaultFactory.getRegistry());
  }

  // Note: Testing for non-R4 context would require DSTU3/R5 libraries on classpath
  // The version check is tested implicitly by the implementation

  // ========== fromQueryString tests ==========

  @Test
  void fromQueryString_createsFilterForGenderSearch() {
    final ResourceFilter filter = factory.fromQueryString(ResourceType.PATIENT, "gender=male");

    assertNotNull(filter);
    assertEquals(ResourceType.PATIENT, filter.getResourceType());
  }

  @Test
  void fromQueryString_emptyString_matchesAllResources() {
    final ObjectDataSource dataSource = createPatientDataSource();
    final Dataset<Row> dataset = dataSource.read("Patient");

    final ResourceFilter filter = factory.fromQueryString(ResourceType.PATIENT, "");
    final Dataset<Row> result = filter.apply(dataset);

    assertEquals(4, result.count());
  }

  @Test
  void fromQueryString_unknownParameter_throwsException() {
    assertThrows(UnknownSearchParameterException.class,
        () -> factory.fromQueryString(ResourceType.PATIENT, "unknown-param=value"));
  }

  @Test
  void fromQueryString_invalidModifier_throwsException() {
    assertThrows(InvalidModifierException.class,
        () -> factory.fromQueryString(ResourceType.PATIENT, "gender:exact=male"));
  }

  // ========== fromSearch tests ==========

  @Test
  void fromSearch_createsFilterFromFhirSearch() {
    final FhirSearch search = FhirSearch.builder()
        .criterion("gender", "male")
        .build();

    final ResourceFilter filter = factory.fromSearch(ResourceType.PATIENT, search);

    assertNotNull(filter);
    assertEquals(ResourceType.PATIENT, filter.getResourceType());
  }

  @Test
  void fromSearch_emptyCriteria_matchesAllResources() {
    final ObjectDataSource dataSource = createPatientDataSource();
    final Dataset<Row> dataset = dataSource.read("Patient");

    final FhirSearch search = FhirSearch.builder().build();
    final ResourceFilter filter = factory.fromSearch(ResourceType.PATIENT, search);
    final Dataset<Row> result = filter.apply(dataset);

    assertEquals(4, result.count());
  }

  // ========== fromExpression tests ==========

  @Test
  void fromExpression_createsFilterFromFhirPathExpression() {
    final ResourceFilter filter = factory.fromExpression(
        ResourceType.PATIENT, "gender = 'male'");

    assertNotNull(filter);
    assertEquals(ResourceType.PATIENT, filter.getResourceType());
  }

  @Test
  void fromExpression_appliesCorrectly() {
    final ObjectDataSource dataSource = createPatientDataSource();
    final Dataset<Row> dataset = dataSource.read("Patient");

    final ResourceFilter filter = factory.fromExpression(
        ResourceType.PATIENT, "gender = 'male'");
    final Dataset<Row> result = filter.apply(dataset);

    final Set<String> resultIds = extractIds(result);
    assertEquals(Set.of("1", "3"), resultIds);
  }

  @Test
  void fromExpression_withActiveField_appliesCorrectly() {
    final ObjectDataSource dataSource = createPatientDataSourceWithActive();
    final Dataset<Row> dataset = dataSource.read("Patient");

    final ResourceFilter filter = factory.fromExpression(
        ResourceType.PATIENT, "active = true");
    final Dataset<Row> result = filter.apply(dataset);

    final Set<String> resultIds = extractIds(result);
    assertEquals(Set.of("1", "3"), resultIds);
  }

  // ========== Filter composition tests ==========

  @Test
  void composedFilters_andLogic_worksCorrectly() {
    final ObjectDataSource dataSource = createPatientDataSourceWithActive();
    final Dataset<Row> dataset = dataSource.read("Patient");

    final ResourceFilter genderFilter = factory.fromQueryString(
        ResourceType.PATIENT, "gender=male");
    final ResourceFilter activeFilter = factory.fromExpression(
        ResourceType.PATIENT, "active = true");

    final ResourceFilter combined = genderFilter.and(activeFilter);
    final Dataset<Row> result = combined.apply(dataset);

    // Should match Patients 1 and 3: male AND active
    final Set<String> resultIds = extractIds(result);
    assertEquals(Set.of("1", "3"), resultIds);
  }

  @Test
  void composedFilters_orLogic_worksCorrectly() {
    final ObjectDataSource dataSource = createPatientDataSource();
    final Dataset<Row> dataset = dataSource.read("Patient");

    final ResourceFilter maleFilter = factory.fromQueryString(
        ResourceType.PATIENT, "gender=male");
    final ResourceFilter femaleFilter = factory.fromQueryString(
        ResourceType.PATIENT, "gender=female");

    final ResourceFilter combined = maleFilter.or(femaleFilter);
    final Dataset<Row> result = combined.apply(dataset);

    // Should match all patients with gender set
    final Set<String> resultIds = extractIds(result);
    assertEquals(Set.of("1", "2", "3"), resultIds);
  }

  // ========== Integration tests: Compare with FhirSearchExecutor ==========

  Stream<Arguments> equivalenceTestCases() {
    return Stream.of(
        // Simple gender search
        Arguments.of("gender=male", Set.of("1", "3")),
        Arguments.of("gender=female", Set.of("2")),
        // Multiple values (OR logic)
        Arguments.of("gender=male,female", Set.of("1", "2", "3")),
        // :not modifier
        Arguments.of("gender:not=male", Set.of("2", "4")),
        // Active search
        Arguments.of("active=true", Set.of("1", "3")),
        Arguments.of("active=false", Set.of("2"))
    );
  }

  @ParameterizedTest(name = "equivalence test: {0}")
  @MethodSource("equivalenceTestCases")
  void filterFactory_producesEquivalentResults_toFhirSearchExecutor(
      final String queryString,
      final Set<String> expectedIds) {

    final ObjectDataSource dataSource = createPatientDataSourceWithActive();
    final Dataset<Row> dataset = dataSource.read("Patient");

    // Execute using FhirSearchExecutor (existing implementation)
    final FhirSearchExecutor executor = FhirSearchExecutor.withRegistry(
        fhirContext, dataSource, registry);
    final FhirSearch search = FhirSearch.fromQueryString(queryString);
    final Dataset<Row> executorResult = executor.execute(ResourceType.PATIENT, search);
    final Set<String> executorIds = extractIds(executorResult);

    // Execute using ResourceFilterFactory (new implementation)
    final ResourceFilter filter = factory.fromQueryString(ResourceType.PATIENT, queryString);
    final Dataset<Row> filterResult = filter.apply(dataset);
    final Set<String> filterIds = extractIds(filterResult);

    // Verify both produce the same results
    assertAll(
        () -> assertEquals(expectedIds, executorIds,
            "FhirSearchExecutor should return expected IDs"),
        () -> assertEquals(expectedIds, filterIds,
            "ResourceFilterFactory should return expected IDs"),
        () -> assertEquals(executorIds, filterIds,
            "Both implementations should return identical results")
    );
  }

  @Test
  void filterFactory_schemaEquivalent_toFhirSearchExecutor() {
    final ObjectDataSource dataSource = createPatientDataSource();
    final Dataset<Row> dataset = dataSource.read("Patient");

    // Execute using FhirSearchExecutor
    final FhirSearchExecutor executor = FhirSearchExecutor.withRegistry(
        fhirContext, dataSource, registry);
    final FhirSearch search = FhirSearch.fromQueryString("gender=male");
    final Dataset<Row> executorResult = executor.execute(ResourceType.PATIENT, search);

    // Execute using ResourceFilterFactory
    final ResourceFilter filter = factory.fromQueryString(ResourceType.PATIENT, "gender=male");
    final Dataset<Row> filterResult = filter.apply(dataset);

    // Schemas should be identical
    assertEquals(executorResult.schema(), filterResult.schema());
  }

  // ========== withDefaultRegistry integration test ==========

  @Test
  void withDefaultRegistry_canFilterWithStandardParameters() {
    final ObjectDataSource dataSource = createPatientDataSource();
    final Dataset<Row> dataset = dataSource.read("Patient");

    final ResourceFilterFactory defaultFactory = ResourceFilterFactory.withDefaultRegistry(
        fhirContext);

    // Use a standard FHIR search parameter
    final ResourceFilter filter = defaultFactory.fromQueryString(
        ResourceType.PATIENT, "gender=male");
    final Dataset<Row> result = filter.apply(dataset);

    final Set<String> resultIds = extractIds(result);
    assertEquals(Set.of("1", "3"), resultIds);
  }

  // ========== Data source creation methods ==========

  private ObjectDataSource createPatientDataSource() {
    final Patient patient1 = new Patient();
    patient1.setId("1");
    patient1.setGender(AdministrativeGender.MALE);

    final Patient patient2 = new Patient();
    patient2.setId("2");
    patient2.setGender(AdministrativeGender.FEMALE);

    final Patient patient3 = new Patient();
    patient3.setId("3");
    patient3.setGender(AdministrativeGender.MALE);

    final Patient patient4 = new Patient();
    patient4.setId("4");
    // No gender

    return new ObjectDataSource(spark, encoders, List.of(patient1, patient2, patient3, patient4));
  }

  private ObjectDataSource createPatientDataSourceWithActive() {
    final Patient patient1 = new Patient();
    patient1.setId("1");
    patient1.setGender(AdministrativeGender.MALE);
    patient1.setActive(true);

    final Patient patient2 = new Patient();
    patient2.setId("2");
    patient2.setGender(AdministrativeGender.FEMALE);
    patient2.setActive(false);

    final Patient patient3 = new Patient();
    patient3.setId("3");
    patient3.setGender(AdministrativeGender.MALE);
    patient3.setActive(true);

    final Patient patient4 = new Patient();
    patient4.setId("4");
    // No gender or active

    return new ObjectDataSource(spark, encoders, List.of(patient1, patient2, patient3, patient4));
  }

  private Set<String> extractIds(final Dataset<Row> results) {
    return results.select("id").collectAsList().stream()
        .map(row -> row.getString(0))
        .collect(Collectors.toSet());
  }
}
