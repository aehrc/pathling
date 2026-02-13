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
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.AdministrativeGender;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Reference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;

/** Tests for {@link SearchColumnBuilder}. */
@SpringBootUnitTest
@TestInstance(Lifecycle.PER_CLASS)
@Slf4j
class SearchColumnBuilderTest {

  @Autowired SparkSession spark;

  @Autowired FhirEncoders encoders;

  private SearchParameterRegistry registry;
  private SearchColumnBuilder builder;
  private FhirContext fhirContext;

  @BeforeEach
  void setUp() {
    fhirContext = encoders.getContext();
    registry = new TestSearchParameterRegistry();
    builder = new SearchColumnBuilder(fhirContext, registry, new Parser());
  }

  // ========== Factory method tests ==========

  @Test
  void withDefaultRegistry_createsBuilderWithR4Registry() {
    final SearchColumnBuilder defaultBuilder = SearchColumnBuilder.withDefaultRegistry(fhirContext);

    assertNotNull(defaultBuilder);
    assertNotNull(defaultBuilder.getRegistry());
  }

  // Note: Testing for non-R4 context would require DSTU3/R5 libraries on classpath
  // The version check is tested implicitly by the implementation

  // ========== fromQueryString tests ==========

  @Test
  void fromQueryString_createsColumnForGenderSearch() {
    final Column filterColumn = builder.fromQueryString(ResourceType.PATIENT, "gender=male");

    assertNotNull(filterColumn);
  }

  @Test
  void fromQueryString_emptyString_matchesAllResources() {
    final ObjectDataSource dataSource = createPatientDataSource();
    final Dataset<Row> dataset = dataSource.read("Patient");

    final Column filterColumn = builder.fromQueryString(ResourceType.PATIENT, "");
    final Dataset<Row> result = dataset.filter(filterColumn);

    assertEquals(4, result.count());
  }

  @Test
  void fromQueryString_unknownParameter_throwsException() {
    assertThrows(
        UnknownSearchParameterException.class,
        () -> builder.fromQueryString(ResourceType.PATIENT, "unknown-param=value"));
  }

  @Test
  void fromQueryString_invalidModifier_throwsException() {
    assertThrows(
        InvalidModifierException.class,
        () -> builder.fromQueryString(ResourceType.PATIENT, "gender:exact=male"));
  }

  // ========== fromSearch tests ==========

  @Test
  void fromSearch_createsColumnFromFhirSearch() {
    final FhirSearch search = FhirSearch.builder().criterion("gender", "male").build();

    final Column filterColumn = builder.fromSearch(ResourceType.PATIENT, search);

    assertNotNull(filterColumn);
  }

  @Test
  void fromSearch_emptyCriteria_matchesAllResources() {
    final ObjectDataSource dataSource = createPatientDataSource();
    final Dataset<Row> dataset = dataSource.read("Patient");

    final FhirSearch search = FhirSearch.builder().build();
    final Column filterColumn = builder.fromSearch(ResourceType.PATIENT, search);
    final Dataset<Row> result = dataset.filter(filterColumn);

    assertEquals(4, result.count());
  }

  // ========== fromExpression tests ==========

  @Test
  void fromExpression_createsColumnFromFhirPathExpression() {
    final Column filterColumn = builder.fromExpression(ResourceType.PATIENT, "gender = 'male'");

    assertNotNull(filterColumn);
  }

  @Test
  void fromExpression_appliesCorrectly() {
    final ObjectDataSource dataSource = createPatientDataSource();
    final Dataset<Row> dataset = dataSource.read("Patient");

    final Column filterColumn = builder.fromExpression(ResourceType.PATIENT, "gender = 'male'");
    final Dataset<Row> result = dataset.filter(filterColumn);

    final Set<String> resultIds = extractIds(result);
    assertEquals(Set.of("1", "3"), resultIds);
  }

  @Test
  void fromExpression_withActiveField_appliesCorrectly() {
    final ObjectDataSource dataSource = createPatientDataSourceWithActive();
    final Dataset<Row> dataset = dataSource.read("Patient");

    final Column filterColumn = builder.fromExpression(ResourceType.PATIENT, "active = true");
    final Dataset<Row> result = dataset.filter(filterColumn);

    final Set<String> resultIds = extractIds(result);
    assertEquals(Set.of("1", "3"), resultIds);
  }

  // ========== Filter composition tests ==========

  @Test
  void composedFilters_andLogic_worksCorrectly() {
    final ObjectDataSource dataSource = createPatientDataSourceWithActive();
    final Dataset<Row> dataset = dataSource.read("Patient");

    final Column genderFilter = builder.fromQueryString(ResourceType.PATIENT, "gender=male");
    final Column activeFilter = builder.fromExpression(ResourceType.PATIENT, "active = true");

    // Use standard SparkSQL Column.and() method
    final Column combined = genderFilter.and(activeFilter);
    final Dataset<Row> result = dataset.filter(combined);

    // Should match Patients 1 and 3: male AND active
    final Set<String> resultIds = extractIds(result);
    assertEquals(Set.of("1", "3"), resultIds);
  }

  @Test
  void composedFilters_orLogic_worksCorrectly() {
    final ObjectDataSource dataSource = createPatientDataSource();
    final Dataset<Row> dataset = dataSource.read("Patient");

    final Column maleFilter = builder.fromQueryString(ResourceType.PATIENT, "gender=male");
    final Column femaleFilter = builder.fromQueryString(ResourceType.PATIENT, "gender=female");

    // Use standard SparkSQL Column.or() method
    final Column combined = maleFilter.or(femaleFilter);
    final Dataset<Row> result = dataset.filter(combined);

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
        Arguments.of("active=false", Set.of("2")));
  }

  @ParameterizedTest(name = "equivalence test: {0}")
  @MethodSource("equivalenceTestCases")
  void columnBuilder_producesEquivalentResults_toFhirSearchExecutor(
      final String queryString, final Set<String> expectedIds) {

    final ObjectDataSource dataSource = createPatientDataSourceWithActive();
    final Dataset<Row> dataset = dataSource.read("Patient");

    // Execute using FhirSearchExecutor (existing implementation)
    final FhirSearchExecutor executor =
        FhirSearchExecutor.withRegistry(fhirContext, dataSource, registry);
    final FhirSearch search = FhirSearch.fromQueryString(queryString);
    final Dataset<Row> executorResult = executor.execute(ResourceType.PATIENT, search);
    final Set<String> executorIds = extractIds(executorResult);

    // Execute using SearchColumnBuilder (new implementation)
    final Column filterColumn = builder.fromQueryString(ResourceType.PATIENT, queryString);
    final Dataset<Row> filterResult = dataset.filter(filterColumn);
    final Set<String> filterIds = extractIds(filterResult);

    // Verify both produce the same results
    assertAll(
        () ->
            assertEquals(expectedIds, executorIds, "FhirSearchExecutor should return expected IDs"),
        () ->
            assertEquals(expectedIds, filterIds, "SearchColumnBuilder should return expected IDs"),
        () ->
            assertEquals(
                executorIds, filterIds, "Both implementations should return identical results"));
  }

  @Test
  void columnBuilder_schemaEquivalent_toFhirSearchExecutor() {
    final ObjectDataSource dataSource = createPatientDataSource();
    final Dataset<Row> dataset = dataSource.read("Patient");

    // Execute using FhirSearchExecutor
    final FhirSearchExecutor executor =
        FhirSearchExecutor.withRegistry(fhirContext, dataSource, registry);
    final FhirSearch search = FhirSearch.fromQueryString("gender=male");
    final Dataset<Row> executorResult = executor.execute(ResourceType.PATIENT, search);

    // Execute using SearchColumnBuilder
    final Column filterColumn = builder.fromQueryString(ResourceType.PATIENT, "gender=male");
    final Dataset<Row> filterResult = dataset.filter(filterColumn);

    // Schemas should be identical
    assertEquals(executorResult.schema(), filterResult.schema());
  }

  // ========== withDefaultRegistry integration test ==========

  @Test
  void withDefaultRegistry_canFilterWithStandardParameters() {
    final ObjectDataSource dataSource = createPatientDataSource();
    final Dataset<Row> dataset = dataSource.read("Patient");

    final SearchColumnBuilder defaultBuilder = SearchColumnBuilder.withDefaultRegistry(fhirContext);

    // Use a standard FHIR search parameter
    final Column filterColumn = defaultBuilder.fromQueryString(ResourceType.PATIENT, "gender=male");
    final Dataset<Row> result = dataset.filter(filterColumn);

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

  // ========== Reference search integration tests ==========

  Stream<Arguments> referenceSearchCases() {
    return Stream.of(
        // Single reference match (type-qualified).
        Arguments.of("subject=Patient/p1", Set.of("obs1", "obs2")),
        // OR logic with multiple values.
        Arguments.of("subject=Patient/p1,Patient/p2", Set.of("obs1", "obs2", "obs3")),
        // Bare ID matching.
        Arguments.of("subject=p1", Set.of("obs1", "obs2")),
        // Absolute URL matching.
        Arguments.of("subject=http://example.org/fhir/Patient/p3", Set.of("obs4")),
        // :not modifier.
        Arguments.of("subject:not=Patient/p1", Set.of("obs3", "obs4", "obs5")),
        // :[type] modifier with bare ID.
        Arguments.of("subject:Patient=p2", Set.of("obs3")));
  }

  @ParameterizedTest(name = "reference search: {0}")
  @MethodSource("referenceSearchCases")
  void referenceSearch_producesCorrectResults(
      final String queryString, final Set<String> expectedIds) {
    final ObjectDataSource dataSource = createObservationDataSource();
    final Dataset<Row> dataset = dataSource.read("Observation");

    final Column filterColumn = builder.fromQueryString(ResourceType.OBSERVATION, queryString);
    final Dataset<Row> result = dataset.filter(filterColumn);

    final Set<String> resultIds = extractIds(result);
    assertEquals(expectedIds, resultIds);
  }

  @Test
  void referenceSearch_arrayValuedParameter_matchesAnyElement() {
    // Patient.generalPractitioner is an array-valued reference. A match on any element should
    // include the resource.
    final ObjectDataSource dataSource = createPatientDataSourceWithGP();
    final Dataset<Row> dataset = dataSource.read("Patient");

    final Column filterColumn =
        builder.fromQueryString(ResourceType.PATIENT, "general-practitioner=Practitioner/gp1");
    final Dataset<Row> result = dataset.filter(filterColumn);

    // Patient 1 has gp1 in their array, Patient 2 has gp1 and gp2.
    final Set<String> resultIds = extractIds(result);
    assertEquals(Set.of("1", "2"), resultIds);
  }

  // ========== Data source creation methods ==========

  private ObjectDataSource createObservationDataSource() {
    // obs1: subject = Patient/p1
    final Observation obs1 = new Observation();
    obs1.setId("obs1");
    obs1.setSubject(new Reference("Patient/p1"));

    // obs2: subject = Patient/p1
    final Observation obs2 = new Observation();
    obs2.setId("obs2");
    obs2.setSubject(new Reference("Patient/p1"));

    // obs3: subject = Patient/p2
    final Observation obs3 = new Observation();
    obs3.setId("obs3");
    obs3.setSubject(new Reference("Patient/p2"));

    // obs4: subject = absolute URL
    final Observation obs4 = new Observation();
    obs4.setId("obs4");
    obs4.setSubject(new Reference("http://example.org/fhir/Patient/p3"));

    // obs5: no subject
    final Observation obs5 = new Observation();
    obs5.setId("obs5");

    return new ObjectDataSource(spark, encoders, List.of(obs1, obs2, obs3, obs4, obs5));
  }

  private ObjectDataSource createPatientDataSourceWithGP() {
    // Patient 1: generalPractitioner = [Practitioner/gp1]
    final Patient patient1 = new Patient();
    patient1.setId("1");
    patient1.addGeneralPractitioner(new Reference("Practitioner/gp1"));

    // Patient 2: generalPractitioner = [Practitioner/gp1, Practitioner/gp2]
    final Patient patient2 = new Patient();
    patient2.setId("2");
    patient2.addGeneralPractitioner(new Reference("Practitioner/gp1"));
    patient2.addGeneralPractitioner(new Reference("Practitioner/gp2"));

    // Patient 3: generalPractitioner = [Practitioner/gp3]
    final Patient patient3 = new Patient();
    patient3.setId("3");
    patient3.addGeneralPractitioner(new Reference("Practitioner/gp3"));

    // Patient 4: no generalPractitioner
    final Patient patient4 = new Patient();
    patient4.setId("4");

    return new ObjectDataSource(spark, encoders, List.of(patient1, patient2, patient3, patient4));
  }

  private Set<String> extractIds(final Dataset<Row> results) {
    return results.select("id").collectAsList().stream()
        .map(row -> row.getString(0))
        .collect(Collectors.toSet());
  }
}
