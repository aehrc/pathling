/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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
import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Address;
import org.hl7.fhir.r4.model.AuditEvent;
import org.hl7.fhir.r4.model.CapabilityStatement;
import org.hl7.fhir.r4.model.CarePlan;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.ContactPoint;
import org.hl7.fhir.r4.model.Coverage;
import org.hl7.fhir.r4.model.DecimalType;
import org.hl7.fhir.r4.model.Enumerations.AdministrativeGender;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.HumanName;
import org.hl7.fhir.r4.model.Identifier;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Period;
import org.hl7.fhir.r4.model.Quantity;
import org.hl7.fhir.r4.model.Reference;
import org.hl7.fhir.r4.model.RiskAssessment;
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

  // ========== URI search integration tests ==========

  @Test
  void uriSearch_fhirDefinedTypeUri_createsFilter() {
    // CarePlan.instantiatesUri resolves to FHIRDefinedType.URI. Verifies that the search column
    // builder can create a filter for a URI-type search parameter with this FHIR type.
    final ObjectDataSource dataSource = createCarePlanDataSource();
    final Dataset<Row> dataset = dataSource.read("CarePlan");

    final Column filterColumn =
        builder.fromQueryString(
            ResourceType.CAREPLAN, "instantiates-uri=http://example.org/protocol/diabetes");
    final Dataset<Row> result = dataset.filter(filterColumn);

    final Set<String> resultIds = extractIds(result);
    assertEquals(Set.of("cp1"), resultIds);
  }

  @Test
  void uriSearch_notModifier_includesNullArrayElements() {
    // Per FHIR spec, :not "includes resources that have no value for the parameter". For array
    // columns like CarePlan.instantiatesUri, resources with a null array must be included in
    // negated results.
    final ObjectDataSource dataSource = createCarePlanDataSource();
    final Dataset<Row> dataset = dataSource.read("CarePlan");

    final Column filterColumn =
        builder.fromQueryString(
            ResourceType.CAREPLAN, "instantiates-uri:not=http://example.org/protocol/diabetes");
    final Dataset<Row> result = dataset.filter(filterColumn);

    // cp2 has a different URI, cp3 has no instantiatesUri at all — both should be included.
    final Set<String> resultIds = extractIds(result);
    assertEquals(Set.of("cp2", "cp3"), resultIds);
  }

  @Test
  void uriSearch_fhirDefinedTypeUrl_createsFilter() {
    // CapabilityStatement.url resolves to FHIRDefinedType.URL. Verifies that the search column
    // builder can create a filter for a URI-type search parameter with this FHIR type.
    final ObjectDataSource dataSource = createCapabilityStatementDataSource();
    final Dataset<Row> dataset = dataSource.read("CapabilityStatement");

    final Column filterColumn =
        builder.fromQueryString(
            ResourceType.CAPABILITYSTATEMENT,
            "url=http://example.org/fhir/CapabilityStatement/server");
    final Dataset<Row> result = dataset.filter(filterColumn);

    final Set<String> resultIds = extractIds(result);
    assertEquals(Set.of("cs1"), resultIds);
  }

  private ObjectDataSource createCarePlanDataSource() {
    // cp1: instantiatesUri = [http://example.org/protocol/diabetes]
    final CarePlan cp1 = new CarePlan();
    cp1.setId("cp1");
    cp1.addInstantiatesUri("http://example.org/protocol/diabetes");

    // cp2: instantiatesUri = [http://example.org/protocol/cardiac]
    final CarePlan cp2 = new CarePlan();
    cp2.setId("cp2");
    cp2.addInstantiatesUri("http://example.org/protocol/cardiac");

    // cp3: no instantiatesUri
    final CarePlan cp3 = new CarePlan();
    cp3.setId("cp3");

    return new ObjectDataSource(spark, encoders, List.of(cp1, cp2, cp3));
  }

  private ObjectDataSource createCapabilityStatementDataSource() {
    // cs1: url = http://example.org/fhir/CapabilityStatement/server
    final CapabilityStatement cs1 = new CapabilityStatement();
    cs1.setId("cs1");
    cs1.setUrl("http://example.org/fhir/CapabilityStatement/server");

    // cs2: url = http://example.org/fhir/CapabilityStatement/client
    final CapabilityStatement cs2 = new CapabilityStatement();
    cs2.setId("cs2");
    cs2.setUrl("http://example.org/fhir/CapabilityStatement/client");

    return new ObjectDataSource(spark, encoders, List.of(cs1, cs2));
  }

  // ========== String search on HumanName (complex type) tests ==========

  @Test
  void stringSearch_nameParameter_matchesFamilyName() {
    // The "name" search parameter points to Patient.name (HumanName). A string search should
    // match against the family sub-field.
    final ObjectDataSource dataSource = createPatientDataSourceWithNames();
    final Dataset<Row> dataset = dataSource.read("Patient");

    final Column filterColumn = builder.fromQueryString(ResourceType.PATIENT, "name=smith");
    final Dataset<Row> result = dataset.filter(filterColumn);

    final Set<String> resultIds = extractIds(result);
    assertEquals(Set.of("1"), resultIds);
  }

  @Test
  void stringSearch_nameParameter_matchesGivenName() {
    // String search on Patient.name should match against given name sub-fields.
    final ObjectDataSource dataSource = createPatientDataSourceWithNames();
    final Dataset<Row> dataset = dataSource.read("Patient");

    final Column filterColumn = builder.fromQueryString(ResourceType.PATIENT, "name=john");
    final Dataset<Row> result = dataset.filter(filterColumn);

    final Set<String> resultIds = extractIds(result);
    assertEquals(Set.of("1"), resultIds);
  }

  @Test
  void stringSearch_nameParameter_matchesTextSubField() {
    // String search on Patient.name should match against the text sub-field.
    final ObjectDataSource dataSource = createPatientDataSourceWithNames();
    final Dataset<Row> dataset = dataSource.read("Patient");

    final Column filterColumn = builder.fromQueryString(ResourceType.PATIENT, "name=mr john");
    final Dataset<Row> result = dataset.filter(filterColumn);

    final Set<String> resultIds = extractIds(result);
    assertEquals(Set.of("1"), resultIds);
  }

  @Test
  void stringSearch_nameParameter_matchesPrefixSubField() {
    // String search on Patient.name should match against the prefix sub-field.
    final ObjectDataSource dataSource = createPatientDataSourceWithNames();
    final Dataset<Row> dataset = dataSource.read("Patient");

    final Column filterColumn = builder.fromQueryString(ResourceType.PATIENT, "name=mr");
    final Dataset<Row> result = dataset.filter(filterColumn);

    // Patient 1 has prefix "Mr".
    final Set<String> resultIds = extractIds(result);
    assertEquals(Set.of("1"), resultIds);
  }

  @Test
  void stringSearch_nameParameter_matchesSuffixSubField() {
    // String search on Patient.name should match against the suffix sub-field.
    final ObjectDataSource dataSource = createPatientDataSourceWithNames();
    final Dataset<Row> dataset = dataSource.read("Patient");

    final Column filterColumn = builder.fromQueryString(ResourceType.PATIENT, "name=jr");
    final Dataset<Row> result = dataset.filter(filterColumn);

    // Patient 2 has suffix "Jr".
    final Set<String> resultIds = extractIds(result);
    assertEquals(Set.of("2"), resultIds);
  }

  @Test
  void stringSearch_nameParameter_isCaseInsensitive() {
    // String search on Patient.name should be case-insensitive.
    final ObjectDataSource dataSource = createPatientDataSourceWithNames();
    final Dataset<Row> dataset = dataSource.read("Patient");

    final Column filterColumn = builder.fromQueryString(ResourceType.PATIENT, "name=SMITH");
    final Dataset<Row> result = dataset.filter(filterColumn);

    final Set<String> resultIds = extractIds(result);
    assertEquals(Set.of("1"), resultIds);
  }

  @Test
  void stringSearch_nameParameter_noMatch_returnsEmpty() {
    // String search should return no results when no sub-fields match.
    final ObjectDataSource dataSource = createPatientDataSourceWithNames();
    final Dataset<Row> dataset = dataSource.read("Patient");

    final Column filterColumn = builder.fromQueryString(ResourceType.PATIENT, "name=zzz");
    final Dataset<Row> result = dataset.filter(filterColumn);

    assertEquals(0, result.count());
  }

  // ========== String search on Address (complex type) tests ==========

  @Test
  void stringSearch_addressParameter_matchesCitySubField() {
    // The "address" search parameter points to Patient.address (Address). A string search should
    // match against the city sub-field.
    final ObjectDataSource dataSource = createPatientDataSourceWithAddresses();
    final Dataset<Row> dataset = dataSource.read("Patient");

    final Column filterColumn = builder.fromQueryString(ResourceType.PATIENT, "address=spring");
    final Dataset<Row> result = dataset.filter(filterColumn);

    final Set<String> resultIds = extractIds(result);
    assertEquals(Set.of("1"), resultIds);
  }

  @Test
  void stringSearch_addressParameter_matchesLineSubField() {
    // String search on Patient.address should match against the line sub-field.
    final ObjectDataSource dataSource = createPatientDataSourceWithAddresses();
    final Dataset<Row> dataset = dataSource.read("Patient");

    final Column filterColumn = builder.fromQueryString(ResourceType.PATIENT, "address=123 main");
    final Dataset<Row> result = dataset.filter(filterColumn);

    final Set<String> resultIds = extractIds(result);
    assertEquals(Set.of("1"), resultIds);
  }

  @Test
  void stringSearch_addressParameter_matchesStateSubField() {
    // String search on Patient.address should match against the state sub-field.
    final ObjectDataSource dataSource = createPatientDataSourceWithAddresses();
    final Dataset<Row> dataset = dataSource.read("Patient");

    final Column filterColumn = builder.fromQueryString(ResourceType.PATIENT, "address=vic");
    final Dataset<Row> result = dataset.filter(filterColumn);

    final Set<String> resultIds = extractIds(result);
    assertEquals(Set.of("2"), resultIds);
  }

  @Test
  void stringSearch_addressParameter_matchesPostalCodeSubField() {
    // String search on Patient.address should match against the postalCode sub-field.
    final ObjectDataSource dataSource = createPatientDataSourceWithAddresses();
    final Dataset<Row> dataset = dataSource.read("Patient");

    final Column filterColumn = builder.fromQueryString(ResourceType.PATIENT, "address=3000");
    final Dataset<Row> result = dataset.filter(filterColumn);

    final Set<String> resultIds = extractIds(result);
    assertEquals(Set.of("2"), resultIds);
  }

  @Test
  void stringSearch_addressParameter_matchesCountrySubField() {
    // String search on Patient.address should match against the country sub-field.
    final ObjectDataSource dataSource = createPatientDataSourceWithAddresses();
    final Dataset<Row> dataset = dataSource.read("Patient");

    final Column filterColumn = builder.fromQueryString(ResourceType.PATIENT, "address=aus");
    final Dataset<Row> result = dataset.filter(filterColumn);

    final Set<String> resultIds = extractIds(result);
    assertEquals(Set.of("2"), resultIds);
  }

  @Test
  void stringSearch_addressParameter_matchesTextSubField() {
    // String search on Patient.address should match against the text sub-field.
    final ObjectDataSource dataSource = createPatientDataSourceWithAddresses();
    final Dataset<Row> dataset = dataSource.read("Patient");

    final Column filterColumn =
        builder.fromQueryString(ResourceType.PATIENT, "address=123 main st");
    final Dataset<Row> result = dataset.filter(filterColumn);

    // Patient 1 has text "123 Main St, Springfield".
    final Set<String> resultIds = extractIds(result);
    assertEquals(Set.of("1"), resultIds);
  }

  @Test
  void stringSearch_addressParameter_matchesDistrictSubField() {
    // String search on Patient.address should match against the district sub-field.
    final ObjectDataSource dataSource = createPatientDataSourceWithAddresses();
    final Dataset<Row> dataset = dataSource.read("Patient");

    final Column filterColumn = builder.fromQueryString(ResourceType.PATIENT, "address=central");
    final Dataset<Row> result = dataset.filter(filterColumn);

    // Patient 2 has district "Central".
    final Set<String> resultIds = extractIds(result);
    assertEquals(Set.of("2"), resultIds);
  }

  // ========== Exact string modifier on complex types tests ==========

  @Test
  void exactStringSearch_nameParameter_matchesCaseSensitive() {
    // The :exact modifier on a HumanName string search should match case-sensitively.
    final ObjectDataSource dataSource = createPatientDataSourceWithNames();
    final Dataset<Row> dataset = dataSource.read("Patient");

    final Column filterColumn = builder.fromQueryString(ResourceType.PATIENT, "name:exact=Smith");
    final Dataset<Row> result = dataset.filter(filterColumn);

    final Set<String> resultIds = extractIds(result);
    assertEquals(Set.of("1"), resultIds);
  }

  @Test
  void exactStringSearch_nameParameter_wrongCase_noMatch() {
    // The :exact modifier should not match when case differs.
    final ObjectDataSource dataSource = createPatientDataSourceWithNames();
    final Dataset<Row> dataset = dataSource.read("Patient");

    final Column filterColumn = builder.fromQueryString(ResourceType.PATIENT, "name:exact=smith");
    final Dataset<Row> result = dataset.filter(filterColumn);

    assertEquals(0, result.count());
  }

  @Test
  void exactStringSearch_addressParameter_matchesCaseSensitive() {
    // The :exact modifier on an Address string search should match case-sensitively.
    final ObjectDataSource dataSource = createPatientDataSourceWithAddresses();
    final Dataset<Row> dataset = dataSource.read("Patient");

    final Column filterColumn =
        builder.fromQueryString(ResourceType.PATIENT, "address:exact=Springfield");
    final Dataset<Row> result = dataset.filter(filterColumn);

    final Set<String> resultIds = extractIds(result);
    assertEquals(Set.of("1"), resultIds);
  }

  // ========== Parameter type coverage tests ==========

  @Test
  void stringSearch_familyParameter_matchesFamilyName() {
    // STRING + string: the "family" parameter points to Patient.name.family (a plain string).
    final ObjectDataSource dataSource = createPatientDataSourceWithNames();
    final Dataset<Row> dataset = dataSource.read("Patient");

    final Column filterColumn = builder.fromQueryString(ResourceType.PATIENT, "family=smith");
    final Dataset<Row> result = dataset.filter(filterColumn);

    final Set<String> resultIds = extractIds(result);
    assertEquals(Set.of("1"), resultIds);
  }

  @Test
  void tokenSearch_codeableConcept_matchesCode() {
    // TOKEN + CodeableConcept: the "code" parameter points to Observation.code.
    final ObjectDataSource dataSource = createObservationDataSourceWithCodes();
    final Dataset<Row> dataset = dataSource.read("Observation");

    final Column filterColumn =
        builder.fromQueryString(ResourceType.OBSERVATION, "code=http://loinc.org|1234-5");
    final Dataset<Row> result = dataset.filter(filterColumn);

    final Set<String> resultIds = extractIds(result);
    assertEquals(Set.of("obs1"), resultIds);
  }

  @Test
  void tokenSearch_identifier_matchesSystemAndValue() {
    // TOKEN + Identifier: the "identifier" parameter points to Patient.identifier.
    final ObjectDataSource dataSource = createPatientDataSourceWithIdentifiers();
    final Dataset<Row> dataset = dataSource.read("Patient");

    final Column filterColumn =
        builder.fromQueryString(ResourceType.PATIENT, "identifier=http://example.org/mrn|MRN001");
    final Dataset<Row> result = dataset.filter(filterColumn);

    final Set<String> resultIds = extractIds(result);
    assertEquals(Set.of("1"), resultIds);
  }

  @Test
  void tokenSearch_contactPoint_matchesValue() {
    // TOKEN + ContactPoint: the "telecom" parameter points to Patient.telecom. ContactPoint
    // matching uses only the value field (system|code syntax is not supported).
    final ObjectDataSource dataSource = createPatientDataSourceWithTelecoms();
    final Dataset<Row> dataset = dataSource.read("Patient");

    final Column filterColumn = builder.fromQueryString(ResourceType.PATIENT, "telecom=555-1234");
    final Dataset<Row> result = dataset.filter(filterColumn);

    final Set<String> resultIds = extractIds(result);
    assertEquals(Set.of("1"), resultIds);
  }

  @Test
  void dateSearch_dateType_matchesBirthDate() {
    // DATE + date: the "birthdate" parameter points to Patient.birthDate.
    final ObjectDataSource dataSource = createPatientDataSourceWithBirthDates();
    final Dataset<Row> dataset = dataSource.read("Patient");

    final Column filterColumn =
        builder.fromQueryString(ResourceType.PATIENT, "birthdate=1990-01-15");
    final Dataset<Row> result = dataset.filter(filterColumn);

    final Set<String> resultIds = extractIds(result);
    assertEquals(Set.of("1"), resultIds);
  }

  @Test
  void dateSearch_period_matchesCoveragePeriod() {
    // DATE + Period: the "period" parameter points to Coverage.period.
    final ObjectDataSource dataSource = createCoverageDataSource();
    final Dataset<Row> dataset = dataSource.read("Coverage");

    // Search for a date that falls within the coverage period.
    final Column filterColumn = builder.fromQueryString(ResourceType.COVERAGE, "period=2024-06-15");
    final Dataset<Row> result = dataset.filter(filterColumn);

    final Set<String> resultIds = extractIds(result);
    assertEquals(Set.of("cov1"), resultIds);
  }

  @Test
  void dateSearch_instant_matchesAuditEventRecorded() {
    // DATE + instant: the "date" parameter points to AuditEvent.recorded.
    final ObjectDataSource dataSource = createAuditEventDataSource();
    final Dataset<Row> dataset = dataSource.read("AuditEvent");

    final Column filterColumn = builder.fromQueryString(ResourceType.AUDITEVENT, "date=2024-03-15");
    final Dataset<Row> result = dataset.filter(filterColumn);

    final Set<String> resultIds = extractIds(result);
    assertEquals(Set.of("ae1"), resultIds);
  }

  @Test
  void numberSearch_decimal_matchesProbability() {
    // NUMBER + decimal: the "probability" parameter points to
    // RiskAssessment.prediction.probability.ofType(decimal).
    final ObjectDataSource dataSource = createRiskAssessmentDataSource();
    final Dataset<Row> dataset = dataSource.read("RiskAssessment");

    final Column filterColumn =
        builder.fromQueryString(ResourceType.RISKASSESSMENT, "probability=gt0.8");
    final Dataset<Row> result = dataset.filter(filterColumn);

    final Set<String> resultIds = extractIds(result);
    assertEquals(Set.of("ra1"), resultIds);
  }

  @Test
  void quantitySearch_quantity_matchesValueQuantity() {
    // QUANTITY + Quantity: the "value-quantity" parameter points to
    // Observation.value.ofType(Quantity).
    final ObjectDataSource dataSource = createObservationDataSourceWithQuantities();
    final Dataset<Row> dataset = dataSource.read("Observation");

    final Column filterColumn =
        builder.fromQueryString(
            ResourceType.OBSERVATION, "value-quantity=5.4|http://unitsofmeasure.org|mg");
    final Dataset<Row> result = dataset.filter(filterColumn);

    final Set<String> resultIds = extractIds(result);
    assertEquals(Set.of("obs1"), resultIds);
  }

  // ========== Data source creation methods for parameter type coverage tests ==========

  private ObjectDataSource createObservationDataSourceWithCodes() {
    final Observation obs1 = new Observation();
    obs1.setId("obs1");
    obs1.setCode(new CodeableConcept(new Coding("http://loinc.org", "1234-5", "Test")));

    final Observation obs2 = new Observation();
    obs2.setId("obs2");
    obs2.setCode(new CodeableConcept(new Coding("http://loinc.org", "9999-9", "Other")));

    return new ObjectDataSource(spark, encoders, List.of(obs1, obs2));
  }

  private ObjectDataSource createPatientDataSourceWithIdentifiers() {
    final Patient patient1 = new Patient();
    patient1.setId("1");
    patient1.addIdentifier(new Identifier().setSystem("http://example.org/mrn").setValue("MRN001"));

    final Patient patient2 = new Patient();
    patient2.setId("2");
    patient2.addIdentifier(new Identifier().setSystem("http://example.org/mrn").setValue("MRN002"));

    return new ObjectDataSource(spark, encoders, List.of(patient1, patient2));
  }

  private ObjectDataSource createPatientDataSourceWithTelecoms() {
    final Patient patient1 = new Patient();
    patient1.setId("1");
    patient1.addTelecom(
        new ContactPoint().setSystem(ContactPoint.ContactPointSystem.PHONE).setValue("555-1234"));

    final Patient patient2 = new Patient();
    patient2.setId("2");
    patient2.addTelecom(
        new ContactPoint()
            .setSystem(ContactPoint.ContactPointSystem.EMAIL)
            .setValue("test@example.org"));

    return new ObjectDataSource(spark, encoders, List.of(patient1, patient2));
  }

  private ObjectDataSource createPatientDataSourceWithBirthDates() {
    final Patient patient1 = new Patient();
    patient1.setId("1");
    patient1.setBirthDateElement(new org.hl7.fhir.r4.model.DateType("1990-01-15"));

    final Patient patient2 = new Patient();
    patient2.setId("2");
    patient2.setBirthDateElement(new org.hl7.fhir.r4.model.DateType("1985-06-20"));

    return new ObjectDataSource(spark, encoders, List.of(patient1, patient2));
  }

  private ObjectDataSource createCoverageDataSource() {
    final Coverage cov1 = new Coverage();
    cov1.setId("cov1");
    cov1.setPeriod(
        new Period()
            .setStartElement(new org.hl7.fhir.r4.model.DateTimeType("2024-01-01"))
            .setEndElement(new org.hl7.fhir.r4.model.DateTimeType("2024-12-31")));

    final Coverage cov2 = new Coverage();
    cov2.setId("cov2");
    cov2.setPeriod(
        new Period()
            .setStartElement(new org.hl7.fhir.r4.model.DateTimeType("2025-01-01"))
            .setEndElement(new org.hl7.fhir.r4.model.DateTimeType("2025-12-31")));

    return new ObjectDataSource(spark, encoders, List.of(cov1, cov2));
  }

  @SuppressWarnings("deprecation")
  private ObjectDataSource createAuditEventDataSource() {
    final AuditEvent ae1 = new AuditEvent();
    ae1.setId("ae1");
    ae1.setRecorded(new Date(124, 2, 15, 10, 30, 0));

    final AuditEvent ae2 = new AuditEvent();
    ae2.setId("ae2");
    ae2.setRecorded(new Date(124, 5, 20, 14, 0, 0));

    return new ObjectDataSource(spark, encoders, List.of(ae1, ae2));
  }

  private ObjectDataSource createRiskAssessmentDataSource() {
    final RiskAssessment ra1 = new RiskAssessment();
    ra1.setId("ra1");
    final RiskAssessment.RiskAssessmentPredictionComponent pred1 = ra1.addPrediction();
    pred1.setProbability(new DecimalType(new BigDecimal("0.95")));

    final RiskAssessment ra2 = new RiskAssessment();
    ra2.setId("ra2");
    final RiskAssessment.RiskAssessmentPredictionComponent pred2 = ra2.addPrediction();
    pred2.setProbability(new DecimalType(new BigDecimal("0.5")));

    return new ObjectDataSource(spark, encoders, List.of(ra1, ra2));
  }

  private ObjectDataSource createObservationDataSourceWithQuantities() {
    final Observation obs1 = new Observation();
    obs1.setId("obs1");
    obs1.setValue(
        new Quantity()
            .setValue(new BigDecimal("5.4"))
            .setSystem("http://unitsofmeasure.org")
            .setCode("mg"));

    final Observation obs2 = new Observation();
    obs2.setId("obs2");
    obs2.setValue(
        new Quantity()
            .setValue(new BigDecimal("10.0"))
            .setSystem("http://unitsofmeasure.org")
            .setCode("mg"));

    return new ObjectDataSource(spark, encoders, List.of(obs1, obs2));
  }

  // ========== Data source creation methods for complex type tests ==========

  private ObjectDataSource createPatientDataSourceWithNames() {
    // Patient 1: name = {family: "Smith", given: ["John"], text: "Mr John Smith",
    //            prefix: ["Mr"]}
    final Patient patient1 = new Patient();
    patient1.setId("1");
    final HumanName name1 = patient1.addName();
    name1.setFamily("Smith");
    name1.addGiven("John");
    name1.setText("Mr John Smith");
    name1.addPrefix("Mr");

    // Patient 2: name = {family: "Jones", given: ["Jane"], suffix: ["Jr"]}
    final Patient patient2 = new Patient();
    patient2.setId("2");
    final HumanName name2 = patient2.addName();
    name2.setFamily("Jones");
    name2.addGiven("Jane");
    name2.addSuffix("Jr");

    // Patient 3: no name
    final Patient patient3 = new Patient();
    patient3.setId("3");

    return new ObjectDataSource(spark, encoders, List.of(patient1, patient2, patient3));
  }

  private ObjectDataSource createPatientDataSourceWithAddresses() {
    // Patient 1: address = {text: "123 Main St, Springfield", line: ["123 Main St"],
    //            city: "Springfield"}
    final Patient patient1 = new Patient();
    patient1.setId("1");
    final Address addr1 = patient1.addAddress();
    addr1.setText("123 Main St, Springfield");
    addr1.addLine("123 Main St");
    addr1.setCity("Springfield");

    // Patient 2: address = {city: "Melbourne", state: "VIC", postalCode: "3000",
    //            country: "Australia", district: "Central"}
    final Patient patient2 = new Patient();
    patient2.setId("2");
    final Address addr2 = patient2.addAddress();
    addr2.setCity("Melbourne");
    addr2.setState("VIC");
    addr2.setPostalCode("3000");
    addr2.setCountry("Australia");
    addr2.setDistrict("Central");

    // Patient 3: no address
    final Patient patient3 = new Patient();
    patient3.setId("3");

    return new ObjectDataSource(spark, encoders, List.of(patient1, patient2, patient3));
  }

  private Set<String> extractIds(final Dataset<Row> results) {
    return results.select("id").collectAsList().stream()
        .map(row -> row.getString(0))
        .collect(Collectors.toSet());
  }
}
