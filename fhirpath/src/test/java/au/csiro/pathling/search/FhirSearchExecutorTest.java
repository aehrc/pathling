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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.datasource.ObjectDataSource;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Address.AddressUse;
import org.hl7.fhir.r4.model.Enumerations.AdministrativeGender;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.DateType;
import org.hl7.fhir.r4.model.DecimalType;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.RiskAssessment;
import org.hl7.fhir.r4.model.RiskAssessment.RiskAssessmentPredictionComponent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Tests for {@link FhirSearchExecutor}.
 */
@SpringBootUnitTest
@TestInstance(Lifecycle.PER_CLASS)
@Slf4j
class FhirSearchExecutorTest {

  @Autowired
  SparkSession spark;

  @Autowired
  FhirEncoders encoders;

  // ========== Parameterized search tests ==========

  Stream<Arguments> searchTestCases() {
    return Stream.of(
        // Gender tests
        Arguments.of("gender search male", ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSource,
            "gender", List.of("male"), Set.of("1", "3")),
        Arguments.of("gender search female", ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSource,
            "gender", List.of("female"), Set.of("2")),
        Arguments.of("gender search multiple values", ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSource,
            "gender", List.of("male", "female"), Set.of("1", "2", "3")),
        Arguments.of("gender search no matches", ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSource,
            "gender", List.of("other"), Set.of()),

        // Address-use tests
        Arguments.of("address-use search home", ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithAddresses,
            "address-use", List.of("home"), Set.of("1", "2")),
        Arguments.of("address-use search work", ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithAddresses,
            "address-use", List.of("work"), Set.of("2")),
        Arguments.of("address-use search no matches", ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithAddresses,
            "address-use", List.of("billing"), Set.of()),
        Arguments.of("address-use search multiple values", ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithAddresses,
            "address-use", List.of("home", "temp"), Set.of("1", "2", "3")),

        // Family tests
        Arguments.of("family search exact match", ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithNames,
            "family", List.of("Smith"), Set.of("1")),
        Arguments.of("family search case insensitive", ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithNames,
            "family", List.of("smith"), Set.of("1")),
        Arguments.of("family search starts with", ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithNames,
            "family", List.of("Smi"), Set.of("1")),
        Arguments.of("family search no match", ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithNames,
            "family", List.of("Williams"), Set.of()),
        Arguments.of("family search multiple names", ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithNames,
            "family", List.of("John"), Set.of("2")),
        Arguments.of("family search multiple values", ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithNames,
            "family", List.of("Smith", "Jones"), Set.of("1", "2")),

        // :not modifier tests (token type)
        Arguments.of("gender:not search excludes male", ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSource,
            "gender:not", List.of("male"), Set.of("2", "4")),  // female + no gender
        Arguments.of("gender:not search excludes female", ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSource,
            "gender:not", List.of("female"), Set.of("1", "3", "4")),  // male + no gender

        // :exact modifier tests (string type)
        Arguments.of("family:exact search exact match", ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithNames,
            "family:exact", List.of("Smith"), Set.of("1")),
        Arguments.of("family:exact search wrong case", ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithNames,
            "family:exact", List.of("smith"), Set.of()),  // case-sensitive - no match
        Arguments.of("family:exact search partial", ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithNames,
            "family:exact", List.of("Smi"), Set.of()),  // prefix doesn't match

        // Birthdate tests (date type) - basic scenarios only, precision tests are in ElementMatcherTest
        Arguments.of("birthdate search match", ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithBirthDates,
            "birthdate", List.of("1990-01-15"), Set.of("1", "3")),
        Arguments.of("birthdate search no match", ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithBirthDates,
            "birthdate", List.of("2000-01-01"), Set.of()),
        Arguments.of("birthdate search with datetime value", ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithBirthDates,
            "birthdate", List.of("1990-01-15T10:00"), Set.of("1", "3")),
        Arguments.of("birthdate search multiple values", ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithBirthDates,
            "birthdate", List.of("1990-01-15", "1985-06-20"), Set.of("1", "2", "3")),

        // Date prefix tests - basic scenarios only, exhaustive tests are in ElementMatcherTest
        // ge - greater or equal
        Arguments.of("birthdate ge prefix", ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithBirthDates,
            "birthdate", List.of("ge1990-01-15"), Set.of("1", "3")),  // born on or after Jan 15, 1990
        Arguments.of("birthdate ge prefix with year", ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithBirthDates,
            "birthdate", List.of("ge1986"), Set.of("1", "3")),  // born 1986 or later

        // le - less or equal
        Arguments.of("birthdate le prefix", ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithBirthDates,
            "birthdate", List.of("le1989"), Set.of("2")),  // born 1989 or earlier

        // gt - greater than
        Arguments.of("birthdate gt prefix", ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithBirthDates,
            "birthdate", List.of("gt1985-06-20"), Set.of("1", "3")),  // born after Jun 20, 1985

        // lt - less than
        Arguments.of("birthdate lt prefix", ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithBirthDates,
            "birthdate", List.of("lt1990-01-15"), Set.of("2")),  // born before Jan 15, 1990

        // ne - not equal
        Arguments.of("birthdate ne prefix", ResourceType.PATIENT,
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithBirthDates,
            "birthdate", List.of("ne1990-01-15"), Set.of("2")),  // NOT born on Jan 15, 1990

        // ========== RiskAssessment probability tests (number type) ==========
        // eq - equals (default)
        Arguments.of("probability exact match", ResourceType.RISKASSESSMENT,
            (Supplier<ObjectDataSource>) this::createRiskAssessmentDataSource,
            "probability", List.of("0.5"), Set.of("2")),
        Arguments.of("probability eq prefix", ResourceType.RISKASSESSMENT,
            (Supplier<ObjectDataSource>) this::createRiskAssessmentDataSource,
            "probability", List.of("eq0.5"), Set.of("2")),
        Arguments.of("probability multiple values", ResourceType.RISKASSESSMENT,
            (Supplier<ObjectDataSource>) this::createRiskAssessmentDataSource,
            "probability", List.of("0.2", "0.8"), Set.of("1", "3")),

        // gt - greater than
        Arguments.of("probability gt prefix", ResourceType.RISKASSESSMENT,
            (Supplier<ObjectDataSource>) this::createRiskAssessmentDataSource,
            "probability", List.of("gt0.5"), Set.of("3")),

        // ge - greater or equal
        Arguments.of("probability ge prefix", ResourceType.RISKASSESSMENT,
            (Supplier<ObjectDataSource>) this::createRiskAssessmentDataSource,
            "probability", List.of("ge0.5"), Set.of("2", "3")),

        // lt - less than
        Arguments.of("probability lt prefix", ResourceType.RISKASSESSMENT,
            (Supplier<ObjectDataSource>) this::createRiskAssessmentDataSource,
            "probability", List.of("lt0.5"), Set.of("1")),

        // le - less or equal
        Arguments.of("probability le prefix", ResourceType.RISKASSESSMENT,
            (Supplier<ObjectDataSource>) this::createRiskAssessmentDataSource,
            "probability", List.of("le0.5"), Set.of("1", "2")),

        // ne - not equal
        Arguments.of("probability ne prefix", ResourceType.RISKASSESSMENT,
            (Supplier<ObjectDataSource>) this::createRiskAssessmentDataSource,
            "probability", List.of("ne0.5"), Set.of("1", "3"))
    );
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("searchTestCases")
  void testSearch(final String testName, final ResourceType resourceType,
      final Supplier<ObjectDataSource> dataSourceSupplier,
      final String paramCode, final List<String> searchValues, final Set<String> expectedIds) {

    final ObjectDataSource dataSource = dataSourceSupplier.get();

    final FhirSearch search = FhirSearch.builder()
        .criterion(paramCode, searchValues.toArray(new String[0]))
        .build();

    final FhirSearchExecutor executor = new FhirSearchExecutor(
        encoders.getContext(), dataSource);

    final Dataset<Row> results = executor.execute(resourceType, search);

    assertEquals(expectedIds, extractIds(results));
  }

  // ========== Special tests (not parameterized) ==========

  @Test
  void testNoCriteriaReturnsAll() {
    final ObjectDataSource dataSource = createPatientDataSource();

    final FhirSearch search = FhirSearch.builder().build();

    final FhirSearchExecutor executor = new FhirSearchExecutor(
        encoders.getContext(), dataSource);

    final Dataset<Row> results = executor.execute(ResourceType.PATIENT, search);

    // Should return all 4 patients
    assertEquals(4, results.count());
  }

  @Test
  void testUnknownParameterThrowsException() {
    final ObjectDataSource dataSource = createPatientDataSource();

    final FhirSearch search = FhirSearch.builder()
        .criterion("unknown-param", "value")
        .build();

    final FhirSearchExecutor executor = new FhirSearchExecutor(
        encoders.getContext(), dataSource);

    final UnknownSearchParameterException exception = assertThrows(
        UnknownSearchParameterException.class,
        () -> executor.execute(ResourceType.PATIENT, search));

    assertTrue(exception.getMessage().contains("unknown-param"));
    assertTrue(exception.getMessage().contains("Patient"));
  }

  @Test
  void testResultSchemaMatchesOriginal() {
    final ObjectDataSource dataSource = createPatientDataSource();

    final FhirSearch search = FhirSearch.builder()
        .criterion("gender", "male")
        .build();

    final FhirSearchExecutor executor = new FhirSearchExecutor(
        encoders.getContext(), dataSource);

    final Dataset<Row> results = executor.execute(ResourceType.PATIENT, search);
    final Dataset<Row> original = dataSource.read("Patient");

    // Schema should be identical
    assertEquals(original.schema(), results.schema());
  }

  @Test
  void testInvalidModifierOnTokenThrowsException() {
    final ObjectDataSource dataSource = createPatientDataSource();

    // :exact is not valid for token type parameters
    final FhirSearch search = FhirSearch.builder()
        .criterion("gender:exact", "male")
        .build();

    final FhirSearchExecutor executor = new FhirSearchExecutor(
        encoders.getContext(), dataSource);

    final InvalidModifierException exception = assertThrows(
        InvalidModifierException.class,
        () -> executor.execute(ResourceType.PATIENT, search));

    assertTrue(exception.getMessage().contains("exact"));
    assertTrue(exception.getMessage().contains("TOKEN"));
  }

  @Test
  void testInvalidModifierOnStringThrowsException() {
    final ObjectDataSource dataSource = createPatientDataSourceWithNames();

    // :not is not valid for string type parameters
    final FhirSearch search = FhirSearch.builder()
        .criterion("family:not", "Smith")
        .build();

    final FhirSearchExecutor executor = new FhirSearchExecutor(
        encoders.getContext(), dataSource);

    final InvalidModifierException exception = assertThrows(
        InvalidModifierException.class,
        () -> executor.execute(ResourceType.PATIENT, search));

    assertTrue(exception.getMessage().contains("not"));
    assertTrue(exception.getMessage().contains("STRING"));
  }

  @Test
  void testInvalidModifierOnDateThrowsException() {
    final ObjectDataSource dataSource = createPatientDataSourceWithBirthDates();

    // No modifiers are supported for date type parameters in the limited implementation
    final FhirSearch search = FhirSearch.builder()
        .criterion("birthdate:exact", "1990-01-15")
        .build();

    final FhirSearchExecutor executor = new FhirSearchExecutor(
        encoders.getContext(), dataSource);

    final InvalidModifierException exception = assertThrows(
        InvalidModifierException.class,
        () -> executor.execute(ResourceType.PATIENT, search));

    assertTrue(exception.getMessage().contains("exact"));
    assertTrue(exception.getMessage().contains("DATE"));
  }

  // ========== Data source creation methods ==========

  /**
   * Creates a test data source with patients having different name configurations.
   * - Patient 1: name with family "Smith"
   * - Patient 2: two names with families "Jones" and "Johnson"
   * - Patient 3: name with family "Brown"
   * - Patient 4: no name
   */
  private ObjectDataSource createPatientDataSourceWithNames() {
    final Patient patient1 = new Patient();
    patient1.setId("1");
    patient1.addName().setFamily("Smith").addGiven("John");

    final Patient patient2 = new Patient();
    patient2.setId("2");
    patient2.addName().setFamily("Jones").addGiven("Jane");
    patient2.addName().setFamily("Johnson").addGiven("Jane");  // Maiden name

    final Patient patient3 = new Patient();
    patient3.setId("3");
    patient3.addName().setFamily("Brown").addGiven("Bob");

    final Patient patient4 = new Patient();
    patient4.setId("4");
    // No name

    return new ObjectDataSource(spark, encoders, List.of(patient1, patient2, patient3, patient4));
  }

  /**
   * Creates a test data source with patients having different address configurations.
   * - Patient 1: one home address
   * - Patient 2: home and work addresses
   * - Patient 3: temp address
   * - Patient 4: no addresses
   */
  private ObjectDataSource createPatientDataSourceWithAddresses() {
    final Patient patient1 = new Patient();
    patient1.setId("1");
    patient1.addAddress().setUse(AddressUse.HOME).setCity("Sydney");

    final Patient patient2 = new Patient();
    patient2.setId("2");
    patient2.addAddress().setUse(AddressUse.HOME).setCity("Melbourne");
    patient2.addAddress().setUse(AddressUse.WORK).setCity("Brisbane");

    final Patient patient3 = new Patient();
    patient3.setId("3");
    patient3.addAddress().setUse(AddressUse.TEMP).setCity("Perth");

    final Patient patient4 = new Patient();
    patient4.setId("4");
    // No addresses

    return new ObjectDataSource(spark, encoders, List.of(patient1, patient2, patient3, patient4));
  }

  /**
   * Creates a test data source with patients having different birth dates.
   * - Patient 1: birthDate = "1990-01-15"
   * - Patient 2: birthDate = "1985-06-20"
   * - Patient 3: birthDate = "1990-01-15" (same as Patient 1)
   * - Patient 4: no birthDate
   */
  private ObjectDataSource createPatientDataSourceWithBirthDates() {
    final Patient patient1 = new Patient();
    patient1.setId("1");
    patient1.setBirthDateElement(new DateType("1990-01-15"));

    final Patient patient2 = new Patient();
    patient2.setId("2");
    patient2.setBirthDateElement(new DateType("1985-06-20"));

    final Patient patient3 = new Patient();
    patient3.setId("3");
    patient3.setBirthDateElement(new DateType("1990-01-15"));

    final Patient patient4 = new Patient();
    patient4.setId("4");
    // No birthDate set

    return new ObjectDataSource(spark, encoders, List.of(patient1, patient2, patient3, patient4));
  }

  /**
   * Creates a test data source with patients having different genders.
   */
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
    // No gender set

    return new ObjectDataSource(spark, encoders, List.of(patient1, patient2, patient3, patient4));
  }

  /**
   * Creates a test data source with RiskAssessments having different probabilities.
   * - RiskAssessment 1: probability = 0.2
   * - RiskAssessment 2: probability = 0.5
   * - RiskAssessment 3: probability = 0.8
   * - RiskAssessment 4: no probability
   */
  private ObjectDataSource createRiskAssessmentDataSource() {
    final RiskAssessment ra1 = new RiskAssessment();
    ra1.setId("1");
    final RiskAssessmentPredictionComponent prediction1 = ra1.addPrediction();
    prediction1.setProbability(new DecimalType("0.2"));

    final RiskAssessment ra2 = new RiskAssessment();
    ra2.setId("2");
    final RiskAssessmentPredictionComponent prediction2 = ra2.addPrediction();
    prediction2.setProbability(new DecimalType("0.5"));

    final RiskAssessment ra3 = new RiskAssessment();
    ra3.setId("3");
    final RiskAssessmentPredictionComponent prediction3 = ra3.addPrediction();
    prediction3.setProbability(new DecimalType("0.8"));

    final RiskAssessment ra4 = new RiskAssessment();
    ra4.setId("4");
    // No prediction/probability

    return new ObjectDataSource(spark, encoders, List.of(ra1, ra2, ra3, ra4));
  }

  /**
   * Extracts resource IDs from a result dataset.
   */
  private Set<String> extractIds(final Dataset<Row> results) {
    return results.select("id").collectAsList().stream()
        .map(row -> row.getString(0))
        .collect(Collectors.toSet());
  }
}
