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
import org.hl7.fhir.r4.model.Patient;
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
        Arguments.of("gender search male",
            (Supplier<ObjectDataSource>) this::createPatientDataSource,
            "gender", List.of("male"), Set.of("1", "3")),
        Arguments.of("gender search female",
            (Supplier<ObjectDataSource>) this::createPatientDataSource,
            "gender", List.of("female"), Set.of("2")),
        Arguments.of("gender search multiple values",
            (Supplier<ObjectDataSource>) this::createPatientDataSource,
            "gender", List.of("male", "female"), Set.of("1", "2", "3")),
        Arguments.of("gender search no matches",
            (Supplier<ObjectDataSource>) this::createPatientDataSource,
            "gender", List.of("other"), Set.of()),

        // Address-use tests
        Arguments.of("address-use search home",
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithAddresses,
            "address-use", List.of("home"), Set.of("1", "2")),
        Arguments.of("address-use search work",
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithAddresses,
            "address-use", List.of("work"), Set.of("2")),
        Arguments.of("address-use search no matches",
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithAddresses,
            "address-use", List.of("billing"), Set.of()),
        Arguments.of("address-use search multiple values",
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithAddresses,
            "address-use", List.of("home", "temp"), Set.of("1", "2", "3")),

        // Family tests
        Arguments.of("family search exact match",
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithNames,
            "family", List.of("Smith"), Set.of("1")),
        Arguments.of("family search case insensitive",
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithNames,
            "family", List.of("smith"), Set.of("1")),
        Arguments.of("family search starts with",
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithNames,
            "family", List.of("Smi"), Set.of("1")),
        Arguments.of("family search no match",
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithNames,
            "family", List.of("Williams"), Set.of()),
        Arguments.of("family search multiple names",
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithNames,
            "family", List.of("John"), Set.of("2")),
        Arguments.of("family search multiple values",
            (Supplier<ObjectDataSource>) this::createPatientDataSourceWithNames,
            "family", List.of("Smith", "Jones"), Set.of("1", "2"))
    );
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("searchTestCases")
  void testPatientSearch(final String testName,
      final Supplier<ObjectDataSource> dataSourceSupplier,
      final String paramCode, final List<String> searchValues, final Set<String> expectedIds) {

    final ObjectDataSource dataSource = dataSourceSupplier.get();

    final FhirSearch search = FhirSearch.builder()
        .criterion(paramCode, searchValues.toArray(new String[0]))
        .build();

    final FhirSearchExecutor executor = new FhirSearchExecutor(
        encoders.getContext(), dataSource);

    final Dataset<Row> results = executor.execute(ResourceType.PATIENT, search);

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
   * Extracts patient IDs from a result dataset.
   */
  private Set<String> extractIds(final Dataset<Row> results) {
    return results.select("id").collectAsList().stream()
        .map(row -> row.getString(0))
        .collect(Collectors.toSet());
  }
}
