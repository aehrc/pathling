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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.datasource.ObjectDataSource;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Address.AddressUse;
import org.hl7.fhir.r4.model.Enumerations.AdministrativeGender;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.Patient;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Tests for {@link FhirSearchExecutor}.
 */
@SpringBootUnitTest
@Slf4j
class FhirSearchExecutorTest {

  @Autowired
  SparkSession spark;

  @Autowired
  FhirEncoders encoders;

  @Test
  void testGenderSearchMale() {
    final ObjectDataSource dataSource = createPatientDataSource();

    final FhirSearch search = FhirSearch.builder()
        .criterion("gender", "male")
        .build();

    final FhirSearchExecutor executor = new FhirSearchExecutor(
        encoders.getContext(), dataSource);

    final Dataset<Row> results = executor.execute(ResourceType.PATIENT, search);

    assertEquals(2, results.count());
    final Set<String> ids = extractIds(results);
    assertTrue(ids.contains("1"));
    assertTrue(ids.contains("3"));
  }

  @Test
  void testGenderSearchFemale() {
    final ObjectDataSource dataSource = createPatientDataSource();

    final FhirSearch search = FhirSearch.builder()
        .criterion("gender", "female")
        .build();

    final FhirSearchExecutor executor = new FhirSearchExecutor(
        encoders.getContext(), dataSource);

    final Dataset<Row> results = executor.execute(ResourceType.PATIENT, search);

    assertEquals(1, results.count());
    final Set<String> ids = extractIds(results);
    assertTrue(ids.contains("2"));
  }

  @Test
  void testGenderSearchMultipleValues() {
    final ObjectDataSource dataSource = createPatientDataSource();

    // Search for male OR female
    final FhirSearch search = FhirSearch.builder()
        .criterion("gender", "male", "female")
        .build();

    final FhirSearchExecutor executor = new FhirSearchExecutor(
        encoders.getContext(), dataSource);

    final Dataset<Row> results = executor.execute(ResourceType.PATIENT, search);

    // Should match patients 1, 2, and 3 (all with gender set)
    assertEquals(3, results.count());
    final Set<String> ids = extractIds(results);
    assertTrue(ids.contains("1"));
    assertTrue(ids.contains("2"));
    assertTrue(ids.contains("3"));
  }

  @Test
  void testGenderSearchNoMatches() {
    final ObjectDataSource dataSource = createPatientDataSource();

    final FhirSearch search = FhirSearch.builder()
        .criterion("gender", "other")
        .build();

    final FhirSearchExecutor executor = new FhirSearchExecutor(
        encoders.getContext(), dataSource);

    final Dataset<Row> results = executor.execute(ResourceType.PATIENT, search);

    assertEquals(0, results.count());
  }

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
  void testNullGenderNotMatched() {
    final ObjectDataSource dataSource = createPatientDataSource();

    // Search for male - should NOT match patient 4 (null gender)
    final FhirSearch search = FhirSearch.builder()
        .criterion("gender", "male")
        .build();

    final FhirSearchExecutor executor = new FhirSearchExecutor(
        encoders.getContext(), dataSource);

    final Dataset<Row> results = executor.execute(ResourceType.PATIENT, search);

    assertEquals(2, results.count());
    final Set<String> ids = extractIds(results);
    assertFalse(ids.contains("4"));
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

  // ========== Array value tests (address-use) ==========

  @Test
  void testAddressUseSearchSingleAddress() {
    final ObjectDataSource dataSource = createPatientDataSourceWithAddresses();

    final FhirSearch search = FhirSearch.builder()
        .criterion("address-use", "home")
        .build();

    final FhirSearchExecutor executor = new FhirSearchExecutor(
        encoders.getContext(), dataSource);

    final Dataset<Row> results = executor.execute(ResourceType.PATIENT, search);

    // Patients 1 and 2 have home addresses
    assertEquals(2, results.count());
    final Set<String> ids = extractIds(results);
    assertTrue(ids.contains("1"));
    assertTrue(ids.contains("2"));
  }

  @Test
  void testAddressUseSearchMultipleAddressesOneMatches() {
    final ObjectDataSource dataSource = createPatientDataSourceWithAddresses();

    final FhirSearch search = FhirSearch.builder()
        .criterion("address-use", "work")
        .build();

    final FhirSearchExecutor executor = new FhirSearchExecutor(
        encoders.getContext(), dataSource);

    final Dataset<Row> results = executor.execute(ResourceType.PATIENT, search);

    // Patient 2 has both home and work addresses
    assertEquals(1, results.count());
    final Set<String> ids = extractIds(results);
    assertTrue(ids.contains("2"));
  }

  @Test
  void testAddressUseSearchNoMatches() {
    final ObjectDataSource dataSource = createPatientDataSourceWithAddresses();

    final FhirSearch search = FhirSearch.builder()
        .criterion("address-use", "billing")
        .build();

    final FhirSearchExecutor executor = new FhirSearchExecutor(
        encoders.getContext(), dataSource);

    final Dataset<Row> results = executor.execute(ResourceType.PATIENT, search);

    // No patients have billing addresses
    assertEquals(0, results.count());
  }

  @Test
  void testAddressUseSearchPatientWithNoAddresses() {
    final ObjectDataSource dataSource = createPatientDataSourceWithAddresses();

    final FhirSearch search = FhirSearch.builder()
        .criterion("address-use", "home")
        .build();

    final FhirSearchExecutor executor = new FhirSearchExecutor(
        encoders.getContext(), dataSource);

    final Dataset<Row> results = executor.execute(ResourceType.PATIENT, search);

    // Patient 4 has no addresses - should not be in results
    final Set<String> ids = extractIds(results);
    assertFalse(ids.contains("4"));
  }

  @Test
  void testAddressUseSearchMultipleValues() {
    final ObjectDataSource dataSource = createPatientDataSourceWithAddresses();

    // Search for home OR temp addresses
    final FhirSearch search = FhirSearch.builder()
        .criterion("address-use", "home", "temp")
        .build();

    final FhirSearchExecutor executor = new FhirSearchExecutor(
        encoders.getContext(), dataSource);

    final Dataset<Row> results = executor.execute(ResourceType.PATIENT, search);

    // Patients 1, 2 (home), and 3 (temp)
    assertEquals(3, results.count());
    final Set<String> ids = extractIds(results);
    assertTrue(ids.contains("1"));
    assertTrue(ids.contains("2"));
    assertTrue(ids.contains("3"));
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
