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

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.datasource.ObjectDataSource;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.AdministrativeGender;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.Patient;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Tests for {@link ResourceFilter}.
 */
@SpringBootUnitTest
class ResourceFilterTest {

  @Autowired
  SparkSession spark;

  @Autowired
  FhirEncoders encoders;

  @Test
  void apply_filtersDatasetCorrectly() {
    final Dataset<Row> dataset = createPatientDataSource().read("Patient");

    // Create a filter that matches male patients
    // The filter expression uses the wrapped schema format: Patient.gender = 'male'
    final ResourceFilter filter = new ResourceFilter(
        ResourceType.PATIENT,
        col("Patient.gender").equalTo(lit("male")));

    final Dataset<Row> result = filter.apply(dataset);

    // Should return only the male patients
    final Set<String> resultIds = extractIds(result);
    assertEquals(Set.of("1", "3"), resultIds);
  }

  @Test
  void apply_preservesOriginalSchema() {
    final Dataset<Row> dataset = createPatientDataSource().read("Patient");

    final ResourceFilter filter = new ResourceFilter(
        ResourceType.PATIENT,
        col("Patient.gender").equalTo(lit("male")));

    final Dataset<Row> result = filter.apply(dataset);

    // Schema should match the original flat schema
    assertEquals(dataset.schema(), result.schema());
  }

  @Test
  void and_combinesFiltersWithAndLogic() {
    final ResourceFilter filter1 = new ResourceFilter(
        ResourceType.PATIENT, col("Patient.gender").equalTo(lit("male")));
    final ResourceFilter filter2 = new ResourceFilter(
        ResourceType.PATIENT, col("Patient.active").equalTo(lit(true)));

    final ResourceFilter combined = filter1.and(filter2);

    assertEquals(ResourceType.PATIENT, combined.getResourceType());
  }

  @Test
  void or_combinesFiltersWithOrLogic() {
    final ResourceFilter filter1 = new ResourceFilter(
        ResourceType.PATIENT, col("Patient.gender").equalTo(lit("male")));
    final ResourceFilter filter2 = new ResourceFilter(
        ResourceType.PATIENT, col("Patient.gender").equalTo(lit("female")));

    final ResourceFilter combined = filter1.or(filter2);

    assertEquals(ResourceType.PATIENT, combined.getResourceType());
  }

  @Test
  void not_negatesFilter() {
    final ResourceFilter filter = new ResourceFilter(
        ResourceType.PATIENT, col("Patient.gender").equalTo(lit("male")));

    final ResourceFilter negated = filter.not();

    assertEquals(ResourceType.PATIENT, negated.getResourceType());
  }

  @Test
  void and_throwsExceptionForDifferentResourceTypes() {
    final ResourceFilter patientFilter = new ResourceFilter(
        ResourceType.PATIENT, lit(true));
    final ResourceFilter observationFilter = new ResourceFilter(
        ResourceType.OBSERVATION, lit(true));

    final IllegalArgumentException exception = assertThrows(
        IllegalArgumentException.class,
        () -> patientFilter.and(observationFilter));

    assertTrue(exception.getMessage().contains("Patient"));
    assertTrue(exception.getMessage().contains("Observation"));
  }

  @Test
  void or_throwsExceptionForDifferentResourceTypes() {
    final ResourceFilter patientFilter = new ResourceFilter(
        ResourceType.PATIENT, lit(true));
    final ResourceFilter conditionFilter = new ResourceFilter(
        ResourceType.CONDITION, lit(true));

    final IllegalArgumentException exception = assertThrows(
        IllegalArgumentException.class,
        () -> patientFilter.or(conditionFilter));

    assertTrue(exception.getMessage().contains("Patient"));
    assertTrue(exception.getMessage().contains("Condition"));
  }

  @Test
  void apply_withTrueFilter_returnsAllRows() {
    final Dataset<Row> dataset = createPatientDataSource().read("Patient");

    // Filter that always returns true
    final ResourceFilter filter = new ResourceFilter(ResourceType.PATIENT, lit(true));

    final Dataset<Row> result = filter.apply(dataset);

    assertEquals(4, result.count());
  }

  @Test
  void apply_withFalseFilter_returnsNoRows() {
    final Dataset<Row> dataset = createPatientDataSource().read("Patient");

    // Filter that always returns false
    final ResourceFilter filter = new ResourceFilter(ResourceType.PATIENT, lit(false));

    final Dataset<Row> result = filter.apply(dataset);

    assertEquals(0, result.count());
  }

  @Test
  void composedFilter_and_appliesCorrectly() {
    final Dataset<Row> dataset = createPatientDataSourceWithActive().read("Patient");

    // Create filter for male AND active patients
    final ResourceFilter maleFilter = new ResourceFilter(
        ResourceType.PATIENT, col("Patient.gender").equalTo(lit("male")));
    final ResourceFilter activeFilter = new ResourceFilter(
        ResourceType.PATIENT, col("Patient.active").equalTo(lit(true)));

    final ResourceFilter combined = maleFilter.and(activeFilter);
    final Dataset<Row> result = combined.apply(dataset);

    // Should return only Patient 1 (male AND active)
    final Set<String> resultIds = extractIds(result);
    assertEquals(Set.of("1"), resultIds);
  }

  @Test
  void composedFilter_or_appliesCorrectly() {
    final Dataset<Row> dataset = createPatientDataSource().read("Patient");

    // Create filter for male OR female patients (should include patients 1, 2, 3)
    final ResourceFilter maleFilter = new ResourceFilter(
        ResourceType.PATIENT, col("Patient.gender").equalTo(lit("male")));
    final ResourceFilter femaleFilter = new ResourceFilter(
        ResourceType.PATIENT, col("Patient.gender").equalTo(lit("female")));

    final ResourceFilter combined = maleFilter.or(femaleFilter);
    final Dataset<Row> result = combined.apply(dataset);

    // Should return patients 1, 2, 3 (all who have gender set)
    final Set<String> resultIds = extractIds(result);
    assertEquals(Set.of("1", "2", "3"), resultIds);
  }

  @Test
  void composedFilter_not_appliesCorrectly() {
    final Dataset<Row> dataset = createPatientDataSource().read("Patient");

    // Create filter for NOT male patients
    // Note: NOT(male) only returns true when gender is explicitly not 'male'
    // It does NOT include null values since NOT(null) = null, not true
    final ResourceFilter maleFilter = new ResourceFilter(
        ResourceType.PATIENT, col("Patient.gender").equalTo(lit("male")));

    final ResourceFilter notMaleFilter = maleFilter.not();
    final Dataset<Row> result = notMaleFilter.apply(dataset);

    // Should return patient 2 (female) only
    // Patient 4 (null gender) is excluded because NOT(null == 'male') = NOT(null) = null
    final Set<String> resultIds = extractIds(result);
    assertEquals(Set.of("2"), resultIds);
  }

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
    patient2.setActive(true);

    final Patient patient3 = new Patient();
    patient3.setId("3");
    patient3.setGender(AdministrativeGender.MALE);
    patient3.setActive(false);

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
