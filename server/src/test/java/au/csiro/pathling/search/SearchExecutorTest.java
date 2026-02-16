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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.util.CustomObjectDataSource;
import au.csiro.pathling.util.FhirServerTestConfiguration;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.api.server.IBundleProvider;
import ca.uhn.fhir.rest.param.StringAndListParam;
import ca.uhn.fhir.rest.param.StringOrListParam;
import ca.uhn.fhir.rest.param.StringParam;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.DateType;
import org.hl7.fhir.r4.model.Enumerations.AdministrativeGender;
import org.hl7.fhir.r4.model.Patient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;

/**
 * Unit tests for SearchExecutor.
 *
 * @author John Grimes
 */
@Slf4j
@Import(FhirServerTestConfiguration.class)
@SpringBootUnitTest
class SearchExecutorTest {

  @Autowired private FhirContext fhirContext;

  @Autowired private SparkSession sparkSession;

  @Autowired private PathlingContext pathlingContext;

  @Autowired private FhirEncoders fhirEncoders;

  private CustomObjectDataSource dataSource;

  @BeforeEach
  void setUp() {
    // Create test patients with varied attributes for filtering tests.
    // Patients 0-24 have even indices (male), patients 1-49 have odd indices (female).
    // Patients 0-24 are born in 1980, patients 25-49 are born in 2000.
    final List<IBaseResource> patients = new ArrayList<>();

    for (int i = 0; i < 50; i++) {
      final Patient patient = new Patient();
      patient.setId("patient-" + i);
      patient.setGender(i % 2 == 0 ? AdministrativeGender.MALE : AdministrativeGender.FEMALE);
      patient.setActive(i % 3 != 0);
      patient.addName().setFamily("Family" + i).addGiven("Given" + i);
      // First 25 patients born in 1980, rest in 2000.
      patient.setBirthDateElement(new DateType(i < 25 ? "1980-01-01" : "2000-01-01"));
      patients.add(patient);
    }

    dataSource = new CustomObjectDataSource(sparkSession, pathlingContext, fhirEncoders, patients);
  }

  @Test
  void searchWithoutFilters() {
    // When: search without any filters.
    final IBundleProvider result =
        new SearchExecutor(
            fhirContext, dataSource, fhirEncoders, "Patient", null, Optional.empty(), false);

    // Then: returns all Patient resources.
    assertThat(result.size()).isEqualTo(50);
    final List<IBaseResource> resources = result.getResources(0, result.size());
    assertThat(resources).hasSize(50);
    assertThat(resources).allSatisfy(r -> assertThat(r).isInstanceOf(Patient.class));
  }

  @Test
  void searchWithSimpleFilter() {
    // Given: a filter for male patients.
    final StringAndListParam filters = new StringAndListParam();
    filters.addAnd(new StringParam("gender = 'male'"));

    // When: search with filter.
    final IBundleProvider result =
        new SearchExecutor(
            fhirContext, dataSource, fhirEncoders, "Patient", null, Optional.of(filters), false);

    // Then: returns only male patients (25 of 50).
    assertThat(result.size()).isEqualTo(25);
    final List<IBaseResource> resources = result.getResources(0, result.size());
    assertThat(resources).hasSize(25);
    assertThat(resources)
        .allSatisfy(
            r -> {
              assertThat(r).isInstanceOf(Patient.class);
              assertThat(((Patient) r).getGender().toCode()).isEqualTo("male");
            });
  }

  @Test
  void searchWithAndFilters() {
    // Given: AND conditions (multiple filter parameters).
    final StringAndListParam filters = new StringAndListParam();
    filters.addAnd(new StringParam("gender = 'female'"));
    filters.addAnd(new StringParam("active = true"));

    // When: search with AND filters.
    final IBundleProvider result =
        new SearchExecutor(
            fhirContext, dataSource, fhirEncoders, "Patient", null, Optional.of(filters), false);

    // Then: returns patients matching both conditions (17 female AND active).
    assertThat(result.size()).isEqualTo(17);
    final List<IBaseResource> resources = result.getResources(0, result.size());
    assertThat(resources).hasSize(17);
    assertThat(resources)
        .allSatisfy(
            r -> {
              assertThat(r).isInstanceOf(Patient.class);
              final Patient patient = (Patient) r;
              assertThat(patient.getGender().toCode()).isEqualTo("female");
              assertThat(patient.getActive()).isTrue();
            });
  }

  @Test
  void searchWithOrFilters() {
    // Given: OR conditions (comma-separated in same parameter).
    final StringAndListParam filters = new StringAndListParam();
    final StringOrListParam orList = new StringOrListParam();
    orList.addOr(new StringParam("gender = 'male'"));
    orList.addOr(new StringParam("gender = 'female'"));
    filters.addAnd(orList);

    // When: search with OR filters.
    final IBundleProvider result =
        new SearchExecutor(
            fhirContext, dataSource, fhirEncoders, "Patient", null, Optional.of(filters), false);

    // Then: returns patients matching either condition (all 50).
    assertThat(result.size()).isEqualTo(50);
    final List<IBaseResource> resources = result.getResources(0, result.size());
    assertThat(resources).hasSize(50);
    assertThat(resources)
        .allSatisfy(
            r -> {
              assertThat(r).isInstanceOf(Patient.class);
              final Patient patient = (Patient) r;
              assertThat(patient.getGender().toCode()).isIn("male", "female");
            });
  }

  @Test
  void searchWithPagination() {
    // Given: a search that returns 50 patients.
    final IBundleProvider result =
        new SearchExecutor(
            fhirContext, dataSource, fhirEncoders, "Patient", null, Optional.empty(), false);

    // When: retrieving pages of 10.
    final int pageSize = 10;
    final List<IBaseResource> firstPage = result.getResources(0, pageSize);
    final List<IBaseResource> secondPage = result.getResources(pageSize, pageSize * 2);

    // Then: pages contain different resources.
    assertThat(firstPage).hasSize(pageSize);
    assertThat(secondPage).hasSize(pageSize);

    // Extract IDs to verify different resources.
    final List<String> firstPageIds =
        firstPage.stream().map(r -> ((Patient) r).getIdElement().getIdPart()).toList();
    final List<String> secondPageIds =
        secondPage.stream().map(r -> ((Patient) r).getIdElement().getIdPart()).toList();

    assertThat(firstPageIds).isNotEmpty().doesNotContainAnyElementsOf(secondPageIds);
  }

  @Test
  void filterOnNonExistentElementReturnsEmptyResult() {
    // Given: a filter referencing an element that doesn't exist on Patient.
    final StringAndListParam filters = new StringAndListParam();
    filters.addAnd(new StringParam("nonExistentElement = 'value'"));

    // When: search with filter.
    final IBundleProvider result =
        new SearchExecutor(
            fhirContext, dataSource, fhirEncoders, "Patient", null, Optional.of(filters), false);

    // Then: returns empty result (not an error).
    assertThat(result.size()).isEqualTo(0);
  }

  @Test
  void nonBooleanFilterUsesTruthySemantics() {
    // Given: a filter that doesn't evaluate to Boolean (string collection).
    final StringAndListParam filters = new StringAndListParam();
    filters.addAnd(new StringParam("name.given"));

    // When: search with filter.
    final IBundleProvider result =
        new SearchExecutor(
            fhirContext, dataSource, fhirEncoders, "Patient", null, Optional.of(filters), false);

    // Then: returns patients with given names (truthy semantics: non-empty = true).
    assertThat(result.size()).isEqualTo(50);
  }

  @Test
  void throwsInvalidInputOnEmptyFilter() {
    // Given: a blank filter expression.
    final StringAndListParam filters = new StringAndListParam();
    filters.addAnd(new StringParam(""));

    // When/Then: construction throws InvalidUserInputError.
    assertThatThrownBy(
            () ->
                new SearchExecutor(
                    fhirContext,
                    dataSource,
                    fhirEncoders,
                    "Patient",
                    null,
                    Optional.of(filters),
                    false))
        .isInstanceOf(InvalidUserInputError.class)
        .hasMessageContaining("Filter expression cannot be blank");
  }

  @Test
  void searchWithCaching() {
    // Given: caching enabled.
    final StringAndListParam filters = new StringAndListParam();
    filters.addAnd(new StringParam("gender = 'male'"));

    // When: search with caching.
    final IBundleProvider result =
        new SearchExecutor(
            fhirContext,
            dataSource,
            fhirEncoders,
            "Patient",
            null,
            Optional.of(filters),
            true // cacheResults enabled
            );

    // Then: results are still correctly returned (25 male patients).
    assertThat(result.size()).isEqualTo(25);
    final List<IBaseResource> resources = result.getResources(0, result.size());
    assertThat(resources).hasSize(25);
  }

  // ========== Standard search parameter tests (tasks 1.1-1.6) ==========

  @Test
  void standardSearchByTokenParameter() {
    // Verifies that a standard token search parameter produces correct filtered results.
    final IBundleProvider result =
        new SearchExecutor(
            fhirContext,
            dataSource,
            fhirEncoders,
            "Patient",
            "gender=male",
            Optional.empty(),
            false);

    // 25 male patients (even indices: 0, 2, 4, ..., 48).
    assertThat(result.size()).isEqualTo(25);
    final List<IBaseResource> resources = result.getResources(0, result.size());
    assertThat(resources)
        .allSatisfy(r -> assertThat(((Patient) r).getGender().toCode()).isEqualTo("male"));
  }

  @Test
  void standardSearchByDateParameter() {
    // Verifies that a standard date search parameter with a prefix produces correct results.
    final IBundleProvider result =
        new SearchExecutor(
            fhirContext,
            dataSource,
            fhirEncoders,
            "Patient",
            "birthdate=ge1990-01-01",
            Optional.empty(),
            false);

    // 25 patients born in 2000 (indices 25-49).
    assertThat(result.size()).isEqualTo(25);
    final List<IBaseResource> resources = result.getResources(0, result.size());
    assertThat(resources)
        .allSatisfy(
            r -> {
              final Date birthDate = ((Patient) r).getBirthDate();
              assertThat(birthDate).isAfterOrEqualTo("1990-01-01");
            });
  }

  @Test
  void standardSearchByStringParameter() {
    // Verifies that a standard string search parameter (prefix match) works correctly.
    // Patient 0 has family name "Family0". The "family" search parameter has a simple string
    // expression (Patient.name.family) that the string matcher can handle directly.
    final IBundleProvider result =
        new SearchExecutor(
            fhirContext,
            dataSource,
            fhirEncoders,
            "Patient",
            "family=Family0",
            Optional.empty(),
            false);

    // "Family0" should match "Family0" (exact prefix match).
    assertThat(result.size()).isEqualTo(1);
  }

  @Test
  void standardSearchAndCombiningSemanticsRepeatedParameters() {
    // Verifies AND logic between different parameters: gender=male AND birthdate>=1990.
    final IBundleProvider result =
        new SearchExecutor(
            fhirContext,
            dataSource,
            fhirEncoders,
            "Patient",
            "gender=male&birthdate=ge1990-01-01",
            Optional.empty(),
            false);

    // Male patients born on or after 1990 (indices 26, 28, 30, ..., 48 = 12 patients).
    assertThat(result.size()).isEqualTo(12);
    final List<IBaseResource> resources = result.getResources(0, result.size());
    assertThat(resources)
        .allSatisfy(
            r -> {
              final Patient patient = (Patient) r;
              assertThat(patient.getGender().toCode()).isEqualTo("male");
              assertThat(patient.getBirthDate()).isAfterOrEqualTo("1990-01-01");
            });
  }

  @Test
  void standardSearchOrCombiningSemanticsCommaSeparatedValues() {
    // Verifies OR logic within comma-separated values: gender=male,female.
    final IBundleProvider result =
        new SearchExecutor(
            fhirContext,
            dataSource,
            fhirEncoders,
            "Patient",
            "gender=male,female",
            Optional.empty(),
            false);

    // All 50 patients match male or female.
    assertThat(result.size()).isEqualTo(50);
  }

  @Test
  void combinedStandardAndFhirPathFilters() {
    // Verifies that standard parameters and FHIRPath filters are combined with AND logic.
    final StringAndListParam filters = new StringAndListParam();
    filters.addAnd(new StringParam("active = true"));

    final IBundleProvider result =
        new SearchExecutor(
            fhirContext,
            dataSource,
            fhirEncoders,
            "Patient",
            "gender=male",
            Optional.of(filters),
            false);

    // Male patients (25) that are also active (active = i % 3 != 0).
    // Male indices: 0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36,
    // 38, 40, 42, 44, 46, 48.
    // Of these, not active (i % 3 == 0): 0, 6, 12, 18, 24, 30, 36, 42, 48 = 9 not active.
    // So 25 - 9 = 16 active male patients.
    assertThat(result.size()).isEqualTo(16);
    final List<IBaseResource> resources = result.getResources(0, result.size());
    assertThat(resources)
        .allSatisfy(
            r -> {
              final Patient patient = (Patient) r;
              assertThat(patient.getGender().toCode()).isEqualTo("male");
              assertThat(patient.getActive()).isTrue();
            });
  }

  @Test
  void fhirPathOnlySearchBackwardsCompatibility() {
    // Verifies that FHIRPath-only search (without standard params) still works identically.
    final StringAndListParam filters = new StringAndListParam();
    filters.addAnd(new StringParam("gender = 'male'"));
    filters.addAnd(new StringParam("active = true"));

    final IBundleProvider result =
        new SearchExecutor(
            fhirContext, dataSource, fhirEncoders, "Patient", null, Optional.of(filters), false);

    // Same as before: male AND active patients = 16.
    assertThat(result.size()).isEqualTo(16);
  }

  @Test
  void unknownStandardParameterThrowsInvalidUserInputError() {
    // Verifies that an unknown search parameter throws InvalidUserInputError.
    assertThatThrownBy(
            () ->
                new SearchExecutor(
                    fhirContext,
                    dataSource,
                    fhirEncoders,
                    "Patient",
                    "unknownparam=value",
                    Optional.empty(),
                    false))
        .isInstanceOf(InvalidUserInputError.class)
        .hasMessageContaining("unknownparam");
  }

  @Test
  void standardSearchWithEmptyQueryStringReturnsAllResources() {
    // Verifies that an empty standard query string returns all resources.
    final IBundleProvider result =
        new SearchExecutor(
            fhirContext, dataSource, fhirEncoders, "Patient", "", Optional.empty(), false);

    assertThat(result.size()).isEqualTo(50);
  }
}
