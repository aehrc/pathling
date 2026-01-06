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
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBaseResource;
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
    final List<IBaseResource> patients = new ArrayList<>();

    for (int i = 0; i < 50; i++) {
      final Patient patient = new Patient();
      patient.setId("patient-" + i);
      patient.setGender(i % 2 == 0 ? AdministrativeGender.MALE : AdministrativeGender.FEMALE);
      patient.setActive(i % 3 != 0);
      patient.addName().setFamily("Family" + i).addGiven("Given" + i);
      patients.add(patient);
    }

    dataSource = new CustomObjectDataSource(sparkSession, pathlingContext, fhirEncoders, patients);
  }

  @Test
  void searchWithoutFilters() {
    // When: search without any filters.
    final IBundleProvider result =
        new SearchExecutor(
            fhirContext, dataSource, fhirEncoders, "Patient", Optional.empty(), false);

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
            fhirContext, dataSource, fhirEncoders, "Patient", Optional.of(filters), false);

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
            fhirContext, dataSource, fhirEncoders, "Patient", Optional.of(filters), false);

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
            fhirContext, dataSource, fhirEncoders, "Patient", Optional.of(filters), false);

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
            fhirContext, dataSource, fhirEncoders, "Patient", Optional.empty(), false);

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

    assertThat(firstPageIds).doesNotContainAnyElementsOf(secondPageIds);
  }

  @Test
  void filterOnNonExistentElementReturnsEmptyResult() {
    // Given: a filter referencing an element that doesn't exist on Patient.
    final StringAndListParam filters = new StringAndListParam();
    filters.addAnd(new StringParam("nonExistentElement = 'value'"));

    // When: search with filter.
    final IBundleProvider result =
        new SearchExecutor(
            fhirContext, dataSource, fhirEncoders, "Patient", Optional.of(filters), false);

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
            fhirContext, dataSource, fhirEncoders, "Patient", Optional.of(filters), false);

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
                    fhirContext, dataSource, fhirEncoders, "Patient", Optional.of(filters), false))
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
            Optional.of(filters),
            true // cacheResults enabled
            );

    // Then: results are still correctly returned (25 male patients).
    assertThat(result.size()).isEqualTo(25);
    final List<IBaseResource> resources = result.getResources(0, result.size());
    assertThat(resources).hasSize(25);
  }
}
