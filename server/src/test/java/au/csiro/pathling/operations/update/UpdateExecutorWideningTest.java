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

package au.csiro.pathling.operations.update;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import au.csiro.pathling.cache.CacheableDatabase;
import au.csiro.pathling.io.DynamicDeltaSource;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.read.ReadExecutor;
import au.csiro.pathling.search.SearchExecutor;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.util.FhirServerTestConfiguration;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.param.StringAndListParam;
import ca.uhn.fhir.rest.param.StringParam;
import jakarta.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.AdministrativeGender;
import org.hl7.fhir.r4.model.Patient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;

/**
 * Tests that an update can introduce an element the stored table has never held.
 *
 * <p>Stored tables carry only the elements their data populates, so an update that brings a new
 * element must widen the table rather than fail, and the dataset served for the type must be
 * refreshed so that read and search see the new element.
 *
 * @author John Grimes
 */
@Import(FhirServerTestConfiguration.class)
@SpringBootUnitTest
class UpdateExecutorWideningTest {

  private static final String MARITAL_STATUS_SYSTEM =
      "http://terminology.hl7.org/CodeSystem/v3-MaritalStatus";

  @Autowired private PathlingContext pathlingContext;

  @Autowired private FhirContext fhirContext;

  @Autowired private CacheableDatabase cacheableDatabase;

  private Path tempDatabasePath;

  private DynamicDeltaSource servedSource;

  private UpdateExecutor executor;

  @BeforeEach
  void setUp() throws IOException {
    tempDatabasePath = Files.createTempDirectory("update-widening-test-");
    servedSource = mock(DynamicDeltaSource.class);
    executor =
        new UpdateExecutor(
            pathlingContext,
            tempDatabasePath.toAbsolutePath().toString(),
            cacheableDatabase,
            servedSource);
  }

  @AfterEach
  void tearDown() throws IOException {
    if (tempDatabasePath != null && Files.exists(tempDatabasePath)) {
      try (final var paths = Files.walk(tempDatabasePath)) {
        paths.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
      }
    }
  }

  @Test
  void updateIntroducingNewElementWidensTableAndIsReadableAndSearchable() {
    // The table starts out holding only gender, so it has no maritalStatus column.
    executor.merge("Patient", patient("p1", null));
    assertThat(storedColumns()).contains("gender").doesNotContain("maritalStatus");
    clearInvocations(servedSource);

    // An update bringing maritalStatus succeeds, rather than being refused for a schema mismatch.
    executor.merge("Patient", patient("p2", "M"));
    assertThat(storedColumns()).contains("gender", "maritalStatus");

    // The table schema changed, so the served dataset is refreshed to expose the new column.
    verify(servedSource).refresh("Patient");

    // The new element is readable through read.
    final DataSource stored = pathlingContext.read().delta(tempDatabasePath.toString());
    final Patient read = (Patient) new ReadExecutor(stored, pathlingContext).read("Patient", "p2");
    assertThat(read.getMaritalStatus().getCodingFirstRep().getCode()).isEqualTo("M");

    // The resource stored before the table widened is still there, without the new element.
    final Patient earlier =
        (Patient) new ReadExecutor(stored, pathlingContext).read("Patient", "p1");
    assertThat(earlier.hasMaritalStatus()).isFalse();
    assertThat(earlier.getGender()).isEqualTo(AdministrativeGender.FEMALE);

    // The new element is searchable through a FHIRPath filter.
    final StringAndListParam filters = new StringAndListParam();
    filters.addAnd(new StringParam("maritalStatus.coding.code = 'M'"));
    assertThat(searchIds(stored, filters)).containsExactly("p2");
  }

  @Test
  void updateWithoutSchemaChangeDoesNotRefresh() {
    // Writing resources that bring no new elements leaves the schema, and the served dataset, as
    // they were.
    executor.merge("Patient", patient("p1", "M"));
    clearInvocations(servedSource);

    executor.merge("Patient", patient("p2", "S"));

    verify(servedSource, never()).refresh("Patient");
  }

  @Nonnull
  private List<String> storedColumns() {
    return Arrays.asList(
        pathlingContext
            .getSpark()
            .read()
            .format("delta")
            .load(tempDatabasePath.resolve("Patient.parquet").toString())
            .columns());
  }

  @Nonnull
  private List<String> searchIds(
      @Nonnull final DataSource source, @Nonnull final StringAndListParam filters) {
    final SearchExecutor search =
        new SearchExecutor(
            fhirContext, source, pathlingContext, "Patient", null, Optional.of(filters), false);
    return search.getResources(0, search.size()).stream()
        .map(resource -> resource.getIdElement().getIdPart())
        .toList();
  }

  @Nonnull
  private static Patient patient(@Nonnull final String id, final String maritalStatus) {
    final Patient patient = new Patient();
    patient.setId(id);
    patient.setGender(AdministrativeGender.FEMALE);
    if (maritalStatus != null) {
      patient.setMaritalStatus(
          new CodeableConcept(new Coding(MARITAL_STATUS_SYSTEM, maritalStatus, null)));
    }
    return patient;
  }
}
