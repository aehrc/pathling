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

package au.csiro.pathling.operations.export;

import static org.assertj.core.api.Assertions.assertThat;

import au.csiro.pathling.config.StorageConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.io.DynamicDeltaSource;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.operations.compartment.PatientCompartmentService;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.util.FhirServerTestConfiguration;
import au.csiro.pathling.util.TestDataSetup;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import java.nio.file.Path;
import java.util.Set;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.InstantType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;

/**
 * Tests the export filter chain built by {@link ExportDataSourceBuilder} over a live source whose
 * Patient table appeared after the source was constructed.
 *
 * <p>This is the same defect as the filtered {@code $sql-run}, reached at the export boundary: the
 * {@code _since} map and the patient-compartment filter each derive a source, and a derivation that
 * captured the parent's startup resource map could not see the table. These are regression tests -
 * the fix is in the derivation itself, not in this builder.
 *
 * @author John Grimes
 */
@Import(FhirServerTestConfiguration.class)
@SpringBootUnitTest
class ExportDataSourceBuilderDynamicTest {

  @Autowired private SparkSession sparkSession;

  @Autowired private PathlingContext pathlingContext;

  @Autowired private FhirEncoders fhirEncoders;

  @Autowired private FhirContext fhirContext;

  // The _since filter alone derives the source, so it too must see the post-startup table.
  @Test
  void sinceFilteredSourceSeesPostStartupTable(@TempDir final Path tempDir) {
    final DynamicDeltaSource base = emptyWarehouseSource(tempDir);
    TestDataSetup.copyTestDataToTempDir(tempDir, "Patient");

    final QueryableDataSource filtered =
        builder().build(base, new InstantType("2000-01-01T00:00:00Z"), Set.of());

    assertThat(filtered.read("Patient").count()).isPositive();
  }

  // The patient-compartment filter narrows to the named patient, over a table the source did not
  // know about when it was built.
  @Test
  void patientFilteredSourceSeesPostStartupTable(@TempDir final Path tempDir) {
    final DynamicDeltaSource base = emptyWarehouseSource(tempDir);
    TestDataSetup.copyTestDataToTempDir(tempDir, "Patient");
    final String patientId = base.read("Patient").select("id").first().getString(0);

    final QueryableDataSource filtered = builder().build(base, null, Set.of(patientId));

    assertThat(filtered.read("Patient").count()).isEqualTo(1);
  }

  // A patient id that matches nothing yields an empty result rather than a failure: the type is
  // visible, the filter simply removes every row.
  @Test
  void patientFilterWithNoMatchesYieldsEmptyResult(@TempDir final Path tempDir) {
    final DynamicDeltaSource base = emptyWarehouseSource(tempDir);
    TestDataSetup.copyTestDataToTempDir(tempDir, "Patient");

    final QueryableDataSource filtered = builder().build(base, null, Set.of("no-such-patient"));

    assertThat(filtered.read("Patient").count()).isZero();
  }

  // Both filters together compose over the post-startup table.
  @Test
  void chainedFiltersSeePostStartupTable(@TempDir final Path tempDir) {
    final DynamicDeltaSource base = emptyWarehouseSource(tempDir);
    TestDataSetup.copyTestDataToTempDir(tempDir, "Patient");
    final String patientId = base.read("Patient").select("id").first().getString(0);

    final QueryableDataSource filtered =
        builder().build(base, new InstantType("2000-01-01T00:00:00Z"), Set.of(patientId));

    assertThat(filtered.read("Patient").count()).isEqualTo(1);
  }

  // ---- helpers ----

  @Nonnull
  private ExportDataSourceBuilder builder() {
    return new ExportDataSourceBuilder(new PatientCompartmentService(fhirContext));
  }

  /** Builds a live source over an empty warehouse, so the Patient table is created after it. */
  @Nonnull
  private DynamicDeltaSource emptyWarehouseSource(@Nonnull final Path databaseDir) {
    final String databasePath = databaseDir.toAbsolutePath().toString();
    final StorageConfiguration storageConfiguration = new StorageConfiguration();
    storageConfiguration.setCacheDatasets(false);
    final QueryableDataSource baseSource = pathlingContext.read().delta(databasePath);
    return new DynamicDeltaSource(
        pathlingContext,
        baseSource,
        sparkSession,
        databasePath,
        fhirEncoders,
        storageConfiguration,
        Set.of());
  }
}
