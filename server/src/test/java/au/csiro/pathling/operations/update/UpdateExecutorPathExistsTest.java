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
import static org.assertj.core.api.Assertions.assertThatNoException;

import au.csiro.pathling.cache.CacheableDatabase;
import au.csiro.pathling.config.StorageConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.util.FhirServerTestConfiguration;
import io.delta.tables.DeltaTable;
import jakarta.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.HumanName;
import org.hl7.fhir.r4.model.Patient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;

/**
 * In-process regression tests for the create-versus-merge branch in {@link UpdateExecutor}.
 *
 * <p>Reproduces the {@code DELTA_PATH_EXISTS} failure that occurs when the target table path exists
 * on disk but is not a recognised Delta table, so {@link UpdateExecutor#merge} takes the create
 * branch yet cannot write because the directory is already present. An update must succeed
 * regardless of whether the directory was left behind by a prior request, so the create branch must
 * tolerate an existing path rather than relying on {@link
 * org.apache.spark.sql.SaveMode#ErrorIfExists}.
 *
 * @author John Grimes
 */
@Import(FhirServerTestConfiguration.class)
@SpringBootUnitTest
class UpdateExecutorPathExistsTest {

  @Autowired private PathlingContext pathlingContext;

  @Autowired private FhirEncoders fhirEncoders;

  @Autowired private CacheableDatabase cacheableDatabase;

  private Path tempDatabasePath;

  @BeforeEach
  void setUp() throws IOException {
    tempDatabasePath = Files.createTempDirectory("path-exists-test-");
  }

  @AfterEach
  void tearDown() throws IOException {
    if (tempDatabasePath != null && Files.exists(tempDatabasePath)) {
      try (final var paths = Files.walk(tempDatabasePath)) {
        paths.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
      }
    }
  }

  /**
   * A merge into a type whose target directory already exists but is not a recognised Delta table
   * must still complete the write rather than failing with {@code DELTA_PATH_EXISTS}. The directory
   * here holds parquet data with no {@code _delta_log}, so {@link DeltaTable#isDeltaTable} returns
   * false while the path exists - exactly the state that drives {@link UpdateExecutor#merge} into
   * its create branch. This guards the persistence requirement that an update recovers when the
   * target path is already present.
   */
  @Test
  void mergeWhenPathExistsButNotDeltaTable_recoversAndWrites() throws IOException {
    // Arrange: create a directory at the target table path that contains parquet data files but no
    // Delta transaction log. This is the realistic "exists but not a Delta table" state: data is
    // present so Delta refuses an ErrorIfExists write, yet there is no _delta_log so isDeltaTable
    // returns false and the create branch is taken.
    final Path tablePath = tempDatabasePath.resolve("Patient.parquet");
    pathlingContext.getSpark().range(1).write().format("parquet").save(tablePath.toString());
    assertThat(DeltaTable.isDeltaTable(pathlingContext.getSpark(), tablePath.toString()))
        .as("precondition: directory exists with data but is not a Delta table")
        .isFalse();

    final UpdateExecutor executor = newExecutor();
    final Patient patient = createPatient("path-exists-patient", "PathExists");

    // Act + Assert: the write recovers rather than failing with DELTA_PATH_EXISTS.
    assertThatNoException().isThrownBy(() -> executor.merge("Patient", patient));

    // The resource is persisted and retrievable.
    final List<Row> rows =
        pathlingContext
            .getSpark()
            .read()
            .format("delta")
            .load(tablePath.toString())
            .select("id")
            .collectAsList();
    assertThat(rows).extracting(row -> row.getString(0)).contains("path-exists-patient");
  }

  @Nonnull
  private UpdateExecutor newExecutor() {
    final StorageConfiguration storageConfiguration = new StorageConfiguration();
    storageConfiguration.setSchemaAutoMerge(true);
    return new UpdateExecutor(
        pathlingContext,
        fhirEncoders,
        tempDatabasePath.toAbsolutePath().toString(),
        cacheableDatabase,
        storageConfiguration);
  }

  @Nonnull
  private Patient createPatient(@Nonnull final String id, @Nonnull final String family) {
    final Patient patient = new Patient();
    patient.setId(id);
    patient.addName(new HumanName().setFamily(family));
    return patient;
  }
}
