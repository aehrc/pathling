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

package au.csiro.pathling.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import au.csiro.pathling.config.StorageConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.sink.FileInformation;
import au.csiro.pathling.library.io.sink.WriteDetails;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.util.FhirServerTestConfiguration;
import au.csiro.pathling.util.TestDataSetup;
import au.csiro.pathling.views.FhirView;
import jakarta.annotation.Nonnull;
import java.nio.file.Path;
import java.util.Set;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;

/**
 * Unit tests for {@link DerivedSource}, the source produced by {@code map}, {@code
 * filterByResourceType} and {@code cache} on a {@link DriftGuardedSource}.
 *
 * <p>Every test creates the resource type's table <b>after</b> the parent source is constructed,
 * because that is the case the derivation used to get wrong: a source derived eagerly over the
 * parent's startup resource map could not see a type discovered later, so a filtered read failed
 * while the equivalent unfiltered read succeeded.
 *
 * @author John Grimes
 */
@Import(FhirServerTestConfiguration.class)
@SpringBootUnitTest
class DerivedSourceTest {

  /** A resource type that has no table in any of the warehouses used here. */
  private static final String ABSENT_TYPE = "ImmunizationEvaluation";

  /** The number of Patient resources in the copied test table. */
  private static final long PATIENT_COUNT = 125;

  @Autowired private SparkSession sparkSession;

  @Autowired private PathlingContext pathlingContext;

  @Autowired private FhirEncoders fhirEncoders;

  // A source derived through map() must read a type whose table appeared after the parent was
  // constructed, and must apply the row operator to what it reads (FR-001).
  @Test
  void mapDerivedReadSeesTypeCreatedAfterConstruction(@TempDir final Path tempDir) {
    final DynamicDeltaSource parent = emptyWarehouseSource(tempDir);
    createPatientTable(tempDir);

    final QueryableDataSource derived = parent.map((resourceType, dataset) -> dataset.limit(3));

    assertThat(derived.read("Patient").count()).isEqualTo(3);
    assertThat(parent.read("Patient").count()).isEqualTo(PATIENT_COUNT);
  }

  // The same for a source derived through filterByResourceType(): a retained type created after
  // construction is readable (FR-001).
  @Test
  void filterDerivedReadSeesTypeCreatedAfterConstruction(@TempDir final Path tempDir) {
    final DynamicDeltaSource parent = emptyWarehouseSource(tempDir);
    createPatientTable(tempDir);

    final QueryableDataSource derived = parent.filterByResourceType("Patient"::equals);

    assertThat(derived.read("Patient").count()).isEqualTo(PATIENT_COUNT);
  }

  // A type removed by the predicate keeps the failure behaviour it has today, so that compartment
  // semantics are not silently changed by this fix (FR-009).
  @Test
  void filterDerivedReadOfExcludedTypeFails(@TempDir final Path tempDir) {
    final DynamicDeltaSource parent = emptyWarehouseSource(tempDir);
    createPatientTable(tempDir);

    final QueryableDataSource derived = parent.filterByResourceType(resourceType -> false);

    assertThatThrownBy(() -> derived.read("Patient"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("No data found for resource type: Patient");
  }

  // Derivation composes: chaining map, filterByResourceType and map again applies every stage to a
  // type discovered after construction (FR-002).
  @Test
  void chainedDerivationAppliesEveryStage(@TempDir final Path tempDir) {
    final DynamicDeltaSource parent = emptyWarehouseSource(tempDir);
    createPatientTable(tempDir);

    final QueryableDataSource derived =
        parent
            .map((resourceType, dataset) -> dataset.limit(10))
            .filterByResourceType("Patient"::equals)
            .map((resourceType, dataset) -> dataset.limit(4));

    assertThat(derived.read("Patient").count()).isEqualTo(4);
    // The excluded type is still excluded further down the chain.
    assertThatThrownBy(() -> derived.read("Observation"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  // A derived read of a type with no table anywhere returns an empty dataset with the type's
  // schema, exactly as the unfiltered read does, rather than failing (FR-003).
  @Test
  void derivedReadOfAbsentTypeReturnsEmptyDataset(@TempDir final Path tempDir) {
    final DynamicDeltaSource parent = emptyWarehouseSource(tempDir);

    final Dataset<Row> result = parent.map((resourceType, dataset) -> dataset).read(ABSENT_TYPE);

    assertThat(result.count()).isZero();
    assertThat(result.schema().fieldNames()).contains("id", "status");
  }

  // Deriving a source must not weaken the schema drift guard: a drifted, unmigrated type still
  // fails with the actionable error through any derivation chain (FR-004).
  @Test
  void derivedReadOfDriftedTypeThrowsSchemaDriftError(@TempDir final Path tempDir) {
    final DynamicDeltaSource parent = emptyWarehouseSource(tempDir, false, Set.of("Patient"));
    createPatientTable(tempDir);

    final QueryableDataSource derived =
        parent.map((resourceType, dataset) -> dataset).filterByResourceType(resourceType -> true);

    assertThatThrownBy(() -> derived.read("Patient")).isInstanceOf(SchemaDriftError.class);
    // Constructing a view over a drifted subject fails up front, as it does on the parent.
    assertThatThrownBy(() -> derived.view("Patient")).isInstanceOf(SchemaDriftError.class);
  }

  // A derived source enumerates its parent's types minus the ones its predicates removed (FR-007).
  @Test
  void derivedResourceTypesAreParentTypesMinusExcluded(@TempDir final Path tempDir) {
    final DynamicDeltaSource parent = emptyWarehouseSource(tempDir);
    createPatientTable(tempDir);
    createTable(tempDir, "Observation");
    // Reading brings both post-startup types into the parent's enumeration through dynamic
    // discovery; enumeration of never-read tables is covered separately in DynamicDeltaSourceTest.
    parent.read("Patient");
    parent.read("Observation");

    assertThat(parent.map((resourceType, dataset) -> dataset).getResourceTypes())
        .containsExactlyInAnyOrder("Patient", "Observation");
    assertThat(parent.filterByResourceType("Patient"::equals).getResourceTypes())
        .containsExactly("Patient");
  }

  // cache() is a derivation like any other, so it must also see a post-startup type, and must
  // still mark the datasets it serves for caching (FR-001).
  @Test
  void cacheDerivationReadsPostStartupTypeAndMarksForCaching(@TempDir final Path tempDir) {
    final DynamicDeltaSource parent = emptyWarehouseSource(tempDir);
    createPatientTable(tempDir);

    final Dataset<Row> result = ((QueryableDataSource) parent.cache()).read("Patient");

    try {
      assertThat(result.count()).isEqualTo(PATIENT_COUNT);
      assertThat(result.storageLevel()).isEqualTo(StorageLevel.MEMORY_AND_DISK());
    } finally {
      result.unpersist();
    }
  }

  // A write through a derived source enumerates and reads through that source, so the output
  // carries the derivation's transformations and only its retained types.
  @Test
  void writeThroughDerivedSourceWritesTransformedDatasets(
      @TempDir final Path tempDir, @TempDir final Path outputDir) {
    final DynamicDeltaSource parent = emptyWarehouseSource(tempDir);
    createPatientTable(tempDir);
    createTable(tempDir, "Observation");
    parent.read("Patient");
    parent.read("Observation");

    final WriteDetails written =
        parent
            .filterByResourceType("Patient"::equals)
            .map((resourceType, dataset) -> dataset.limit(2))
            .write()
            .saveMode("overwrite")
            .ndjson(outputDir.toAbsolutePath().toString());

    // Only the retained type is written, and its file carries the row limit the derivation
    // applied.
    assertThat(written.fileInfos())
        .extracting(FileInformation::fhirResourceType)
        .containsOnly("Patient");
    final long lines =
        written.fileInfos().stream()
            .mapToLong(info -> sparkSession.read().text(info.absoluteUrl()).count())
            .sum();
    assertThat(lines).isEqualTo(2);
  }

  // A view over a derived source resolves its subject through the derived read, so it too sees a
  // post-startup type and observes the derivation's row filtering.
  @Test
  void viewOverDerivedSourceResolvesThroughDerivedRead(@TempDir final Path tempDir) {
    final DynamicDeltaSource parent = emptyWarehouseSource(tempDir);
    createPatientTable(tempDir);
    final FhirView view =
        FhirView.ofResource("Patient")
            .select(FhirView.columns(FhirView.column("id", "id")))
            .build();

    final Dataset<Row> result =
        parent.map((resourceType, dataset) -> dataset.limit(5)).view(view).execute();

    assertThat(result.count()).isEqualTo(5);
    assertThat(result.schema().fieldNames()).containsExactly("id");
  }

  // ---- helpers ----

  /** Builds a live source over an empty warehouse, so every table used is created after it. */
  @Nonnull
  private DynamicDeltaSource emptyWarehouseSource(@Nonnull final Path databaseDir) {
    return emptyWarehouseSource(databaseDir, false, Set.of());
  }

  @Nonnull
  private DynamicDeltaSource emptyWarehouseSource(
      @Nonnull final Path databaseDir,
      final boolean cacheDatasets,
      @Nonnull final Set<String> driftedTypes) {
    final String databasePath = databaseDir.toAbsolutePath().toString();
    final StorageConfiguration storageConfiguration = new StorageConfiguration();
    storageConfiguration.setCacheDatasets(cacheDatasets);
    final QueryableDataSource baseSource = pathlingContext.read().delta(databasePath);
    return new DynamicDeltaSource(
        pathlingContext,
        baseSource,
        sparkSession,
        databasePath,
        fhirEncoders,
        storageConfiguration,
        driftedTypes);
  }

  /** Creates the Patient table in the warehouse, from the encoded test data. */
  private void createPatientTable(@Nonnull final Path databaseDir) {
    TestDataSetup.copyTestDataToTempDir(databaseDir, "Patient");
  }

  /** Creates a table for another resource type in the warehouse. */
  private void createTable(@Nonnull final Path databaseDir, @Nonnull final String resourceType) {
    TestDataSetup.copyTestDataToTempDir(databaseDir, resourceType);
  }
}
