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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import au.csiro.pathling.cache.CacheableDatabase;
import au.csiro.pathling.config.StorageConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.encoders.ViewDefinitionResource;
import au.csiro.pathling.encoders.ViewDefinitionResource.ColumnComponent;
import au.csiro.pathling.encoders.ViewDefinitionResource.SelectComponent;
import au.csiro.pathling.io.DynamicDeltaSource;
import au.csiro.pathling.io.SchemaDrift;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.util.DeltaSchemaFixtures;
import au.csiro.pathling.util.FhirEncoderFixtures;
import au.csiro.pathling.util.FhirServerTestConfiguration;
import au.csiro.pathling.util.LogCapture;
import ch.qos.logback.classic.Level;
import jakarta.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.DateTimeType;
import org.hl7.fhir.r4.model.Enumerations.AdministrativeGender;
import org.hl7.fhir.r4.model.Extension;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Period;
import org.hl7.fhir.r4.model.StringType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;

/**
 * In-process regression tests for the {@code schemaAutoMerge} workaround in {@link UpdateExecutor}.
 *
 * <p>Reproduces the {@code DELTA_UPDATE_SCHEMA_MISMATCH_EXPRESSION} failure that occurs when a
 * Delta table's persisted schema lags the current FHIR encoder schema in the specific way that
 * Delta's MERGE cannot reconcile: missing fields inside nested struct types. A target struct with
 * fewer fields than the corresponding source struct cannot be implicitly cast by Delta's MERGE
 * planner, even with {@code spark.databricks.delta.schema.autoMerge.enabled=true}.
 *
 * <p>Setup: a Delta table is created via {@link UpdateExecutor} so it has the full encoder schema,
 * then the {@code schemaString} in the initial Delta log commit is rewritten in place to remove the
 * {@code forEach} and {@code forEachOrNull} fields from every nested struct. The CRC side-cars are
 * deleted so checksum validation does not reject the modified commit. The result looks like a Delta
 * table that was written by an older encoder version that lacked those fields.
 *
 * <p>Replaces the container-based regression coverage previously provided by {@code
 * ViewDefinitionInstallContainerIT}, runs in-process without Docker, and validates the
 * locally-built {@code UpdateExecutor} directly rather than the published Pathling image.
 */
@Import(FhirServerTestConfiguration.class)
@SpringBootUnitTest
class UpdateExecutorSchemaAutoMergeTest {

  private static final String VIEW_ID = "schema-compat-test";

  /** The id of the row seeded at the wide schema, carrying a Period-valued extension. */
  private static final String WIDE_PATIENT_ID = "wide-schema-patient";

  /** The id written by the narrowed encoder after the table was seeded wide. */
  private static final String NEW_PATIENT_ID = "narrowed-write-patient";

  @Autowired private PathlingContext pathlingContext;

  @Autowired private FhirEncoders fhirEncoders;

  @Autowired private CacheableDatabase cacheableDatabase;

  private Path tempDatabasePath;

  @BeforeEach
  void setUp() throws IOException {
    tempDatabasePath = Files.createTempDirectory("schema-automerge-test-");
  }

  @AfterEach
  void tearDown() throws IOException {
    if (tempDatabasePath != null && Files.exists(tempDatabasePath)) {
      Files.walk(tempDatabasePath)
          .sorted(Comparator.reverseOrder())
          .map(Path::toFile)
          .forEach(File::delete);
    }
  }

  /**
   * With {@code schemaAutoMerge=false} a merge into a table whose nested-struct schema has been
   * downgraded must fail with {@code DELTA_UPDATE_SCHEMA_MISMATCH_EXPRESSION} because Delta cannot
   * cast the richer source struct into the narrower target struct on the {@code
   * whenMatched().updateAll()} path.
   */
  @Test
  void mergeIntoOldSchemaTable_withoutAutoMerge_throwsSchemaMismatch() throws Exception {
    seedTableAndDowngradeSchema();

    final UpdateExecutor executor = newExecutor(false);
    final ViewDefinitionResource update = createViewDefinition(VIEW_ID, "updated_view", "Patient");

    assertThatThrownBy(() -> executor.merge("ViewDefinition", update))
        .hasMessageContaining("DELTA_UPDATE_SCHEMA_MISMATCH_EXPRESSION");
  }

  /**
   * With {@code schemaAutoMerge=true} the warmup write evolves the table schema to match the
   * encoder's current shape before the MERGE runs, so the merge succeeds and the matched row is
   * updated.
   */
  @Test
  void mergeIntoOldSchemaTable_withAutoMerge_succeeds() throws Exception {
    seedTableAndDowngradeSchema();

    final UpdateExecutor executor = newExecutor(true);
    final ViewDefinitionResource update = createViewDefinition(VIEW_ID, "updated_view", "Patient");

    assertThatNoException().isThrownBy(() -> executor.merge("ViewDefinition", update));
  }

  /**
   * When the warmup write evolves the table schema, the executor must refresh the data source entry
   * for the resource type so that in-process consumers see the evolved schema (FR-001), and must
   * log the fields that were added at INFO level (FR-010).
   */
  @Test
  void warmupWriteTriggersRefreshAndLogsAddedFields() throws Exception {
    seedTableAndDowngradeSchema();

    final DynamicDeltaSource dataSource = mock(DynamicDeltaSource.class);
    final UpdateExecutor executor = newExecutor(true, dataSource);
    final ViewDefinitionResource update = createViewDefinition(VIEW_ID, "updated_view", "Patient");

    try (final LogCapture logCapture = LogCapture.forClass(UpdateExecutor.class)) {
      executor.merge("ViewDefinition", update);

      verify(dataSource).refresh("ViewDefinition");
      assertThat(logCapture.events())
          .anySatisfy(
              event -> {
                assertThat(event.getLevel()).isEqualTo(Level.INFO);
                assertThat(event.getFormattedMessage())
                    .contains("ViewDefinition")
                    .contains("forEach");
              });
    }
  }

  /**
   * When the table schema already matches the encoder output, no warmup write occurs and no refresh
   * must be invoked, so undrifted updates carry no extra cost (FR-008 analogue for the update
   * path).
   */
  @Test
  void mergeWithoutDriftDoesNotTriggerRefresh() {
    seedTable();

    final DynamicDeltaSource dataSource = mock(DynamicDeltaSource.class);
    final UpdateExecutor executor = newExecutor(true, dataSource);
    final ViewDefinitionResource update = createViewDefinition(VIEW_ID, "updated_view", "Patient");

    executor.merge("ViewDefinition", update);

    verify(dataSource, never()).refresh(anyString());
  }

  // ---- the narrowing direction: a table wider than the running encoder (US3, FR-008, FR-009) ----

  /**
   * US3 scenario 1: a merge into a table carrying fields the encoder does not emit succeeds, and
   * the table keeps its wider schema. Without the tolerance this fails with {@code
   * DELTA_UPDATE_SCHEMA_MISMATCH_EXPRESSION}, which is the #2697 reproduction.
   */
  @Test
  void mergeIntoWiderTable_succeedsAndLeavesTheSchemaUnchanged() {
    seedWidePatientTable();
    final StructType schemaBefore = patientTableSchema();

    final UpdateExecutor executor = newNarrowExecutor(false);

    assertThatNoException()
        .isThrownBy(() -> executor.merge("Patient", narrowPatient(NEW_PATIENT_ID)));
    assertThat(patientTableSchema()).isEqualTo(schemaBefore);
  }

  /**
   * US3 scenario 1, the null-fill half: the row written by the narrow encoder carries nothing for
   * the fields it cannot express, so reading it back with the wide encoder finds no Period
   * extension.
   */
  @Test
  void mergeIntoWiderTable_leavesStoredOnlyColumnsNull() {
    seedWidePatientTable();

    newNarrowExecutor(false).merge("Patient", narrowPatient(NEW_PATIENT_ID));

    final Patient written = readPatientWithWideEncoder(NEW_PATIENT_ID);
    assertThat(written.getExtension()).noneSatisfy(e -> assertThat(e.getValue()).isNotNull());
  }

  /**
   * US3 scenario 4: the rows written before the configuration was narrowed are untouched, so a
   * server later restarted with the original open types still reads the columns it wrote before.
   */
  @Test
  void mergeIntoWiderTable_leavesEarlierWideRowsIntact() {
    seedWidePatientTable();

    newNarrowExecutor(false).merge("Patient", narrowPatient(NEW_PATIENT_ID));

    final Patient existing = readPatientWithWideEncoder(WIDE_PATIENT_ID);
    assertThat(existing.getExtension()).hasSize(1);
    final Extension extension = existing.getExtension().get(0);
    assertThat(extension.getValue()).isInstanceOf(Period.class);
    assertThat(((Period) extension.getValue()).getStartElement().asStringValue())
        .startsWith("2020-01-01");
  }

  /**
   * US3 scenario 2: a merge matching an existing id replaces that row rather than duplicating it.
   */
  @Test
  void mergeIntoWiderTable_replacesAMatchedRow() {
    seedWidePatientTable();

    final UpdateExecutor executor = newNarrowExecutor(false);
    final Patient replacement = narrowPatient(WIDE_PATIENT_ID);
    replacement.setGender(AdministrativeGender.OTHER);
    executor.merge("Patient", replacement);

    final Dataset<Row> table = readPatientTable();
    assertThat(table.filter("id = '" + WIDE_PATIENT_ID + "'").count()).isEqualTo(1);
    assertThat(readPatientWithWideEncoder(WIDE_PATIENT_ID).getGender())
        .isEqualTo(AdministrativeGender.OTHER);
  }

  /**
   * FR-009: where the table differs in both directions at once, the widening direction stays under
   * the existing policy. With {@code schemaAutoMerge} disabled the merge still fails, so the
   * tolerance has not quietly overridden the flag.
   */
  @Test
  void mergeIntoBothDirectionsTable_withoutAutoMerge_stillFails() throws IOException {
    seedWidePatientTableMissingNestedFields();

    final UpdateExecutor executor = newNarrowExecutor(false);

    assertThatThrownBy(() -> executor.merge("Patient", narrowPatient(NEW_PATIENT_ID)))
        .hasMessageContaining("DELTA_UPDATE_SCHEMA_MISMATCH_EXPRESSION");
  }

  /**
   * FR-009: with {@code schemaAutoMerge} enabled the two directions are settled in order - the
   * warmup write adds the fields the encoder requires, after which the remaining difference is
   * purely narrowing and the tolerance applies.
   */
  @Test
  void mergeIntoBothDirectionsTable_withAutoMerge_succeeds() throws IOException {
    seedWidePatientTableMissingNestedFields();

    final UpdateExecutor executor = newNarrowExecutor(true);

    assertThatNoException()
        .isThrownBy(() -> executor.merge("Patient", narrowPatient(NEW_PATIENT_ID)));
    // The warmup write added the encoder's fields; the table's own wider fields are still there.
    final StructType schema = patientTableSchema();
    assertThat(SchemaDrift.missingFieldPaths(narrowEncoders().of("Patient").schema(), schema))
        .isEmpty();
    assertThat(SchemaDrift.excessFieldPaths(narrowEncoders().of("Patient").schema(), schema))
        .isNotEmpty();
  }

  /**
   * FR-007 and FR-009: a table that reconciles with the running encoder takes neither the warmup
   * write nor the tolerance, so the ordinary write path is unchanged.
   */
  @Test
  void mergeIntoReconcilingTable_isUnaffectedByTheTolerance() {
    seedTable();
    final DynamicDeltaSource dataSource = mock(DynamicDeltaSource.class);
    final UpdateExecutor executor = newExecutor(true, dataSource);

    executor.merge("ViewDefinition", createViewDefinition(VIEW_ID, "updated_view", "Patient"));

    verify(dataSource, never()).refresh(anyString());
  }

  // ---- helpers ----

  /**
   * Two-stage setup: write a ViewDefinition through {@link UpdateExecutor} so the Delta log is
   * created in the correct format, then rewrite the {@code schemaString} on disk to simulate an
   * older encoder version. Struct-level removal is used rather than a wholesale schema replacement
   * because Delta 4.x {@code updateAll()} silently ignores extra top-level columns in the source;
   * {@code DELTA_UPDATE_SCHEMA_MISMATCH_EXPRESSION} only fires when a struct-typed column in the
   * target has fewer fields than the corresponding struct in the source.
   */
  private void seedTableAndDowngradeSchema() throws IOException {
    seedTable();

    final Path tablePath = tempDatabasePath.resolve("ViewDefinition.parquet");
    DeltaSchemaFixtures.removeFieldsFromTableSchema(tablePath, Set.of("forEach", "forEachOrNull"));
    invalidateDeltaMetadataCache(tablePath);
  }

  /** Writes an initial ViewDefinition through {@link UpdateExecutor} to create the Delta table. */
  private void seedTable() {
    final UpdateExecutor seed = newExecutor(true);
    final ViewDefinitionResource initial = createViewDefinition(VIEW_ID, "initial_view", "Patient");
    seed.merge("ViewDefinition", initial);
  }

  private void invalidateDeltaMetadataCache(@Nonnull final Path tablePath) {
    pathlingContext.getSpark().catalog().clearCache();
    pathlingContext.getSpark().catalog().refreshByPath(tablePath.toAbsolutePath().toString());
  }

  @Nonnull
  private UpdateExecutor newExecutor(final boolean schemaAutoMerge) {
    return newExecutor(schemaAutoMerge, mock(QueryableDataSource.class));
  }

  @Nonnull
  private UpdateExecutor newExecutor(
      final boolean schemaAutoMerge, @Nonnull final QueryableDataSource dataSource) {
    return newExecutor(schemaAutoMerge, dataSource, fhirEncoders);
  }

  /**
   * Builds an executor running against the narrowed encoders, so that the table seeded at the
   * ambient schema is wider than the encoder writing to it. This is the state a warehouse is in
   * after {@code pathling.encoding.openTypes} has been narrowed, and it does not depend on which
   * encoding configuration the test profile happens to use.
   */
  @Nonnull
  private UpdateExecutor newNarrowExecutor(final boolean schemaAutoMerge) {
    return newExecutor(schemaAutoMerge, mock(QueryableDataSource.class), narrowEncoders());
  }

  @Nonnull
  private UpdateExecutor newExecutor(
      final boolean schemaAutoMerge,
      @Nonnull final QueryableDataSource dataSource,
      @Nonnull final FhirEncoders encoders) {
    final StorageConfiguration storageConfiguration = new StorageConfiguration();
    storageConfiguration.setSchemaAutoMerge(schemaAutoMerge);
    return new UpdateExecutor(
        pathlingContext,
        encoders,
        tempDatabasePath.toAbsolutePath().toString(),
        cacheableDatabase,
        storageConfiguration,
        dataSource);
  }

  /** The ambient encoders with Period and Quantity dropped from their open types. */
  @Nonnull
  private FhirEncoders narrowEncoders() {
    return FhirEncoderFixtures.narrow(fhirEncoders);
  }

  /**
   * Seeds a Patient table at the ambient (wide) schema, holding one row whose extension carries a
   * Period value. The narrowed encoder cannot express that value, so the table is wider than the
   * encoder in a way that has real data behind it.
   */
  private void seedWidePatientTable() {
    FhirEncoderFixtures.seedTable(
        pathlingContext.getSpark(),
        fhirEncoders,
        "Patient",
        List.of(widePatient()),
        patientTablePath().toString());
  }

  /**
   * Seeds the wide Patient table and then removes the nested {@code prefix} and {@code suffix}
   * fields from its committed schema, so that it differs from the narrowed encoder in both
   * directions at once.
   */
  private void seedWidePatientTableMissingNestedFields() throws IOException {
    seedWidePatientTable();
    DeltaSchemaFixtures.removeFieldsFromTableSchema(patientTablePath(), Set.of("prefix", "suffix"));
    invalidateDeltaMetadataCache(patientTablePath());
  }

  /** A Patient carrying a Period-valued extension, which only the wide encoder can represent. */
  @Nonnull
  private static Patient widePatient() {
    final Patient patient = new Patient();
    patient.setId(WIDE_PATIENT_ID);
    patient.setGender(AdministrativeGender.FEMALE);
    final Period period = new Period();
    period.setStartElement(new DateTimeType("2020-01-01T00:00:00Z"));
    patient.addExtension("http://example.org/period", period);
    return patient;
  }

  /** A Patient whose content the narrowed encoder can represent in full. */
  @Nonnull
  private static Patient narrowPatient(@Nonnull final String id) {
    final Patient patient = new Patient();
    patient.setId(id);
    patient.setActive(true);
    return patient;
  }

  @Nonnull
  private Path patientTablePath() {
    return tempDatabasePath.resolve("Patient.parquet");
  }

  @Nonnull
  private Dataset<Row> readPatientTable() {
    invalidateDeltaMetadataCache(patientTablePath());
    return pathlingContext
        .getSpark()
        .read()
        .format("delta")
        .load(patientTablePath().toAbsolutePath().toString());
  }

  @Nonnull
  private StructType patientTableSchema() {
    return readPatientTable().schema();
  }

  /**
   * Reads one Patient back through the wide encoder, which is what a server restarted with the
   * original open types would do.
   */
  @Nonnull
  private Patient readPatientWithWideEncoder(@Nonnull final String id) {
    final List<Patient> patients =
        readPatientTable()
            .filter("id = '" + id + "'")
            .as(fhirEncoders.<Patient>of("Patient"))
            .collectAsList();
    assertThat(patients).hasSize(1);
    return patients.get(0);
  }

  @Nonnull
  private ViewDefinitionResource createViewDefinition(
      @Nonnull final String id, @Nonnull final String name, @Nonnull final String resource) {
    final ViewDefinitionResource viewDef = new ViewDefinitionResource();
    viewDef.setId(id);
    viewDef.setName(new StringType(name));
    viewDef.setResource(new CodeType(resource));
    viewDef.setStatus(new CodeType("active"));

    final SelectComponent select = new SelectComponent();
    final ColumnComponent column = new ColumnComponent();
    column.setName(new StringType("id"));
    column.setPath(new StringType("id"));
    select.getColumn().add(column);
    viewDef.getSelect().add(select);

    return viewDef;
  }
}
