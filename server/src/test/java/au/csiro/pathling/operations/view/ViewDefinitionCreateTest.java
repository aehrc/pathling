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

package au.csiro.pathling.operations.view;

import static org.assertj.core.api.Assertions.assertThat;

import au.csiro.pathling.cache.CacheableDatabase;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.encoders.ViewDefinitionResource;
import au.csiro.pathling.encoders.ViewDefinitionResource.ColumnComponent;
import au.csiro.pathling.encoders.ViewDefinitionResource.SelectComponent;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.operations.create.CreateProvider;
import au.csiro.pathling.operations.update.UpdateExecutor;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.util.FhirServerTestConfiguration;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.api.MethodOutcome;
import io.delta.tables.DeltaTable;
import jakarta.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.UUID;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.StringType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;

/**
 * Tests for creating ViewDefinition resources via the FHIR create operation.
 *
 * @author John Grimes
 */
@Import(FhirServerTestConfiguration.class)
@SpringBootUnitTest
class ViewDefinitionCreateTest {

  @Autowired private SparkSession sparkSession;

  @Autowired private PathlingContext pathlingContext;

  @Autowired private FhirEncoders fhirEncoders;

  @Autowired private FhirContext fhirContext;

  @Autowired private CacheableDatabase cacheableDatabase;

  private Path tempDatabasePath;
  private CreateProvider createProvider;

  @BeforeEach
  void setUp() throws IOException {
    // Create a temporary directory for the Delta Lake database.
    tempDatabasePath = Files.createTempDirectory("viewdefinition-create-test-");

    // Create UpdateExecutor with the temp database path.
    final UpdateExecutor updateExecutor =
        new UpdateExecutor(
            pathlingContext,
            fhirEncoders,
            tempDatabasePath.toAbsolutePath().toString(),
            cacheableDatabase);

    // Create the CreateProvider for ViewDefinition.
    createProvider = new CreateProvider(updateExecutor, fhirContext, ViewDefinitionResource.class);
  }

  @AfterEach
  void tearDown() throws IOException {
    // Clean up the temporary directory.
    if (tempDatabasePath != null && Files.exists(tempDatabasePath)) {
      Files.walk(tempDatabasePath)
          .sorted(Comparator.reverseOrder())
          .map(Path::toFile)
          .forEach(File::delete);
    }
  }

  // -------------------------------------------------------------------------
  // Test successful create operations
  // -------------------------------------------------------------------------

  @Test
  void createViewDefinitionGeneratesNewUuid() {
    // Given: a ViewDefinition resource without an ID.
    final ViewDefinitionResource viewDef = createViewDefinition(null, "test_view", "Patient");

    // When: creating the resource.
    final MethodOutcome outcome = createProvider.create(viewDef);

    // Then: a new UUID should be generated.
    assertThat(outcome.getId()).isNotNull();
    assertThat(outcome.getId().getIdPart()).isNotEmpty();
    assertThat(isValidUuid(outcome.getId().getIdPart())).isTrue();
  }

  @Test
  void createViewDefinitionIgnoresClientProvidedId() {
    // Given: a ViewDefinition resource with a client-provided ID.
    final String clientId = "client-provided-view-id";
    final ViewDefinitionResource viewDef = createViewDefinition(clientId, "test_view", "Patient");

    // When: creating the resource.
    final MethodOutcome outcome = createProvider.create(viewDef);

    // Then: the client-provided ID should be ignored and a new UUID generated.
    assertThat(outcome.getId()).isNotNull();
    assertThat(outcome.getId().getIdPart()).isNotEqualTo(clientId);
    assertThat(isValidUuid(outcome.getId().getIdPart())).isTrue();
  }

  @Test
  void createViewDefinitionSetsCreatedToTrue() {
    // Given: a ViewDefinition resource.
    final ViewDefinitionResource viewDef = createViewDefinition(null, "test_view", "Patient");

    // When: creating the resource.
    final MethodOutcome outcome = createProvider.create(viewDef);

    // Then: the outcome should indicate the resource was created.
    assertThat(outcome.getCreated()).isTrue();
  }

  @Test
  void createViewDefinitionPersistsToDeltaLake() {
    // Given: a ViewDefinition resource.
    final ViewDefinitionResource viewDef =
        createViewDefinition(null, "persisted_view", "Observation");

    // When: creating the resource.
    final MethodOutcome outcome = createProvider.create(viewDef);

    // Then: the resource should be persisted to Delta Lake.
    final String tablePath = tempDatabasePath.resolve("ViewDefinition.parquet").toString();
    assertThat(DeltaTable.isDeltaTable(sparkSession, tablePath)).isTrue();

    final Dataset<Row> dataset = sparkSession.read().format("delta").load(tablePath);
    assertThat(dataset.count()).isEqualTo(1);

    final Row row = dataset.first();
    assertThat(row.getAs("id").toString()).isEqualTo(outcome.getId().getIdPart());
  }

  @Test
  void createMultipleViewDefinitionsGeneratesUniqueIds() {
    // Given: multiple ViewDefinition resources.
    final ViewDefinitionResource viewDef1 = createViewDefinition(null, "view_1", "Patient");
    final ViewDefinitionResource viewDef2 = createViewDefinition(null, "view_2", "Observation");

    // When: creating both resources.
    final MethodOutcome outcome1 = createProvider.create(viewDef1);
    final MethodOutcome outcome2 = createProvider.create(viewDef2);

    // Then: each should have a unique UUID.
    assertThat(outcome1.getId().getIdPart()).isNotEqualTo(outcome2.getId().getIdPart());
    assertThat(isValidUuid(outcome1.getId().getIdPart())).isTrue();
    assertThat(isValidUuid(outcome2.getId().getIdPart())).isTrue();

    // And: both should be persisted.
    final String tablePath = tempDatabasePath.resolve("ViewDefinition.parquet").toString();
    final Dataset<Row> dataset = sparkSession.read().format("delta").load(tablePath);
    assertThat(dataset.count()).isEqualTo(2);
  }

  @Test
  void createViewDefinitionPreservesContent() {
    // Given: a ViewDefinition with specific content.
    final ViewDefinitionResource viewDef =
        createViewDefinition(null, "patient_demographics", "Patient");

    // When: creating the resource.
    final MethodOutcome outcome = createProvider.create(viewDef);

    // Then: the returned resource should preserve the content.
    assertThat(outcome.getResource()).isNotNull();
    assertThat(outcome.getResource()).isInstanceOf(ViewDefinitionResource.class);

    final ViewDefinitionResource returnedViewDef = (ViewDefinitionResource) outcome.getResource();
    assertThat(returnedViewDef.getName().getValue()).isEqualTo("patient_demographics");
    assertThat(returnedViewDef.getResource().getValue()).isEqualTo("Patient");
  }

  // -------------------------------------------------------------------------
  // Helper methods
  // -------------------------------------------------------------------------

  @Nonnull
  private ViewDefinitionResource createViewDefinition(
      final String id, @Nonnull final String name, @Nonnull final String resource) {
    final ViewDefinitionResource viewDef = new ViewDefinitionResource();
    if (id != null) {
      viewDef.setId(id);
    }
    viewDef.setName(new StringType(name));
    viewDef.setResource(new CodeType(resource));
    viewDef.setStatus(new CodeType("active"));

    // Add minimal select clause.
    final SelectComponent select = new SelectComponent();
    select.getColumn().add(createColumn("id", "id"));
    viewDef.getSelect().add(select);

    return viewDef;
  }

  @Nonnull
  private ColumnComponent createColumn(@Nonnull final String name, @Nonnull final String path) {
    final ColumnComponent column = new ColumnComponent();
    column.setName(new StringType(name));
    column.setPath(new StringType(path));
    return column;
  }

  private boolean isValidUuid(@Nonnull final String str) {
    try {
      UUID.fromString(str);
      return true;
    } catch (final IllegalArgumentException e) {
      return false;
    }
  }
}
