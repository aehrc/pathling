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
import static org.assertj.core.api.Assertions.assertThatNoException;

import au.csiro.pathling.FhirServer;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.encoders.ViewDefinitionResource;
import au.csiro.pathling.encoders.ViewDefinitionResource.ColumnComponent;
import au.csiro.pathling.encoders.ViewDefinitionResource.ConstantComponent;
import au.csiro.pathling.encoders.ViewDefinitionResource.SelectComponent;
import au.csiro.pathling.encoders.ViewDefinitionResource.WhereComponent;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.util.FhirServerTestConfiguration;
import jakarta.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
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
 * Tests for updating ViewDefinition resources via the FHIR Update operation.
 *
 * <p>These tests verify that ViewDefinition resources can be created and updated using the same
 * mechanisms that work for standard FHIR resources. ViewDefinition is a custom resource type from
 * the SQL on FHIR specification that should be treated like any other updateable resource.
 *
 * @author John Grimes
 */
@Import(FhirServerTestConfiguration.class)
@SpringBootUnitTest
class ViewDefinitionUpdateTest {

  @Autowired private SparkSession sparkSession;

  @Autowired private PathlingContext pathlingContext;

  @Autowired private FhirEncoders fhirEncoders;

  private Path tempDatabasePath;

  @BeforeEach
  void setUp() throws IOException {
    // Create a temporary directory for the Delta Lake database.
    tempDatabasePath = Files.createTempDirectory("viewdefinition-update-test-");
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
  // Encoder tests
  // -------------------------------------------------------------------------

  @Test
  void fhirEncoderSupportsViewDefinition() {
    // Given: a ViewDefinition resource.
    final ViewDefinitionResource view =
        createViewDefinition("test-view-1", "patient_demographics", "Patient", "active");

    // When: encoding the resource using FhirEncoders.
    // Then: the encoder should successfully encode ViewDefinition resources.
    assertThatNoException()
        .isThrownBy(
            () -> {
              final Dataset<Row> dataset =
                  sparkSession
                      .createDataset(List.of(view), fhirEncoders.of("ViewDefinition"))
                      .toDF();
              assertThat(dataset.count()).isEqualTo(1);
            });
  }

  @Test
  void fhirEncoderPreservesViewDefinitionFields() {
    // Given: a comprehensive ViewDefinition resource.
    final ViewDefinitionResource view =
        createComprehensiveViewDefinition("test-view-2", "patient_contacts");

    // When: encoding and then accessing the dataset.
    final Dataset<Row> dataset =
        sparkSession.createDataset(List.of(view), fhirEncoders.of("ViewDefinition")).toDF();

    // Then: the dataset should contain the expected fields.
    final Row row = dataset.first();
    assertThat(row.getAs("id").toString()).isEqualTo("test-view-2");
    assertThat(row.getAs("name").toString()).contains("patient_contacts");
    assertThat(row.getAs("resource").toString()).contains("Patient");
    assertThat(row.getAs("status").toString()).contains("active");
  }

  @Test
  void fhirEncoderHandlesMultipleViewDefinitions() {
    // Given: multiple ViewDefinition resources.
    final List<ViewDefinitionResource> views =
        List.of(
            createViewDefinition("view-1", "patient_view", "Patient", "active"),
            createViewDefinition("view-2", "observation_view", "Observation", "active"),
            createViewDefinition("view-3", "condition_view", "Condition", "draft"));

    // When: encoding multiple resources.
    final Dataset<Row> dataset =
        sparkSession.createDataset(views, fhirEncoders.of("ViewDefinition")).toDF();

    // Then: all resources should be encoded.
    assertThat(dataset.count()).isEqualTo(3);
  }

  // -------------------------------------------------------------------------
  // Resource type handling tests
  // -------------------------------------------------------------------------

  @Test
  void viewDefinitionIsRecognizedAsCustomResourceType() {
    // Given: the string "ViewDefinition".
    final String resourceTypeCode = "ViewDefinition";

    // When: checking if ViewDefinition is a custom resource type.
    // Then: it should be recognized as a valid custom type.
    assertThat(FhirServer.isCustomResourceType(resourceTypeCode)).isTrue();
  }

  // -------------------------------------------------------------------------
  // Delta Lake merge operation tests
  // -------------------------------------------------------------------------

  @Test
  void viewDefinitionCanBeSavedToDeltaTable() {
    // Given: a ViewDefinition resource.
    final ViewDefinitionResource view =
        createViewDefinition("delta-test-1", "test_view", "Patient", "active");

    // When: saving to a Delta table.
    final String tablePath = tempDatabasePath.resolve("ViewDefinition.parquet").toString();
    final Dataset<Row> dataset =
        sparkSession.createDataset(List.of(view), fhirEncoders.of("ViewDefinition")).toDF();

    // Then: the save operation should succeed.
    assertThatNoException()
        .isThrownBy(
            () -> {
              dataset.write().format("delta").mode("overwrite").save(tablePath);
            });

    // And: the data should be readable.
    final Dataset<Row> loaded = sparkSession.read().format("delta").load(tablePath);
    assertThat(loaded.count()).isEqualTo(1);
  }

  @Test
  void viewDefinitionCanBeMergedIntoDeltaTable() {
    // Given: an initial ViewDefinition saved to Delta.
    final ViewDefinitionResource initialView =
        createViewDefinition("merge-test-1", "initial_name", "Patient", "draft");
    final String tablePath = tempDatabasePath.resolve("ViewDefinition.parquet").toString();

    final Dataset<Row> initialDataset =
        sparkSession.createDataset(List.of(initialView), fhirEncoders.of("ViewDefinition")).toDF();
    initialDataset.write().format("delta").mode("overwrite").save(tablePath);

    // When: merging an updated ViewDefinition with the same ID.
    final ViewDefinitionResource updatedView =
        createViewDefinition("merge-test-1", "updated_name", "Patient", "active");
    final Dataset<Row> updateDataset =
        sparkSession.createDataset(List.of(updatedView), fhirEncoders.of("ViewDefinition")).toDF();

    // Then: the merge should update the existing row.
    assertThatNoException()
        .isThrownBy(
            () -> {
              io.delta.tables.DeltaTable.forPath(sparkSession, tablePath)
                  .as("original")
                  .merge(updateDataset.as("updates"), "original.id = updates.id")
                  .whenMatched()
                  .updateAll()
                  .whenNotMatched()
                  .insertAll()
                  .execute();
            });

    // And: the table should contain the updated data.
    final Dataset<Row> loaded = sparkSession.read().format("delta").load(tablePath);
    assertThat(loaded.count()).isEqualTo(1);
    final Row row = loaded.first();
    assertThat(row.getAs("name").toString()).contains("updated_name");
    assertThat(row.getAs("status").toString()).contains("active");
  }

  @Test
  void viewDefinitionMergeInsertsNewResources() {
    // Given: an existing ViewDefinition in Delta.
    final ViewDefinitionResource existingView =
        createViewDefinition("existing-1", "existing_view", "Patient", "active");
    final String tablePath = tempDatabasePath.resolve("ViewDefinition.parquet").toString();

    final Dataset<Row> existingDataset =
        sparkSession.createDataset(List.of(existingView), fhirEncoders.of("ViewDefinition")).toDF();
    existingDataset.write().format("delta").mode("overwrite").save(tablePath);

    // When: merging a new ViewDefinition with a different ID.
    final ViewDefinitionResource newView =
        createViewDefinition("new-1", "new_view", "Observation", "active");
    final Dataset<Row> newDataset =
        sparkSession.createDataset(List.of(newView), fhirEncoders.of("ViewDefinition")).toDF();

    io.delta.tables.DeltaTable.forPath(sparkSession, tablePath)
        .as("original")
        .merge(newDataset.as("updates"), "original.id = updates.id")
        .whenMatched()
        .updateAll()
        .whenNotMatched()
        .insertAll()
        .execute();

    // Then: the table should contain both resources.
    final Dataset<Row> loaded = sparkSession.read().format("delta").load(tablePath);
    assertThat(loaded.count()).isEqualTo(2);
  }

  // -------------------------------------------------------------------------
  // Comprehensive structure tests
  // -------------------------------------------------------------------------

  @Test
  void comprehensiveViewDefinitionCanBeEncodedAndDecoded() {
    // Given: a comprehensive ViewDefinition with all structure elements.
    final ViewDefinitionResource view =
        createComprehensiveViewDefinition("comprehensive-1", "full_patient_view");

    // When: encoding to Spark Dataset and reading back.
    final String tablePath = tempDatabasePath.resolve("ViewDefinition.parquet").toString();
    final Dataset<Row> dataset =
        sparkSession.createDataset(List.of(view), fhirEncoders.of("ViewDefinition")).toDF();
    dataset.write().format("delta").mode("overwrite").save(tablePath);

    // Then: the comprehensive structure should be preserved.
    final Dataset<Row> loaded = sparkSession.read().format("delta").load(tablePath);
    assertThat(loaded.count()).isEqualTo(1);

    final Row row = loaded.first();
    assertThat(row.getAs("id").toString()).isEqualTo("comprehensive-1");
    // Verify nested structures are present.
    assertThat(row.schema().fieldNames()).contains("select", "where", "constant");
  }

  // -------------------------------------------------------------------------
  // Helper methods
  // -------------------------------------------------------------------------

  @Nonnull
  private ViewDefinitionResource createViewDefinition(
      @Nonnull final String id,
      @Nonnull final String name,
      @Nonnull final String resource,
      @Nonnull final String status) {
    final ViewDefinitionResource view = new ViewDefinitionResource();
    view.setId(id);
    view.setName(new StringType(name));
    view.setResource(new CodeType(resource));
    view.setStatus(new CodeType(status));

    // Add minimal select clause.
    final SelectComponent select = new SelectComponent();
    select.getColumn().add(createColumn("id", "id"));
    view.getSelect().add(select);

    return view;
  }

  @Nonnull
  private ViewDefinitionResource createComprehensiveViewDefinition(
      @Nonnull final String id, @Nonnull final String name) {
    final ViewDefinitionResource view = new ViewDefinitionResource();
    view.setId(id);
    view.setName(new StringType(name));
    view.setResource(new CodeType("Patient"));
    view.setStatus(new CodeType("active"));

    // Add constant.
    final ConstantComponent constant = new ConstantComponent();
    constant.setName(new StringType("extraction_date"));
    constant.setValue(new StringType("2024-01-01"));
    view.getConstant().add(constant);

    // Add where clause.
    final WhereComponent where = new WhereComponent();
    where.setPath(new StringType("active = true"));
    view.getWhere().add(where);

    // Add select with columns.
    final SelectComponent select = new SelectComponent();
    select.getColumn().add(createColumn("id", "id"));
    select.getColumn().add(createColumn("family_name", "name.first().family"));
    select.getColumn().add(createColumn("gender", "gender"));
    view.getSelect().add(select);

    // Add nested select with forEach.
    final SelectComponent nestedSelect = new SelectComponent();
    nestedSelect.setForEach(new StringType("telecom"));
    nestedSelect.getColumn().add(createColumn("phone_system", "system"));
    nestedSelect.getColumn().add(createColumn("phone_value", "value"));
    view.getSelect().add(nestedSelect);

    return view;
  }

  @Nonnull
  private ColumnComponent createColumn(@Nonnull final String name, @Nonnull final String path) {
    final ColumnComponent column = new ColumnComponent();
    column.setName(new StringType(name));
    column.setPath(new StringType(path));
    return column;
  }
}
