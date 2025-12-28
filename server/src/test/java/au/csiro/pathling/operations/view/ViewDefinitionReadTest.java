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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.encoders.ViewDefinitionResource;
import au.csiro.pathling.encoders.ViewDefinitionResource.ColumnComponent;
import au.csiro.pathling.encoders.ViewDefinitionResource.ConstantComponent;
import au.csiro.pathling.encoders.ViewDefinitionResource.SelectComponent;
import au.csiro.pathling.encoders.ViewDefinitionResource.WhereComponent;
import au.csiro.pathling.errors.ResourceNotFoundError;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.read.ReadExecutor;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.util.CustomObjectDataSource;
import au.csiro.pathling.util.FhirServerTestConfiguration;
import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.StringType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;

/**
 * Tests for reading ViewDefinition resources via the read operation.
 *
 * <p>These tests verify that ViewDefinition resources can be read by ID, including those with
 * complex nested structures such as nested selects, where clauses, and constants.
 *
 * @author John Grimes
 */
@Import(FhirServerTestConfiguration.class)
@SpringBootUnitTest
class ViewDefinitionReadTest {

  @Autowired private SparkSession sparkSession;

  @Autowired private PathlingContext pathlingContext;

  @Autowired private FhirEncoders fhirEncoders;

  private CustomObjectDataSource dataSource;
  private ReadExecutor readExecutor;

  @BeforeEach
  void setUp() {
    // Create ViewDefinition resources with varying complexity.
    final List<IBaseResource> viewDefinitions = new ArrayList<>();

    // Simple ViewDefinition with minimal structure.
    viewDefinitions.add(createSimpleViewDefinition("view-simple", "simple_view", "Patient"));

    // ViewDefinition with where clause.
    viewDefinitions.add(
        createViewDefinitionWithWhere(
            "view-filtered", "filtered_view", "Patient", "active = true"));

    // ViewDefinition with constants.
    viewDefinitions.add(
        createViewDefinitionWithConstant(
            "view-const", "constant_view", "Patient", "extraction_date", "2024-01-01"));

    // Complex ViewDefinition with nested selects.
    viewDefinitions.add(createComplexViewDefinition("view-complex", "complex_view"));

    dataSource =
        new CustomObjectDataSource(sparkSession, pathlingContext, fhirEncoders, viewDefinitions);
    readExecutor = new ReadExecutor(dataSource, fhirEncoders);
  }

  // -------------------------------------------------------------------------
  // Test reading simple ViewDefinition
  // -------------------------------------------------------------------------

  @Test
  void readSimpleViewDefinition() {
    // When: reading a simple ViewDefinition by ID.
    final IBaseResource result = readExecutor.read("ViewDefinition", "view-simple");

    // Then: the correct ViewDefinition should be returned.
    assertThat(result).isInstanceOf(ViewDefinitionResource.class);
    final ViewDefinitionResource view = (ViewDefinitionResource) result;
    assertThat(view.getIdElement().getIdPart()).isEqualTo("view-simple");
    assertThat(view.getName().getValue()).isEqualTo("simple_view");
    assertThat(view.getResource().getValue()).isEqualTo("Patient");
    assertThat(view.getStatus().getValue()).isEqualTo("active");
  }

  // -------------------------------------------------------------------------
  // Test reading ViewDefinition with where clause
  // -------------------------------------------------------------------------

  @Test
  void readViewDefinitionWithWhereClause() {
    // When: reading a ViewDefinition with a where clause.
    final IBaseResource result = readExecutor.read("ViewDefinition", "view-filtered");

    // Then: the ViewDefinition should include the where clause.
    assertThat(result).isInstanceOf(ViewDefinitionResource.class);
    final ViewDefinitionResource view = (ViewDefinitionResource) result;
    assertThat(view.getIdElement().getIdPart()).isEqualTo("view-filtered");
    assertThat(view.getName().getValue()).isEqualTo("filtered_view");
    assertThat(view.getWhere()).hasSize(1);
    assertThat(view.getWhere().get(0).getPath().getValue()).isEqualTo("active = true");
  }

  // -------------------------------------------------------------------------
  // Test reading ViewDefinition with constants
  // -------------------------------------------------------------------------

  @Test
  void readViewDefinitionWithConstant() {
    // When: reading a ViewDefinition with a constant.
    final IBaseResource result = readExecutor.read("ViewDefinition", "view-const");

    // Then: the ViewDefinition should include the constant.
    assertThat(result).isInstanceOf(ViewDefinitionResource.class);
    final ViewDefinitionResource view = (ViewDefinitionResource) result;
    assertThat(view.getIdElement().getIdPart()).isEqualTo("view-const");
    assertThat(view.getConstant()).hasSize(1);

    final ConstantComponent constant = view.getConstant().get(0);
    assertThat(constant.getName().getValue()).isEqualTo("extraction_date");
    assertThat(((StringType) constant.getValue()).getValue()).isEqualTo("2024-01-01");
  }

  // -------------------------------------------------------------------------
  // Test reading complex ViewDefinition with nested structure
  // -------------------------------------------------------------------------

  @Test
  void readComplexViewDefinitionWithNestedSelects() {
    // When: reading a complex ViewDefinition with nested structure.
    final IBaseResource result = readExecutor.read("ViewDefinition", "view-complex");

    // Then: the ViewDefinition should include all nested elements.
    assertThat(result).isInstanceOf(ViewDefinitionResource.class);
    final ViewDefinitionResource view = (ViewDefinitionResource) result;
    assertThat(view.getIdElement().getIdPart()).isEqualTo("view-complex");
    assertThat(view.getName().getValue()).isEqualTo("complex_view");
    assertThat(view.getResource().getValue()).isEqualTo("Patient");

    // Verify select clauses.
    assertThat(view.getSelect()).hasSize(2);

    // First select with columns.
    final SelectComponent firstSelect = view.getSelect().get(0);
    assertThat(firstSelect.getColumn()).hasSizeGreaterThanOrEqualTo(2);

    // Second select with forEach (nested select).
    final SelectComponent nestedSelect = view.getSelect().get(1);
    assertThat(nestedSelect.getForEach().getValue()).isEqualTo("telecom");
    assertThat(nestedSelect.getColumn()).hasSizeGreaterThanOrEqualTo(1);

    // Verify where clause is present.
    assertThat(view.getWhere()).hasSize(1);

    // Verify constant is present.
    assertThat(view.getConstant()).hasSize(1);
  }

  @Test
  void readComplexViewDefinitionPreservesColumnDetails() {
    // When: reading a complex ViewDefinition.
    final IBaseResource result = readExecutor.read("ViewDefinition", "view-complex");
    final ViewDefinitionResource view = (ViewDefinitionResource) result;

    // Then: column details should be preserved.
    final SelectComponent firstSelect = view.getSelect().get(0);
    final List<ColumnComponent> columns = firstSelect.getColumn();

    assertThat(columns)
        .anySatisfy(
            col -> {
              assertThat(col.getName().getValue()).isEqualTo("id");
              assertThat(col.getPath().getValue()).isEqualTo("id");
            });

    assertThat(columns)
        .anySatisfy(
            col -> {
              assertThat(col.getName().getValue()).isEqualTo("family_name");
              assertThat(col.getPath().getValue()).isEqualTo("name.first().family");
            });
  }

  // -------------------------------------------------------------------------
  // Test error cases
  // -------------------------------------------------------------------------

  @Test
  void readNonExistentViewDefinitionThrowsError() {
    // When/Then: reading a non-existent ViewDefinition should throw ResourceNotFoundError.
    assertThatThrownBy(() -> readExecutor.read("ViewDefinition", "non-existent"))
        .isInstanceOf(ResourceNotFoundError.class)
        .hasMessageContaining("ViewDefinition")
        .hasMessageContaining("non-existent");
  }

  // -------------------------------------------------------------------------
  // Helper methods
  // -------------------------------------------------------------------------

  @Nonnull
  private ViewDefinitionResource createSimpleViewDefinition(
      @Nonnull final String id, @Nonnull final String name, @Nonnull final String resource) {
    final ViewDefinitionResource view = new ViewDefinitionResource();
    view.setId(id);
    view.setName(new StringType(name));
    view.setResource(new CodeType(resource));
    view.setStatus(new CodeType("active"));

    // Add minimal select clause.
    final SelectComponent select = new SelectComponent();
    select.getColumn().add(createColumn("id", "id"));
    view.getSelect().add(select);

    return view;
  }

  @Nonnull
  private ViewDefinitionResource createViewDefinitionWithWhere(
      @Nonnull final String id,
      @Nonnull final String name,
      @Nonnull final String resource,
      @Nonnull final String wherePath) {
    final ViewDefinitionResource view = createSimpleViewDefinition(id, name, resource);

    final WhereComponent where = new WhereComponent();
    where.setPath(new StringType(wherePath));
    view.getWhere().add(where);

    return view;
  }

  @Nonnull
  private ViewDefinitionResource createViewDefinitionWithConstant(
      @Nonnull final String id,
      @Nonnull final String name,
      @Nonnull final String resource,
      @Nonnull final String constName,
      @Nonnull final String constValue) {
    final ViewDefinitionResource view = createSimpleViewDefinition(id, name, resource);

    final ConstantComponent constant = new ConstantComponent();
    constant.setName(new StringType(constName));
    constant.setValue(new StringType(constValue));
    view.getConstant().add(constant);

    return view;
  }

  @Nonnull
  private ViewDefinitionResource createComplexViewDefinition(
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
