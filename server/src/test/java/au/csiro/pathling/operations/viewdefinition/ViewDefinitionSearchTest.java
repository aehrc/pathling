/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.operations.viewdefinition;

import static org.assertj.core.api.Assertions.assertThat;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.encoders.ViewDefinitionResource;
import au.csiro.pathling.encoders.ViewDefinitionResource.ColumnComponent;
import au.csiro.pathling.encoders.ViewDefinitionResource.ConstantComponent;
import au.csiro.pathling.encoders.ViewDefinitionResource.SelectComponent;
import au.csiro.pathling.encoders.ViewDefinitionResource.WhereComponent;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.search.SearchExecutor;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.util.CustomObjectDataSource;
import au.csiro.pathling.util.FhirServerTestConfiguration;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.api.server.IBundleProvider;
import ca.uhn.fhir.rest.param.StringAndListParam;
import ca.uhn.fhir.rest.param.StringParam;
import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.StringType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;

/**
 * Tests for searching ViewDefinition resources via the FHIRPath-based search endpoint.
 * <p>
 * These tests verify that ViewDefinition resources can be searched using the same FHIRPath filter
 * mechanism that works for other FHIR resources. ViewDefinition has fields like name, resource,
 * and status that should be searchable.
 * </p>
 *
 * @author John Grimes
 */
@Import(FhirServerTestConfiguration.class)
@SpringBootUnitTest
class ViewDefinitionSearchTest {

  @Autowired
  private FhirContext fhirContext;

  @Autowired
  private SparkSession sparkSession;

  @Autowired
  private PathlingContext pathlingContext;

  @Autowired
  private FhirEncoders fhirEncoders;

  private CustomObjectDataSource dataSource;

  @BeforeEach
  void setUp() {
    // Create a variety of ViewDefinition resources for search testing.
    final List<IBaseResource> viewDefinitions = new ArrayList<>();

    // Patient view - active.
    viewDefinitions.add(createViewDefinition("view-1", "patient_demographics", "Patient", "active"));

    // Patient view with filters - active.
    viewDefinitions.add(createViewDefinitionWithWhere("view-2", "active_patients", "Patient",
        "active", "active = true"));

    // Observation view - active.
    viewDefinitions.add(createViewDefinition("view-3", "lab_results", "Observation", "active"));

    // Condition view - draft.
    viewDefinitions.add(createViewDefinition("view-4", "diagnoses", "Condition", "draft"));

    // MedicationRequest view - retired.
    viewDefinitions.add(createViewDefinition("view-5", "prescriptions", "MedicationRequest",
        "retired"));

    // Complex Patient view with nested structure.
    viewDefinitions.add(createComprehensiveViewDefinition("view-6", "patient_contacts"));

    dataSource = new CustomObjectDataSource(sparkSession, pathlingContext, fhirEncoders,
        viewDefinitions);
  }

  // -------------------------------------------------------------------------
  // Basic search tests
  // -------------------------------------------------------------------------

  @Test
  void searchViewDefinitionsWithoutFilters() {
    // When: searching ViewDefinitions without any filters.
    final IBundleProvider result = createSearchExecutor(Optional.empty());

    // Then: all ViewDefinition resources should be returned.
    assertThat(result.size()).isEqualTo(6);
    final List<IBaseResource> resources = result.getResources(0, result.size());
    assertThat(resources).hasSize(6);
    assertThat(resources).allSatisfy(r -> assertThat(r).isInstanceOf(ViewDefinitionResource.class));
  }

  // -------------------------------------------------------------------------
  // Filter by name tests
  // -------------------------------------------------------------------------

  @Test
  void searchViewDefinitionsWithNameFilter() {
    // Given: a filter for ViewDefinitions with a specific name.
    final StringAndListParam filters = new StringAndListParam();
    filters.addAnd(new StringParam("name = 'patient_demographics'"));

    // When: searching with the filter.
    final IBundleProvider result = createSearchExecutor(Optional.of(filters));

    // Then: only the matching ViewDefinition should be returned.
    assertThat(result.size()).isEqualTo(1);
    final List<IBaseResource> resources = result.getResources(0, result.size());
    assertThat(resources).hasSize(1);
    final ViewDefinitionResource view = (ViewDefinitionResource) resources.get(0);
    assertThat(view.getName().getValue()).isEqualTo("patient_demographics");
  }

  @Test
  void searchViewDefinitionsWithNameOrFilter() {
    // Given: a filter for ViewDefinitions with specific names using OR logic.
    final StringAndListParam filters = new StringAndListParam();
    filters.addAnd(new StringParam("name = 'patient_demographics' or name = 'active_patients'"));

    // When: searching with the filter.
    final IBundleProvider result = createSearchExecutor(Optional.of(filters));

    // Then: only the matching ViewDefinitions should be returned.
    assertThat(result.size()).isEqualTo(2);
    final List<IBaseResource> resources = result.getResources(0, result.size());
    assertThat(resources).hasSize(2);
    assertThat(resources).allSatisfy(r -> {
      final ViewDefinitionResource view = (ViewDefinitionResource) r;
      assertThat(view.getName().getValue()).isIn("patient_demographics", "active_patients");
    });
  }

  // -------------------------------------------------------------------------
  // Filter by resource type tests
  // -------------------------------------------------------------------------

  @Test
  void searchViewDefinitionsForPatientResource() {
    // Given: a filter for ViewDefinitions that query Patient resources.
    final StringAndListParam filters = new StringAndListParam();
    filters.addAnd(new StringParam("resource = 'Patient'"));

    // When: searching with the filter.
    final IBundleProvider result = createSearchExecutor(Optional.of(filters));

    // Then: only Patient ViewDefinitions should be returned.
    assertThat(result.size()).isEqualTo(3);
    final List<IBaseResource> resources = result.getResources(0, result.size());
    assertThat(resources).allSatisfy(r -> {
      final ViewDefinitionResource view = (ViewDefinitionResource) r;
      assertThat(view.getResource().getValue()).isEqualTo("Patient");
    });
  }

  @Test
  void searchViewDefinitionsForObservationResource() {
    // Given: a filter for ViewDefinitions that query Observation resources.
    final StringAndListParam filters = new StringAndListParam();
    filters.addAnd(new StringParam("resource = 'Observation'"));

    // When: searching with the filter.
    final IBundleProvider result = createSearchExecutor(Optional.of(filters));

    // Then: only Observation ViewDefinitions should be returned.
    assertThat(result.size()).isEqualTo(1);
    final List<IBaseResource> resources = result.getResources(0, result.size());
    final ViewDefinitionResource view = (ViewDefinitionResource) resources.get(0);
    assertThat(view.getResource().getValue()).isEqualTo("Observation");
    assertThat(view.getName().getValue()).isEqualTo("lab_results");
  }

  // -------------------------------------------------------------------------
  // Filter by status tests
  // -------------------------------------------------------------------------

  @Test
  void searchViewDefinitionsWithActiveStatus() {
    // Given: a filter for active ViewDefinitions.
    final StringAndListParam filters = new StringAndListParam();
    filters.addAnd(new StringParam("status = 'active'"));

    // When: searching with the filter.
    final IBundleProvider result = createSearchExecutor(Optional.of(filters));

    // Then: only active ViewDefinitions should be returned.
    assertThat(result.size()).isEqualTo(4);
    final List<IBaseResource> resources = result.getResources(0, result.size());
    assertThat(resources).allSatisfy(r -> {
      final ViewDefinitionResource view = (ViewDefinitionResource) r;
      assertThat(view.getStatus().getValue()).isEqualTo("active");
    });
  }

  @Test
  void searchViewDefinitionsWithDraftStatus() {
    // Given: a filter for draft ViewDefinitions.
    final StringAndListParam filters = new StringAndListParam();
    filters.addAnd(new StringParam("status = 'draft'"));

    // When: searching with the filter.
    final IBundleProvider result = createSearchExecutor(Optional.of(filters));

    // Then: only draft ViewDefinitions should be returned.
    assertThat(result.size()).isEqualTo(1);
    final List<IBaseResource> resources = result.getResources(0, result.size());
    final ViewDefinitionResource view = (ViewDefinitionResource) resources.get(0);
    assertThat(view.getStatus().getValue()).isEqualTo("draft");
  }

  // -------------------------------------------------------------------------
  // Combined filter tests
  // -------------------------------------------------------------------------

  @Test
  void searchViewDefinitionsWithMultipleAndFilters() {
    // Given: AND conditions - active Patient ViewDefinitions.
    final StringAndListParam filters = new StringAndListParam();
    filters.addAnd(new StringParam("resource = 'Patient'"));
    filters.addAnd(new StringParam("status = 'active'"));

    // When: searching with AND filters.
    final IBundleProvider result = createSearchExecutor(Optional.of(filters));

    // Then: only active Patient ViewDefinitions should be returned.
    assertThat(result.size()).isEqualTo(3);
    final List<IBaseResource> resources = result.getResources(0, result.size());
    assertThat(resources).allSatisfy(r -> {
      final ViewDefinitionResource view = (ViewDefinitionResource) r;
      assertThat(view.getResource().getValue()).isEqualTo("Patient");
      assertThat(view.getStatus().getValue()).isEqualTo("active");
    });
  }

  // -------------------------------------------------------------------------
  // Pagination tests
  // -------------------------------------------------------------------------

  @Test
  void searchViewDefinitionsWithPagination() {
    // Given: a search that returns all ViewDefinitions.
    final IBundleProvider result = createSearchExecutor(Optional.empty());

    // When: retrieving resources in pages.
    final int pageSize = 2;
    final List<IBaseResource> firstPage = result.getResources(0, pageSize);
    final List<IBaseResource> secondPage = result.getResources(pageSize, pageSize * 2);

    // Then: pages should contain different resources.
    assertThat(firstPage).hasSize(pageSize);
    assertThat(secondPage).hasSize(pageSize);

    final List<String> firstPageIds = firstPage.stream()
        .map(r -> ((ViewDefinitionResource) r).getIdElement().getIdPart())
        .toList();
    final List<String> secondPageIds = secondPage.stream()
        .map(r -> ((ViewDefinitionResource) r).getIdElement().getIdPart())
        .toList();

    assertThat(firstPageIds).doesNotContainAnyElementsOf(secondPageIds);
  }

  // -------------------------------------------------------------------------
  // Helper methods
  // -------------------------------------------------------------------------

  /**
   * Creates a SearchExecutor for ViewDefinition resources.
   */
  @Nonnull
  private IBundleProvider createSearchExecutor(
      @Nonnull final Optional<StringAndListParam> filters) {
    return new SearchExecutor(
        fhirContext,
        dataSource,
        fhirEncoders,
        "ViewDefinition",
        filters,
        false
    );
  }

  @Nonnull
  private ViewDefinitionResource createViewDefinition(@Nonnull final String id,
      @Nonnull final String name, @Nonnull final String resource, @Nonnull final String status) {
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
  private ViewDefinitionResource createViewDefinitionWithWhere(@Nonnull final String id,
      @Nonnull final String name, @Nonnull final String resource, @Nonnull final String status,
      @Nonnull final String wherePath) {
    final ViewDefinitionResource view = createViewDefinition(id, name, resource, status);

    final WhereComponent where = new WhereComponent();
    where.setPath(new StringType(wherePath));
    view.getWhere().add(where);

    return view;
  }

  @Nonnull
  private ViewDefinitionResource createComprehensiveViewDefinition(@Nonnull final String id,
      @Nonnull final String name) {
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
