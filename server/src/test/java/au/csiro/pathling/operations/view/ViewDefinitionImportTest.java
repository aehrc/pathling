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

package au.csiro.pathling.operations.view;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

import au.csiro.pathling.async.PreAsyncValidation.PreAsyncValidationResult;
import au.csiro.pathling.encoders.ViewDefinitionResource;
import au.csiro.pathling.encoders.ViewDefinitionResource.ColumnComponent;
import au.csiro.pathling.encoders.ViewDefinitionResource.ConstantComponent;
import au.csiro.pathling.encoders.ViewDefinitionResource.SelectComponent;
import au.csiro.pathling.encoders.ViewDefinitionResource.WhereComponent;
import au.csiro.pathling.operations.bulkimport.ImportManifest;
import au.csiro.pathling.operations.bulkimport.ImportManifestInput;
import au.csiro.pathling.operations.bulkimport.ImportOperationValidator;
import au.csiro.pathling.operations.bulkimport.ImportRequest;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.util.FhirServerTestConfiguration;
import au.csiro.pathling.util.MockUtil;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import jakarta.annotation.Nonnull;
import java.util.List;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.UrlType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

/**
 * Tests for importing ViewDefinition resources via the $import operation.
 *
 * <p>These tests verify that ViewDefinition resources can be imported like any other FHIR resource
 * type. ViewDefinition is a custom resource type from the SQL on FHIR specification that enables
 * defining tabular views over FHIR data.
 *
 * @author John Grimes
 */
@SpringBootUnitTest
@Import({FhirServerTestConfiguration.class, ViewDefinitionImportTest.TestConfig.class})
class ViewDefinitionImportTest {

  @TestConfiguration
  static class TestConfig {

    @Bean
    public ImportOperationValidator importOperationValidator() {
      return new ImportOperationValidator();
    }
  }

  @Autowired private ImportOperationValidator importOperationValidator;

  @Autowired private FhirContext fhirContext;

  private RequestDetails mockRequest;

  @BeforeEach
  void setUp() {
    mockRequest = MockUtil.mockRequest("application/fhir+json", "respond-async", false);
  }

  // -------------------------------------------------------------------------
  // Parameters format tests
  // -------------------------------------------------------------------------

  @Test
  void importViewDefinitionViaParametersRequest() {
    // Given: a valid Parameters request with ViewDefinition resource type.
    final Parameters params =
        createViewDefinitionImportParams("s3://bucket/viewdefinitions.ndjson");

    // When: validating the request.
    // Then: validation should pass and the request should contain ViewDefinition input.
    assertThatNoException()
        .isThrownBy(
            () -> {
              final PreAsyncValidationResult<ImportRequest> result =
                  importOperationValidator.validateParametersRequest(mockRequest, params);
              assertThat(result.result()).isNotNull();
              assertThat(result.result().input()).containsKey("ViewDefinition");
              assertThat(result.result().input().get("ViewDefinition"))
                  .contains("s3a://bucket/viewdefinitions.ndjson");
            });
  }

  @Test
  void importMultipleViewDefinitionsViaParameters() {
    // Given: a Parameters request with multiple ViewDefinition URLs.
    final Parameters params = createMultipleViewDefinitionImportParams();

    // When: validating the request.
    // Then: all ViewDefinition URLs should be included in the input.
    assertThatNoException()
        .isThrownBy(
            () -> {
              final PreAsyncValidationResult<ImportRequest> result =
                  importOperationValidator.validateParametersRequest(mockRequest, params);
              assertThat(result.result()).isNotNull();
              assertThat(result.result().input()).containsKey("ViewDefinition");
              assertThat(result.result().input().get("ViewDefinition"))
                  .containsExactlyInAnyOrder(
                      "s3a://bucket/patient-views.ndjson", "s3a://bucket/observation-views.ndjson");
            });
  }

  @Test
  void importViewDefinitionWithOtherResourceTypes() {
    // Given: a Parameters request with both ViewDefinition and Patient resource types.
    final Parameters params = createMixedResourceTypeParams();

    // When: validating the request.
    // Then: both resource types should be accepted.
    assertThatNoException()
        .isThrownBy(
            () -> {
              final PreAsyncValidationResult<ImportRequest> result =
                  importOperationValidator.validateParametersRequest(mockRequest, params);
              assertThat(result.result()).isNotNull();
              assertThat(result.result().input()).containsKeys("ViewDefinition", "Patient");
            });
  }

  // -------------------------------------------------------------------------
  // JSON manifest format tests
  // -------------------------------------------------------------------------

  @Test
  void importViewDefinitionViaJsonManifest() {
    // Given: a JSON manifest with ViewDefinition resource type.
    final ImportManifest manifest =
        new ImportManifest(
            "application/fhir+ndjson",
            "https://example.org/source",
            List.of(
                new ImportManifestInput("ViewDefinition", "s3://bucket/viewdefinitions.ndjson")),
            null);

    // When: validating the request.
    // Then: validation should pass and the request should contain ViewDefinition input.
    assertThatNoException()
        .isThrownBy(
            () -> {
              final PreAsyncValidationResult<ImportRequest> result =
                  importOperationValidator.validateJsonRequest(mockRequest, manifest);
              assertThat(result.result()).isNotNull();
              assertThat(result.result().input()).containsKey("ViewDefinition");
            });
  }

  @Test
  void importMultipleViewDefinitionsViaJsonManifest() {
    // Given: a JSON manifest with multiple ViewDefinition inputs.
    final ImportManifest manifest =
        new ImportManifest(
            "application/fhir+ndjson",
            "https://example.org/source",
            List.of(
                new ImportManifestInput("ViewDefinition", "s3://bucket/patient-views.ndjson"),
                new ImportManifestInput("ViewDefinition", "s3://bucket/observation-views.ndjson")),
            null);

    // When: validating the request.
    // Then: all ViewDefinition URLs should be grouped together.
    assertThatNoException()
        .isThrownBy(
            () -> {
              final PreAsyncValidationResult<ImportRequest> result =
                  importOperationValidator.validateJsonRequest(mockRequest, manifest);
              assertThat(result.result()).isNotNull();
              assertThat(result.result().input().get("ViewDefinition")).hasSize(2);
            });
  }

  // -------------------------------------------------------------------------
  // ViewDefinition resource structure tests
  // -------------------------------------------------------------------------

  @Test
  void viewDefinitionCanBeSerializedToJson() {
    // Given: a comprehensive ViewDefinition resource.
    final ViewDefinitionResource view =
        createComprehensiveViewDefinition("test-view-1", "patient_demographics");

    // When: serializing to JSON.
    final String json = fhirContext.newJsonParser().encodeResourceToString(view);

    // Then: the JSON should contain all expected fields.
    assertThat(json).contains("\"resourceType\":\"ViewDefinition\"");
    assertThat(json).contains("\"name\":\"patient_demographics\"");
    assertThat(json).contains("\"resource\":\"Patient\"");
    assertThat(json).contains("\"status\":\"active\"");
    assertThat(json).contains("\"select\"");
    assertThat(json).contains("\"where\"");
    assertThat(json).contains("\"constant\"");
  }

  @Test
  void viewDefinitionCanBeDeserializedFromJson() {
    // Given: a ViewDefinition JSON string.
    final String viewJson =
        """
        {
          "resourceType": "ViewDefinition",
          "id": "test-view-2",
          "name": "observation_flat",
          "resource": "Observation",
          "status": "active",
          "select": [
            {
              "column": [
                {"name": "id", "path": "id"},
                {"name": "code", "path": "code.coding.first().code"}
              ]
            }
          ]
        }
        """;

    // When: parsing the JSON.
    final ViewDefinitionResource view =
        fhirContext.newJsonParser().parseResource(ViewDefinitionResource.class, viewJson);

    // Then: the resource should be correctly parsed.
    assertThat(view.getIdPart()).isEqualTo("test-view-2");
    assertThat(view.getName().getValue()).isEqualTo("observation_flat");
    assertThat(view.getResource().getValue()).isEqualTo("Observation");
    assertThat(view.getStatus().getValue()).isEqualTo("active");
    assertThat(view.getSelect()).hasSize(1);
    assertThat(view.getSelect().get(0).getColumn()).hasSize(2);
  }

  @Test
  void viewDefinitionWithNestedForEachCanBeSerialized() {
    // Given: a ViewDefinition with nested forEach select.
    final ViewDefinitionResource view = createViewDefinitionWithNestedForEach();

    // When: serializing to JSON.
    final String json = fhirContext.newJsonParser().encodeResourceToString(view);

    // Then: the nested structure should be preserved.
    assertThat(json).contains("\"forEach\":\"telecom\"");
    assertThat(json).contains("\"phone_system\"");
    assertThat(json).contains("\"phone_value\"");
  }

  @Test
  void viewDefinitionWithUnionAllCanBeSerialized() {
    // Given: a ViewDefinition with unionAll.
    final ViewDefinitionResource view = createViewDefinitionWithUnionAll();

    // When: serializing to JSON.
    final String json = fhirContext.newJsonParser().encodeResourceToString(view);

    // Then: the unionAll structure should be preserved.
    assertThat(json).contains("\"unionAll\"");
  }

  // -------------------------------------------------------------------------
  // Helper methods
  // -------------------------------------------------------------------------

  @Nonnull
  private Parameters createViewDefinitionImportParams(@Nonnull final String url) {
    final Parameters params = new Parameters();
    params
        .addParameter()
        .setName("inputSource")
        .setValue(new StringType("https://example.org/source"));
    params.addParameter(createInputParam("ViewDefinition", url));
    return params;
  }

  @Nonnull
  private Parameters createMultipleViewDefinitionImportParams() {
    final Parameters params = new Parameters();
    params
        .addParameter()
        .setName("inputSource")
        .setValue(new StringType("https://example.org/source"));
    params.addParameter(createInputParam("ViewDefinition", "s3://bucket/patient-views.ndjson"));
    params.addParameter(createInputParam("ViewDefinition", "s3://bucket/observation-views.ndjson"));
    return params;
  }

  @Nonnull
  private Parameters createMixedResourceTypeParams() {
    final Parameters params = new Parameters();
    params
        .addParameter()
        .setName("inputSource")
        .setValue(new StringType("https://example.org/source"));
    params.addParameter(createInputParam("ViewDefinition", "s3://bucket/views.ndjson"));
    params.addParameter(createInputParam("Patient", "s3://bucket/patients.ndjson"));
    return params;
  }

  @Nonnull
  private ParametersParameterComponent createInputParam(
      @Nonnull final String resourceType, @Nonnull final String url) {
    final ParametersParameterComponent input = new ParametersParameterComponent();
    input.setName("input");
    input.addPart().setName("resourceType").setValue(new CodeType(resourceType));
    input.addPart().setName("url").setValue(new UrlType(url));
    return input;
  }

  /**
   * Creates a comprehensive ViewDefinition with nested selects, forEach, where clauses, and
   * constants to exercise the full structure.
   */
  @Nonnull
  private ViewDefinitionResource createComprehensiveViewDefinition(
      @Nonnull final String id, @Nonnull final String name) {
    final ViewDefinitionResource view = new ViewDefinitionResource();
    view.setId(id);
    view.setName(new StringType(name));
    view.setResource(new CodeType("Patient"));
    view.setStatus(new CodeType("active"));

    // Add constants.
    final ConstantComponent constant = new ConstantComponent();
    constant.setName(new StringType("extraction_date"));
    constant.setValue(new StringType("2024-01-01"));
    view.getConstant().add(constant);

    // Add where clause.
    final WhereComponent where = new WhereComponent();
    where.setPath(new StringType("active = true"));
    view.getWhere().add(where);

    // Add select with multiple columns.
    final SelectComponent select = new SelectComponent();
    select.getColumn().add(createColumn("id", "id", false));
    select.getColumn().add(createColumn("family_name", "name.first().family", false));
    select.getColumn().add(createColumn("given_names", "name.first().given", true));
    select.getColumn().add(createColumn("gender", "gender", false));
    view.getSelect().add(select);

    return view;
  }

  @Nonnull
  private ViewDefinitionResource createViewDefinitionWithNestedForEach() {
    final ViewDefinitionResource view = new ViewDefinitionResource();
    view.setId("nested-foreach-view");
    view.setName(new StringType("patient_contacts"));
    view.setResource(new CodeType("Patient"));
    view.setStatus(new CodeType("active"));

    // Root select with ID.
    final SelectComponent rootSelect = new SelectComponent();
    rootSelect.getColumn().add(createColumn("id", "id", false));
    view.getSelect().add(rootSelect);

    // Nested select with forEach over telecom.
    final SelectComponent nestedSelect = new SelectComponent();
    nestedSelect.setForEach(new StringType("telecom"));
    nestedSelect.getColumn().add(createColumn("phone_system", "system", false));
    nestedSelect.getColumn().add(createColumn("phone_value", "value", false));
    view.getSelect().add(nestedSelect);

    return view;
  }

  @Nonnull
  private ViewDefinitionResource createViewDefinitionWithUnionAll() {
    final ViewDefinitionResource view = new ViewDefinitionResource();
    view.setId("union-all-view");
    view.setName(new StringType("patient_identifiers"));
    view.setResource(new CodeType("Patient"));
    view.setStatus(new CodeType("active"));

    // Root select.
    final SelectComponent rootSelect = new SelectComponent();
    rootSelect.getColumn().add(createColumn("id", "id", false));

    // UnionAll selects for different identifier types.
    final SelectComponent unionSelect1 = new SelectComponent();
    unionSelect1.setForEach(new StringType("identifier.where(system = 'mrn')"));
    unionSelect1.getColumn().add(createColumn("identifier_type", "'MRN'", false));
    unionSelect1.getColumn().add(createColumn("identifier_value", "value", false));

    final SelectComponent unionSelect2 = new SelectComponent();
    unionSelect2.setForEach(new StringType("identifier.where(system = 'ssn')"));
    unionSelect2.getColumn().add(createColumn("identifier_type", "'SSN'", false));
    unionSelect2.getColumn().add(createColumn("identifier_value", "value", false));

    rootSelect.getUnionAll().add(unionSelect1);
    rootSelect.getUnionAll().add(unionSelect2);

    view.getSelect().add(rootSelect);

    return view;
  }

  @Nonnull
  private ColumnComponent createColumn(
      @Nonnull final String name, @Nonnull final String path, final boolean collection) {
    final ColumnComponent column = new ColumnComponent();
    column.setName(new StringType(name));
    column.setPath(new StringType(path));
    if (collection) {
      column.setCollection(new org.hl7.fhir.r4.model.BooleanType(true));
    }
    return column;
  }
}
