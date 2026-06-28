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

package au.csiro.pathling.operations.sqlquery;

import static au.csiro.pathling.operations.sqlquery.SqlLibraryParser.LIBRARY_TYPE_SYSTEM;
import static au.csiro.pathling.operations.sqlquery.SqlLibraryParser.SQL_QUERY_TYPE_CODE;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.encoders.ViewDefinitionResource;
import au.csiro.pathling.encoders.ViewDefinitionResource.ColumnComponent;
import au.csiro.pathling.encoders.ViewDefinitionResource.SelectComponent;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.util.CustomObjectDataSource;
import jakarta.annotation.Nonnull;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Attachment;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.PublicationStatus;
import org.hl7.fhir.r4.model.Library;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Observation.ObservationStatus;
import org.hl7.fhir.r4.model.ParameterDefinition;
import org.hl7.fhir.r4.model.ParameterDefinition.ParameterUse;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Quantity;
import org.hl7.fhir.r4.model.Reference;
import org.hl7.fhir.r4.model.RelatedArtifact;
import org.hl7.fhir.r4.model.RelatedArtifact.RelatedArtifactType;
import org.hl7.fhir.r4.model.StringType;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

/**
 * Test configuration that overrides the production {@code deltaLake} data source with an in-memory
 * one pre-loaded with FHIR data, ViewDefinitions, and stored SQLQuery {@code Library} resources,
 * for the {@code $sqlquery-export} integration tests. The stored Libraries reference stored
 * ViewDefinitions via {@code relatedArtifact}, exercising the full storage-backed resolution path
 * the production warehouse caching otherwise defeats for runtime-created resources.
 *
 * @author John Grimes
 */
@TestConfiguration
public class SqlQueryExportTestConfiguration {

  /** Id of the stored Patient ViewDefinition referenced by the patient query. */
  public static final String PATIENT_VIEW_ID = "patient-bp";

  /** Canonical URL of the stored Patient ViewDefinition (final segment differs from its id). */
  public static final String PATIENT_VIEW_URL =
      "https://pathling.csiro.au/test/ViewDefinition/PatientBp";

  /** Id of the stored Observation ViewDefinition referenced by the observation query. */
  public static final String OBSERVATION_VIEW_ID = "observation-weight";

  /** Canonical URL of the stored Observation ViewDefinition. */
  public static final String OBSERVATION_VIEW_URL =
      "https://pathling.csiro.au/test/ViewDefinition/ObservationWeight";

  /** Id of the stored SQLQuery Library that selects patients. */
  public static final String PATIENT_QUERY_ID = "patient-bp-query";

  /** Id of the stored SQLQuery Library that selects observations. */
  public static final String OBSERVATION_QUERY_ID = "observation-weight-query";

  /** Id of the stored SQLQuery Library that declares a {@code familyName} parameter. */
  public static final String PARAM_QUERY_ID = "patient-by-family-query";

  /** Id of the stored Group whose sole member is patient {@code p1}. */
  public static final String GROUP_ID = "g1";

  /** The {@code meta.lastUpdated} instant of patient {@code p1} (older than the others). */
  public static final String P1_LAST_UPDATED = "2020-01-01T00:00:00.000Z";

  @Primary
  @Bean
  @Nonnull
  public QueryableDataSource deltaLake(
      @Nonnull final SparkSession sparkSession,
      @Nonnull final PathlingContext pathlingContext,
      @Nonnull final FhirEncoders fhirEncoders) {
    final List<IBaseResource> resources = new ArrayList<>();
    resources.add(patientView());
    resources.add(observationView());
    resources.add(
        sqlLibrary(
            PATIENT_QUERY_ID,
            "patient_bp_query",
            "SELECT id, family_name FROM patients ORDER BY id",
            "patients",
            PATIENT_VIEW_URL));
    resources.add(
        sqlLibrary(
            OBSERVATION_QUERY_ID,
            "observation_weight_query",
            "SELECT id, subject, weight_kg FROM observations ORDER BY id",
            "observations",
            OBSERVATION_VIEW_URL));
    resources.add(parameterisedPatientQuery());
    // p1 has an older meta.lastUpdated than p2/p3, so a `_since` filter can scope it out.
    resources.add(patient("p1", "Smith", P1_LAST_UPDATED));
    resources.add(patient("p2", "Johnson", "2025-01-01T00:00:00.000Z"));
    resources.add(patient("p3", "Williams", "2025-01-01T00:00:00.000Z"));
    resources.add(observation("o1", "p1", new BigDecimal("70.50")));
    resources.add(observation("o2", "p2", new BigDecimal("82.250")));
    resources.add(observation("o3", "p3", new BigDecimal("65")));
    // A Group whose sole member is p1, for the group-filter test.
    resources.add(group(GROUP_ID, "p1"));
    return new CustomObjectDataSource(sparkSession, pathlingContext, fhirEncoders, resources);
  }

  @Nonnull
  private static ViewDefinitionResource patientView() {
    final ViewDefinitionResource view = new ViewDefinitionResource();
    view.setId(PATIENT_VIEW_ID);
    view.setUrl(PATIENT_VIEW_URL);
    view.setName(new StringType("patient_view"));
    view.setResource(new CodeType("Patient"));
    view.setStatus(new CodeType("active"));
    final SelectComponent select = new SelectComponent();
    select.getColumn().add(column("id", "id"));
    select.getColumn().add(column("family_name", "name.first().family"));
    view.getSelect().add(select);
    return view;
  }

  @Nonnull
  private static ViewDefinitionResource observationView() {
    final ViewDefinitionResource view = new ViewDefinitionResource();
    view.setId(OBSERVATION_VIEW_ID);
    view.setUrl(OBSERVATION_VIEW_URL);
    view.setName(new StringType("observation_view"));
    view.setResource(new CodeType("Observation"));
    view.setStatus(new CodeType("active"));
    final SelectComponent select = new SelectComponent();
    select.getColumn().add(column("id", "id"));
    select.getColumn().add(column("subject", "subject.reference"));
    select.getColumn().add(column("weight_kg", "value.ofType(Quantity).value.toString()"));
    view.getSelect().add(select);
    return view;
  }

  /** A stored SQLQuery Library declaring a {@code familyName} string parameter. */
  @Nonnull
  private static Library parameterisedPatientQuery() {
    final Library library =
        sqlLibrary(
            PARAM_QUERY_ID,
            "patient_by_family_query",
            "SELECT id, family_name FROM patients WHERE family_name = :familyName",
            "patients",
            PATIENT_VIEW_URL);
    library.addParameter(
        new ParameterDefinition().setName("familyName").setUse(ParameterUse.IN).setType("string"));
    return library;
  }

  /** Builds a stored SQLQuery Library referencing a single ViewDefinition. */
  @Nonnull
  static Library sqlLibrary(
      @Nonnull final String id,
      @Nonnull final String name,
      @Nonnull final String sql,
      @Nonnull final String label,
      @Nonnull final String viewReference) {
    final Library library = new Library();
    library.setId(id);
    library.setName(name);
    library.setStatus(PublicationStatus.ACTIVE);
    library.setType(
        new CodeableConcept()
            .addCoding(new Coding().setSystem(LIBRARY_TYPE_SYSTEM).setCode(SQL_QUERY_TYPE_CODE)));
    final Attachment content = new Attachment();
    content.setContentType("application/sql");
    content.setData(sql.getBytes(StandardCharsets.UTF_8));
    library.addContent(content);
    library.addRelatedArtifact(
        new RelatedArtifact()
            .setType(RelatedArtifactType.DEPENDSON)
            .setLabel(label)
            .setResource(viewReference));
    return library;
  }

  @Nonnull
  private static ColumnComponent column(@Nonnull final String name, @Nonnull final String path) {
    final ColumnComponent column = new ColumnComponent();
    column.setName(new StringType(name));
    column.setPath(new StringType(path));
    return column;
  }

  @Nonnull
  private static Patient patient(
      @Nonnull final String id, @Nonnull final String family, @Nonnull final String lastUpdated) {
    final Patient patient = new Patient();
    patient.setId(id);
    patient.addName().setFamily(family);
    patient.getMeta().setLastUpdatedElement(new org.hl7.fhir.r4.model.InstantType(lastUpdated));
    return patient;
  }

  @Nonnull
  private static org.hl7.fhir.r4.model.Group group(
      @Nonnull final String id, @Nonnull final String memberPatientId) {
    final org.hl7.fhir.r4.model.Group fhirGroup = new org.hl7.fhir.r4.model.Group();
    fhirGroup.setId(id);
    fhirGroup.setType(org.hl7.fhir.r4.model.Group.GroupType.PERSON);
    fhirGroup.setActual(true);
    fhirGroup.addMember().setEntity(new Reference("Patient/" + memberPatientId));
    return fhirGroup;
  }

  @Nonnull
  private static Observation observation(
      @Nonnull final String id, @Nonnull final String patientId, @Nonnull final BigDecimal weight) {
    final Observation observation = new Observation();
    observation.setId(id);
    observation.setStatus(ObservationStatus.FINAL);
    observation.setSubject(new Reference("Patient/" + patientId));
    final Quantity quantity = new Quantity();
    quantity.setValue(weight);
    quantity.setUnit("kg");
    quantity.setSystem("http://unitsofmeasure.org");
    quantity.setCode("kg");
    observation.setValue(quantity);
    return observation;
  }
}
