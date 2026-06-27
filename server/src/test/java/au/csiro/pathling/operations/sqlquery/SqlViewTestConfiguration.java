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
import static au.csiro.pathling.operations.sqlquery.SqlLibraryParser.SQL_VIEW_TYPE_CODE;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.encoders.ViewDefinitionResource;
import au.csiro.pathling.encoders.ViewDefinitionResource.ColumnComponent;
import au.csiro.pathling.encoders.ViewDefinitionResource.SelectComponent;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.util.CustomObjectDataSource;
import jakarta.annotation.Nonnull;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Attachment;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.PublicationStatus;
import org.hl7.fhir.r4.model.Library;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.RelatedArtifact;
import org.hl7.fhir.r4.model.RelatedArtifact.RelatedArtifactType;
import org.hl7.fhir.r4.model.StringType;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

/**
 * Test configuration that overrides the production {@code deltaLake} data source with an in-memory
 * one pre-loaded with FHIR resources, ViewDefinitions, and SQLView {@code Library} resources. Used
 * by the SQLView end-to-end ITs to drive queries that resolve a SQLView dependency, a nested chain,
 * a diamond, and a cycle, against real FHIR data.
 *
 * <p>The stored graph is:
 *
 * <ul>
 *   <li>{@code ViewDefinition/patient-view} - Patient projection (id, family_name).
 *   <li>{@code Library/active-patients} - SQLView over {@code patient-view}.
 *   <li>{@code Library/refined-patients} - SQLView over {@code active-patients} (a nested chain).
 *   <li>{@code Library/cycle-a} / {@code Library/cycle-b} - a mutually-referencing cycle.
 * </ul>
 *
 * @author John Grimes
 */
@TestConfiguration
public class SqlViewTestConfiguration {

  /** Base canonical URL for test ViewDefinitions. */
  private static final String VIEW_DEFINITION_BASE =
      "https://pathling.csiro.au/test/ViewDefinition/";

  /** Base canonical URL for test SQLView Libraries. */
  private static final String LIBRARY_BASE = "https://pathling.csiro.au/test/Library/";

  /** The id of the pre-loaded Patient ViewDefinition. */
  public static final String PATIENT_VIEW_ID = "patient-view";

  /**
   * The canonical URL of the pre-loaded Patient ViewDefinition. The URL's final segment ({@code
   * Patients}) differs from the logical id, so dependencies that resolve it by URL exercise the
   * case the id-based resolution could not handle.
   */
  public static final String PATIENT_VIEW_URL = VIEW_DEFINITION_BASE + "Patients";

  /**
   * Returns the canonical URL of a stored SQLView Library given its local id.
   *
   * @param id the SQLView's logical id
   * @return the SQLView's canonical url
   */
  @Nonnull
  public static String libraryUrl(@Nonnull final String id) {
    return LIBRARY_BASE + id;
  }

  /** The id of the SQLView over the Patient ViewDefinition. */
  public static final String ACTIVE_PATIENTS_ID = "active-patients";

  /** The id of the SQLView over {@link #ACTIVE_PATIENTS_ID} (a nested chain). */
  public static final String REFINED_PATIENTS_ID = "refined-patients";

  /** The id of one half of a mutually-referencing cycle. */
  public static final String CYCLE_A_ID = "cycle-a";

  /** The id of the other half of a mutually-referencing cycle. */
  public static final String CYCLE_B_ID = "cycle-b";

  /** The id of the SQLView shared by both arms of a diamond. */
  public static final String SHARED_PATIENTS_ID = "shared-patients";

  /** The id of the left arm of a diamond, over {@link #SHARED_PATIENTS_ID}. */
  public static final String LEFT_PATIENTS_ID = "left-patients";

  /** The id of the right arm of a diamond, over {@link #SHARED_PATIENTS_ID}. */
  public static final String RIGHT_PATIENTS_ID = "right-patients";

  @Primary
  @Bean
  @Nonnull
  public QueryableDataSource deltaLake(
      @Nonnull final SparkSession sparkSession,
      @Nonnull final PathlingContext pathlingContext,
      @Nonnull final FhirEncoders fhirEncoders) {
    final List<IBaseResource> resources = new ArrayList<>();
    resources.add(patientView());
    resources.add(
        sqlView(
            ACTIVE_PATIENTS_ID,
            "SELECT id, family_name FROM patient_view",
            Map.of("patient_view", PATIENT_VIEW_URL)));
    resources.add(
        sqlView(
            REFINED_PATIENTS_ID,
            "SELECT id, family_name FROM ap WHERE family_name <> 'Johnson'",
            Map.of("ap", libraryUrl(ACTIVE_PATIENTS_ID))));
    resources.add(sqlView(CYCLE_A_ID, "SELECT * FROM b", Map.of("b", libraryUrl(CYCLE_B_ID))));
    resources.add(sqlView(CYCLE_B_ID, "SELECT * FROM a", Map.of("a", libraryUrl(CYCLE_A_ID))));
    resources.add(
        sqlView(
            SHARED_PATIENTS_ID,
            "SELECT id, family_name FROM patient_view",
            Map.of("patient_view", PATIENT_VIEW_URL)));
    resources.add(
        sqlView(
            LEFT_PATIENTS_ID,
            "SELECT id, family_name FROM sp",
            Map.of("sp", libraryUrl(SHARED_PATIENTS_ID))));
    resources.add(
        sqlView(
            RIGHT_PATIENTS_ID, "SELECT id FROM sp", Map.of("sp", libraryUrl(SHARED_PATIENTS_ID))));
    resources.add(patient("p1", "Smith"));
    resources.add(patient("p2", "Johnson"));
    resources.add(patient("p3", "Williams"));
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

  /**
   * Builds a stored SQLView Library with the given id, SQL, and depends-on dependencies (label to
   * resource reference, iteration order preserved).
   */
  @Nonnull
  private static Library sqlView(
      @Nonnull final String id,
      @Nonnull final String sql,
      @Nonnull final Map<String, String> dependenciesByLabel) {
    final Library library = new Library();
    library.setId(id);
    library.setUrl(libraryUrl(id));
    library.setStatus(PublicationStatus.ACTIVE);
    library.setType(
        new CodeableConcept()
            .addCoding(new Coding().setSystem(LIBRARY_TYPE_SYSTEM).setCode(SQL_VIEW_TYPE_CODE)));
    final Attachment content = new Attachment();
    content.setContentType("application/sql");
    content.setData(sql.getBytes(StandardCharsets.UTF_8));
    library.addContent(content);
    new LinkedHashMap<>(dependenciesByLabel)
        .forEach(
            (label, resource) ->
                library.addRelatedArtifact(
                    new RelatedArtifact()
                        .setType(RelatedArtifactType.DEPENDSON)
                        .setLabel(label)
                        .setResource(resource)));
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
  private static Patient patient(@Nonnull final String id, @Nonnull final String family) {
    final Patient patient = new Patient();
    patient.setId(id);
    patient.addName().setFamily(family);
    return patient;
  }
}
