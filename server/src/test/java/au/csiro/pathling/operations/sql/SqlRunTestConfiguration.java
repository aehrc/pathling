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

package au.csiro.pathling.operations.sql;

import static au.csiro.pathling.operations.sqlquery.SqlLibraryParser.LIBRARY_TYPE_SYSTEM;
import static au.csiro.pathling.operations.sqlquery.SqlLibraryParser.SQL_QUERY_TYPE_CODE;
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
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Attachment;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.FHIRAllTypes;
import org.hl7.fhir.r4.model.Enumerations.PublicationStatus;
import org.hl7.fhir.r4.model.Library;
import org.hl7.fhir.r4.model.ParameterDefinition;
import org.hl7.fhir.r4.model.ParameterDefinition.ParameterUse;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.RelatedArtifact;
import org.hl7.fhir.r4.model.RelatedArtifact.RelatedArtifactType;
import org.hl7.fhir.r4.model.StringType;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

/**
 * Test configuration backing {@link SqlRunProviderIT}, substituting an in-memory data source that
 * holds the stored subjects the operation resolves and the Patient data they project.
 *
 * <p>The stored graph is:
 *
 * <ul>
 *   <li>{@code ViewDefinition/patient-view} - a Patient projection (id, family_name).
 *   <li>{@code Library/patients-by-family} - a SQLQuery over the ViewDefinition, declaring a {@code
 *       family} input parameter.
 *   <li>{@code Library/all-patients} - a SQLView over the ViewDefinition.
 * </ul>
 *
 * @author John Grimes
 */
@TestConfiguration
public class SqlRunTestConfiguration {

  /** The logical id of the stored Patient ViewDefinition. */
  public static final String PATIENT_VIEW_ID = "patient-view";

  /**
   * The canonical URL of the stored Patient ViewDefinition. Its final segment deliberately differs
   * from the logical id, so resolution by canonical cannot succeed by accident through the id.
   */
  public static final String PATIENT_VIEW_URL = "https://pathling.csiro.au/test/ViewDefinition/Pts";

  /** The logical id of the stored SQLQuery that filters patients by family name. */
  public static final String PATIENTS_BY_FAMILY_ID = "patients-by-family";

  /** The logical id of the stored SQLView over the Patient ViewDefinition. */
  public static final String ALL_PATIENTS_ID = "all-patients";

  /**
   * Returns the canonical URL of a stored Library given its logical id.
   *
   * @param id the Library's logical id
   * @return the Library's canonical URL
   */
  @Nonnull
  public static String libraryUrl(@Nonnull final String id) {
    return "https://pathling.csiro.au/test/Library/" + id;
  }

  /**
   * Substitutes the server's data source with an in-memory one holding the stored subjects and
   * Patient data.
   *
   * @param sparkSession the Spark session
   * @param pathlingContext the Pathling context
   * @param fhirEncoders the FHIR encoders
   * @return the in-memory data source
   */
  @Primary
  @Bean
  @Nonnull
  public QueryableDataSource deltaLake(
      @Nonnull final SparkSession sparkSession,
      @Nonnull final PathlingContext pathlingContext,
      @Nonnull final FhirEncoders fhirEncoders) {
    final List<IBaseResource> resources = new ArrayList<>();
    resources.add(patientView());
    resources.add(patientsByFamily());
    resources.add(allPatients());
    resources.add(patient("p1", "Smith"));
    resources.add(patient("p2", "Johnson"));
    resources.add(patient("p3", "Williams"));
    return new CustomObjectDataSource(sparkSession, pathlingContext, fhirEncoders, resources);
  }

  /** Builds the stored Patient ViewDefinition. */
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

  /** Builds the stored SQLQuery, which binds a family name supplied at request time. */
  @Nonnull
  private static Library patientsByFamily() {
    final Library library =
        sqlLibrary(
            PATIENTS_BY_FAMILY_ID,
            SQL_QUERY_TYPE_CODE,
            "SELECT id, family_name FROM pv WHERE family_name = :family",
            Map.of("pv", PATIENT_VIEW_URL));
    library.addParameter(
        new ParameterDefinition()
            .setName("family")
            .setUse(ParameterUse.IN)
            .setType(FHIRAllTypes.STRING.toCode()));
    return library;
  }

  /** Builds the stored SQLView over the Patient ViewDefinition. */
  @Nonnull
  private static Library allPatients() {
    return sqlLibrary(
        ALL_PATIENTS_ID,
        SQL_VIEW_TYPE_CODE,
        "SELECT id, family_name FROM pv",
        Map.of("pv", PATIENT_VIEW_URL));
  }

  /** Builds a SQL Library of the given type code, SQL text and depends-on dependencies. */
  @Nonnull
  private static Library sqlLibrary(
      @Nonnull final String id,
      @Nonnull final String typeCode,
      @Nonnull final String sql,
      @Nonnull final Map<String, String> dependenciesByLabel) {
    final Library library = new Library();
    library.setId(id);
    library.setUrl(libraryUrl(id));
    library.setStatus(PublicationStatus.ACTIVE);
    library.setType(
        new CodeableConcept()
            .addCoding(new Coding().setSystem(LIBRARY_TYPE_SYSTEM).setCode(typeCode)));
    final Attachment content = new Attachment();
    content.setContentType("application/sql");
    content.setData(sql.getBytes(StandardCharsets.UTF_8));
    library.addContent(content);
    dependenciesByLabel.forEach(
        (label, resource) ->
            library.addRelatedArtifact(
                new RelatedArtifact()
                    .setType(RelatedArtifactType.DEPENDSON)
                    .setLabel(label)
                    .setResource(resource)));
    return library;
  }

  /** Builds a ViewDefinition column with the given name and path. */
  @Nonnull
  private static ColumnComponent column(@Nonnull final String name, @Nonnull final String path) {
    final ColumnComponent column = new ColumnComponent();
    column.setName(new StringType(name));
    column.setPath(new StringType(path));
    return column;
  }

  /** Builds a Patient with the given id and family name. */
  @Nonnull
  private static Patient patient(@Nonnull final String id, @Nonnull final String family) {
    final Patient patient = new Patient();
    patient.setId(id);
    patient.addName().setFamily(family);
    return patient;
  }
}
