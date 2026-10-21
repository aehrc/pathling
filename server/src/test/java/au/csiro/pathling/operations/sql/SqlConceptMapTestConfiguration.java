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
import static au.csiro.pathling.operations.sqlquery.SqlLibraryParser.SQL_VIEW_TYPE_CODE;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.encoders.ViewDefinitionResource;
import au.csiro.pathling.encoders.ViewDefinitionResource.ColumnComponent;
import au.csiro.pathling.encoders.ViewDefinitionResource.SelectComponent;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.util.CustomObjectDataSource;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Attachment;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.ConceptMap;
import org.hl7.fhir.r4.model.Condition;
import org.hl7.fhir.r4.model.Enumerations.PublicationStatus;
import org.hl7.fhir.r4.model.Library;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Reference;
import org.hl7.fhir.r4.model.RelatedArtifact;
import org.hl7.fhir.r4.model.RelatedArtifact.RelatedArtifactType;
import org.hl7.fhir.r4.model.StringType;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

/**
 * Test configuration backing the concept map integration tests, substituting an in-memory data
 * source that holds the stored ViewDefinition over {@code Condition} of the specification's worked
 * example, a stored SQLView translating it through the worked example concept map, the four
 * Conditions it projects and the Patients they belong to.
 *
 * <p>The stored ViewDefinition projects {@code id}, {@code patient_id}, {@code system}, {@code
 * version} and {@code code} from each Condition's first coding. The Conditions are those of the
 * worked example: {@code p1} has myocardial infarction and {@code p3} diabetes mellitus, both of
 * which the worked example concept map translates; {@code p2} is already coded in ICD-10, so the
 * map is silent about it; and {@code p4} is fit and well, which the map states has no mapping.
 *
 * <p>The stored SQLView, {@code Library/translated-conditions}, left-joins that ViewDefinition to
 * version {@code 2026} of the worked example concept map, projecting {@code id}, {@code
 * patient_id}, {@code code}, {@code target_system}, {@code target_code} and {@code relationship}.
 *
 * @author John Grimes
 */
@TestConfiguration
public class SqlConceptMapTestConfiguration {

  /** The logical id of the stored Condition ViewDefinition. */
  public static final String CONDITION_VIEW_ID = "conditions";

  /** The canonical URL of the stored Condition ViewDefinition. */
  public static final String CONDITION_VIEW_URL = "http://example.org/ViewDefinition/conditions";

  /** The SNOMED CT system URI. */
  public static final String SNOMED = "http://snomed.info/sct";

  /** The ICD-10 system URI. */
  public static final String ICD10 = "http://hl7.org/fhir/sid/icd-10";

  /** A SNOMED CT code for myocardial infarction, which the worked example maps to I21. */
  public static final String MYOCARDIAL_INFARCTION = "22298006";

  /** A SNOMED CT code for diabetes mellitus, which the worked example maps to E14. */
  public static final String DIABETES_MELLITUS = "73211009";

  /** A SNOMED CT code for fit and well, which the worked example states has no mapping. */
  public static final String FIT_AND_WELL = "102499006";

  /** An ICD-10 code for acute myocardial infarction. */
  public static final String ACUTE_MYOCARDIAL_INFARCTION = "I21";

  /** The canonical URL of the worked example concept map. */
  public static final String SCT_TO_ICD10_URL = "http://example.org/ConceptMap/sct-to-icd10";

  /** The classpath location of the worked example concept map. */
  private static final String CONCEPT_MAP_RESOURCE = "/conceptmap/sct-to-icd10.ConceptMap.json";

  /** The logical id of the stored SQLView translating the Condition view through the map. */
  public static final String TRANSLATED_CONDITIONS_ID = "translated-conditions";

  /** The canonical URL of the stored SQLView translating the Condition view through the map. */
  public static final String TRANSLATED_CONDITIONS_URL =
      "https://pathling.csiro.au/test/Library/" + TRANSLATED_CONDITIONS_ID;

  /**
   * Substitutes the server's data source with an in-memory one holding the stored ViewDefinition,
   * the stored SQLView, the Condition data and the Patients.
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
    resources.add(conditionView());
    resources.add(translatedConditions());
    for (final String patientId : List.of("p1", "p2", "p3", "p4")) {
      resources.add(patient(patientId));
    }
    resources.add(condition("c1", "p1", SNOMED, MYOCARDIAL_INFARCTION));
    resources.add(condition("c2", "p2", ICD10, ACUTE_MYOCARDIAL_INFARCTION));
    resources.add(condition("c3", "p3", SNOMED, DIABETES_MELLITUS));
    resources.add(condition("c4", "p4", SNOMED, FIT_AND_WELL));
    return new CustomObjectDataSource(sparkSession, pathlingContext, fhirEncoders, resources);
  }

  /**
   * Parses the worked example concept map, version {@code 2026}, from the test classpath. Each call
   * returns a fresh copy that the caller may edit.
   *
   * @param fhirContext the FHIR context
   * @return the concept map
   */
  @Nonnull
  public static ConceptMap workedExample(@Nonnull final FhirContext fhirContext) {
    try (final InputStream stream =
        SqlConceptMapTestConfiguration.class.getResourceAsStream(CONCEPT_MAP_RESOURCE)) {
      if (stream == null) {
        throw new IllegalStateException("Fixture not found: " + CONCEPT_MAP_RESOURCE);
      }
      return (ConceptMap)
          fhirContext
              .newJsonParser()
              .parseResource(new String(stream.readAllBytes(), StandardCharsets.UTF_8));
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Builds the stored Condition ViewDefinition. */
  @Nonnull
  private static ViewDefinitionResource conditionView() {
    final ViewDefinitionResource view = new ViewDefinitionResource();
    view.setId(CONDITION_VIEW_ID);
    view.setUrl(CONDITION_VIEW_URL);
    view.setName(new StringType("conditions"));
    view.setResource(new CodeType("Condition"));
    view.setStatus(new CodeType("active"));
    final SelectComponent select = new SelectComponent();
    select.getColumn().add(column("id", "id"));
    select.getColumn().add(column("patient_id", "subject.reference"));
    select.getColumn().add(column("system", "code.coding.first().system"));
    select.getColumn().add(column("version", "code.coding.first().version"));
    select.getColumn().add(column("code", "code.coding.first().code"));
    view.getSelect().add(select);
    return view;
  }

  /**
   * Builds the stored SQLView that left-joins the Condition view to version {@code 2026} of the
   * worked example concept map on system and code, as the specification's worked example does.
   */
  @Nonnull
  private static Library translatedConditions() {
    final Library library = new Library();
    library.setId(TRANSLATED_CONDITIONS_ID);
    library.setUrl(TRANSLATED_CONDITIONS_URL);
    library.setStatus(PublicationStatus.ACTIVE);
    library.setType(
        new CodeableConcept()
            .addCoding(new Coding().setSystem(LIBRARY_TYPE_SYSTEM).setCode(SQL_VIEW_TYPE_CODE)));
    final Attachment content = new Attachment();
    content.setContentType("application/sql");
    content.setData(
        ("SELECT conditions.id, conditions.patient_id, conditions.code,"
                + " sct_to_icd10.target_system, sct_to_icd10.target_code,"
                + " sct_to_icd10.relationship"
                + " FROM conditions"
                + " LEFT JOIN sct_to_icd10"
                + " ON sct_to_icd10.source_system = conditions.system"
                + " AND sct_to_icd10.source_code = conditions.code"
                + " AND (sct_to_icd10.relationship IS NULL"
                + " OR sct_to_icd10.relationship <> 'not-related-to')")
            .getBytes(StandardCharsets.UTF_8));
    library.addContent(content);
    library.addRelatedArtifact(
        new RelatedArtifact()
            .setType(RelatedArtifactType.DEPENDSON)
            .setLabel("conditions")
            .setResource(CONDITION_VIEW_URL));
    library.addRelatedArtifact(
        new RelatedArtifact()
            .setType(RelatedArtifactType.DEPENDSON)
            .setLabel("sct_to_icd10")
            .setResource(SCT_TO_ICD10_URL + "|2026"));
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

  /** Builds a Condition on the given patient carrying one coding. */
  @Nonnull
  private static Condition condition(
      @Nonnull final String id,
      @Nonnull final String patientId,
      @Nonnull final String system,
      @Nonnull final String code) {
    final Condition condition = new Condition();
    condition.setId(id);
    condition.setSubject(new Reference("Patient/" + patientId));
    condition.getCode().addCoding().setSystem(system).setCode(code);
    return condition;
  }

  /** Builds a Patient with the given id, so that the {@code patient} filter can resolve it. */
  @Nonnull
  private static Patient patient(@Nonnull final String id) {
    final Patient patient = new Patient();
    patient.setId(id);
    return patient;
  }
}
