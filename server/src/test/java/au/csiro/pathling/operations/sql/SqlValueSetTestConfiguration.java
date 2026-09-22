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
import au.csiro.pathling.test.Rf2Mini;
import au.csiro.pathling.util.CustomObjectDataSource;
import jakarta.annotation.Nonnull;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Attachment;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
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
 * Test configuration backing the value set integration tests, substituting an in-memory data source
 * that holds a stored ViewDefinition over {@code Condition}, a stored SQLView that semi-joins it to
 * the cardiovascular disease value set, the Condition data they project and the Patients those
 * Conditions belong to.
 *
 * <p>The stored graph is {@code ViewDefinition/condition-view}, projecting {@code id}, {@code
 * patient_id}, {@code system} and {@code code} from each Condition's first coding, and {@code
 * Library/cvd-conditions}, a SQLView keeping the rows of that view whose code is a member of the
 * cardiovascular disease value set. The Conditions carry two groups of codes: the SNOMED CT codes
 * of the specification's cardiovascular disease example, which the SERVER-mode test resolves
 * through a stubbed terminology server, and synthetic {@code rf2-mini} concepts, which the
 * LOCAL-mode test resolves through a local store. The Patients exist so that the {@code patient}
 * filter can name them.
 *
 * @author John Grimes
 */
@TestConfiguration
public class SqlValueSetTestConfiguration {

  /** The logical id of the stored Condition ViewDefinition. */
  public static final String CONDITION_VIEW_ID = "condition-view";

  /**
   * The canonical URL of the stored Condition ViewDefinition. Its final segment differs from the
   * logical id, so resolution by canonical cannot succeed by accident through the id.
   */
  public static final String CONDITION_VIEW_URL =
      "https://pathling.csiro.au/test/ViewDefinition/Conditions";

  /** The SNOMED CT system URI. */
  public static final String SNOMED = "http://snomed.info/sct";

  /** A SNOMED CT code for myocardial infarction, a member of the cardiovascular disease example. */
  public static final String MYOCARDIAL_INFARCTION = "22298006";

  /** A SNOMED CT code for diabetes mellitus, not a member of the cardiovascular disease example. */
  public static final String DIABETES_MELLITUS = "73211009";

  /** The canonical URL of the cardiovascular disease value set of the specification's example. */
  public static final String CVD_URL = "http://example.org/ValueSet/cardiovascular-disease";

  /** The pinned version of the cardiovascular disease value set. */
  public static final String CVD_VERSION = "2026";

  /** The logical id of the stored SQLView semi-joining the Condition view to the value set. */
  public static final String CVD_CONDITIONS_ID = "cvd-conditions";

  /** The canonical URL of the stored SQLView semi-joining the Condition view to the value set. */
  public static final String CVD_CONDITIONS_URL =
      "https://pathling.csiro.au/test/Library/" + CVD_CONDITIONS_ID;

  /**
   * Substitutes the server's data source with an in-memory one holding the stored ViewDefinition
   * and the Condition data.
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
    resources.add(cvdConditions());
    resources.add(patient("p1"));
    resources.add(patient("p2"));
    resources.add(patient("p3"));
    // The specification's example: two myocardial infarctions on different patients and one
    // diabetes, so a semi-join to the cardiovascular disease value set keeps exactly two rows.
    resources.add(condition("c1", "p1", MYOCARDIAL_INFARCTION));
    resources.add(condition("c2", "p2", DIABETES_MELLITUS));
    resources.add(condition("c3", "p3", MYOCARDIAL_INFARCTION));
    // The rf2-mini concepts: two descendants of type 2 diabetes and one hypertension, so a
    // semi-join
    // to the implicit value set under type 2 diabetes keeps exactly two rows.
    resources.add(condition("c4", "p1", Rf2Mini.TYPE2_DIABETES));
    resources.add(condition("c5", "p2", Rf2Mini.TYPE2_WITH_COMPLICATION));
    resources.add(condition("c6", "p3", Rf2Mini.HYPERTENSION));
    return new CustomObjectDataSource(sparkSession, pathlingContext, fhirEncoders, resources);
  }

  /** Builds the stored Condition ViewDefinition. */
  @Nonnull
  private static ViewDefinitionResource conditionView() {
    final ViewDefinitionResource view = new ViewDefinitionResource();
    view.setId(CONDITION_VIEW_ID);
    view.setUrl(CONDITION_VIEW_URL);
    view.setName(new StringType("condition_view"));
    view.setResource(new CodeType("Condition"));
    view.setStatus(new CodeType("active"));
    final SelectComponent select = new SelectComponent();
    select.getColumn().add(column("id", "id"));
    select.getColumn().add(column("patient_id", "subject.reference"));
    select.getColumn().add(column("system", "code.coding.first().system"));
    select.getColumn().add(column("code", "code.coding.first().code"));
    view.getSelect().add(select);
    return view;
  }

  /**
   * Builds the stored SQLView that keeps the Condition view's rows whose code is a member of the
   * cardiovascular disease value set, pinned to its version.
   */
  @Nonnull
  private static Library cvdConditions() {
    final Library library = new Library();
    library.setId(CVD_CONDITIONS_ID);
    library.setUrl(CVD_CONDITIONS_URL);
    library.setStatus(PublicationStatus.ACTIVE);
    library.setType(
        new CodeableConcept()
            .addCoding(new Coding().setSystem(LIBRARY_TYPE_SYSTEM).setCode(SQL_VIEW_TYPE_CODE)));
    final Attachment content = new Attachment();
    content.setContentType("application/sql");
    content.setData(
        ("SELECT conditions.patient_id, conditions.code FROM conditions"
                + " WHERE EXISTS (SELECT 1 FROM cvd_codes"
                + " WHERE cvd_codes.system = conditions.system"
                + " AND cvd_codes.code = conditions.code)")
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
            .setLabel("cvd_codes")
            .setResource(CVD_URL + "|" + CVD_VERSION));
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

  /** Builds a Condition on the given patient carrying one SNOMED CT coding. */
  @Nonnull
  private static Condition condition(
      @Nonnull final String id, @Nonnull final String patientId, @Nonnull final String code) {
    final Condition condition = new Condition();
    condition.setId(id);
    condition.setSubject(new Reference("Patient/" + patientId));
    condition.getCode().addCoding().setSystem(SNOMED).setCode(code);
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
