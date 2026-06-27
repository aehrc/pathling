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
import static au.csiro.pathling.operations.sqlquery.SqlLibraryParser.SQL_VIEW_TYPE_CODE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.nio.charset.StandardCharsets;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Enumerations.PublicationStatus;
import org.hl7.fhir.r4.model.Library;
import org.hl7.fhir.r4.model.ParameterDefinition.ParameterUse;
import org.hl7.fhir.r4.model.RelatedArtifact;
import org.hl7.fhir.r4.model.RelatedArtifact.RelatedArtifactType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Unit tests for {@link SqlLibraryParser} covering both the SQLQuery and SQLView profiles. */
class SqlLibraryParserTest {

  private SqlLibraryParser parser;

  @BeforeEach
  void setUp() {
    parser = new SqlLibraryParser();
  }

  @Test
  void extractsSqlFromLibrary() {
    final Library library = createMinimalLibrary("SELECT * FROM patients");
    final ParsedSqlQuery result = parser.parse(library);
    assertThat(result.getSql()).isEqualTo("SELECT * FROM patients");
  }

  @Test
  void extractsViewReferences() {
    final Library library = createMinimalLibrary("SELECT * FROM patients");
    library.addRelatedArtifact(
        new RelatedArtifact()
            .setType(RelatedArtifactType.DEPENDSON)
            .setLabel("patients")
            .setResource("https://example.org/ViewDefinition/patient-view"));
    library.addRelatedArtifact(
        new RelatedArtifact()
            .setType(RelatedArtifactType.DEPENDSON)
            .setLabel("observations")
            .setResource("https://example.org/ViewDefinition/obs-view"));

    final ParsedSqlQuery result = parser.parse(library);

    assertThat(result.getViewReferences()).hasSize(2);
    assertThat(result.getViewReferences().get(0).getLabel()).isEqualTo("patients");
    assertThat(result.getViewReferences().get(0).getCanonicalUrl())
        .isEqualTo("https://example.org/ViewDefinition/patient-view");
    assertThat(result.getViewReferences().get(1).getLabel()).isEqualTo("observations");
    assertThat(result.getViewReferences().get(1).getCanonicalUrl())
        .isEqualTo("https://example.org/ViewDefinition/obs-view");
  }

  @Test
  void extractsParameters() {
    final Library library = createMinimalLibrary("SELECT * FROM t WHERE age > :min_age");
    library.addParameter().setName("min_age").setType("integer").setUse(ParameterUse.IN);

    final ParsedSqlQuery result = parser.parse(library);

    assertThat(result.getDeclaredParameters()).hasSize(1);
    assertThat(result.getDeclaredParameters().get(0).getName()).isEqualTo("min_age");
    assertThat(result.getDeclaredParameters().get(0).getType()).isEqualTo("integer");
  }

  @Test
  void handlesEmptyViewReferencesAndParameters() {
    final Library library = createMinimalLibrary("SELECT 1");
    final ParsedSqlQuery result = parser.parse(library);

    assertThat(result.getSql()).isEqualTo("SELECT 1");
    assertThat(result.getViewReferences()).isEmpty();
    assertThat(result.getDeclaredParameters()).isEmpty();
  }

  @Test
  void reportsSqlQueryAsNotAView() {
    final ParsedSqlQuery result = parser.parse(createMinimalLibrary("SELECT 1"));
    assertThat(result.getLibraryTypeCode()).isEqualTo(SQL_QUERY_TYPE_CODE);
    assertThat(result.isView()).isFalse();
  }

  // ---------------------------------------------------------------------------
  // SQLView profile.
  // ---------------------------------------------------------------------------

  @Test
  void parsesSqlViewWithTypeCodeSqlAndDependencies() {
    final Library library = SqlLibraryFixtures.sqlView("SELECT * FROM patient_view");
    SqlLibraryFixtures.addDependency(
        library, "patient_view", "https://example.org/ViewDefinition/patient-view");
    SqlLibraryFixtures.addDependency(library, "base", "https://example.org/Library/base");

    final ParsedSqlQuery result = parser.parse(library);

    assertThat(result.getLibraryTypeCode()).isEqualTo(SQL_VIEW_TYPE_CODE);
    assertThat(result.isView()).isTrue();
    assertThat(result.getSql()).isEqualTo("SELECT * FROM patient_view");
    assertThat(result.getViewReferences()).hasSize(2);
    assertThat(result.getViewReferences().get(0).getLabel()).isEqualTo("patient_view");
    assertThat(result.getViewReferences().get(0).getCanonicalUrl())
        .isEqualTo("https://example.org/ViewDefinition/patient-view");
    assertThat(result.getViewReferences().get(1).getCanonicalUrl())
        .isEqualTo("https://example.org/Library/base");
    assertThat(result.getDeclaredParameters()).isEmpty();
  }

  @Test
  void rejectsSqlViewDeclaringAParameter() {
    // A SQLView SHALL NOT declare parameters; doing so is a 400.
    final Library library = SqlLibraryFixtures.sqlView("SELECT 1");
    SqlLibraryFixtures.addParameter(library, "min_age", "integer");

    assertThatThrownBy(() -> parser.parse(library))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining(SQL_VIEW_TYPE_CODE)
        .hasMessageContaining("parameter");
  }

  @Test
  void stillParsesSqlQueryWithParameters() {
    // The shared parser must continue to accept SQLQuery parameters unchanged.
    final Library library = SqlLibraryFixtures.sqlQuery("SELECT * FROM t WHERE age > :min_age");
    SqlLibraryFixtures.addParameter(library, "min_age", "integer");

    final ParsedSqlQuery result = parser.parse(library);

    assertThat(result.isView()).isFalse();
    assertThat(result.getDeclaredParameters()).hasSize(1);
    assertThat(result.getDeclaredParameters().get(0).getName()).isEqualTo("min_age");
  }

  // ---------------------------------------------------------------------------
  // SQL content rejections.
  // ---------------------------------------------------------------------------

  @Test
  void rejectsLibraryWithNoSqlContent() {
    final Library library = new Library();
    library.setStatus(PublicationStatus.ACTIVE);
    library.setType(sqlQueryTypeCoding());

    assertThatThrownBy(() -> parser.parse(library))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("application/sql");
  }

  @Test
  void rejectsLibraryWithWrongContentType() {
    final Library library = new Library();
    library.setStatus(PublicationStatus.ACTIVE);
    library.setType(sqlQueryTypeCoding());
    library
        .addContent()
        .setContentType("text/plain")
        .setData("SELECT 1".getBytes(StandardCharsets.UTF_8));

    assertThatThrownBy(() -> parser.parse(library))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("application/sql");
  }

  @Test
  void rejectsLibraryWithEmptySqlData() {
    final Library library = new Library();
    library.setStatus(PublicationStatus.ACTIVE);
    library.setType(sqlQueryTypeCoding());
    library.addContent().setContentType("application/sql").setData(new byte[0]);

    assertThatThrownBy(() -> parser.parse(library))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("no data");
  }

  // ---------------------------------------------------------------------------
  // Library.type profile invariant.
  // ---------------------------------------------------------------------------

  @Test
  void rejectsLibraryWithoutTypeCoding() {
    final Library library = new Library();
    library.setStatus(PublicationStatus.ACTIVE);
    library
        .addContent()
        .setContentType("application/sql")
        .setData("SELECT 1".getBytes(StandardCharsets.UTF_8));

    assertThatThrownBy(() -> parser.parse(library))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("Library.type")
        .hasMessageContaining(SQL_QUERY_TYPE_CODE);
  }

  @Test
  void rejectsLibraryWithUnrelatedTypeCoding() {
    final Library library = new Library();
    library.setStatus(PublicationStatus.ACTIVE);
    library.setType(
        new CodeableConcept()
            .addCoding(
                new org.hl7.fhir.r4.model.Coding()
                    .setSystem("http://terminology.hl7.org/CodeSystem/library-type")
                    .setCode("logic-library")));
    library
        .addContent()
        .setContentType("application/sql")
        .setData("SELECT 1".getBytes(StandardCharsets.UTF_8));

    assertThatThrownBy(() -> parser.parse(library))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining(LIBRARY_TYPE_SYSTEM)
        .hasMessageContaining(SQL_QUERY_TYPE_CODE);
  }

  @Test
  void acceptsLibraryWithAdditionalTypeCodings() {
    final Library library = new Library();
    library.setStatus(PublicationStatus.ACTIVE);
    library.setType(
        new CodeableConcept()
            .addCoding(
                new org.hl7.fhir.r4.model.Coding()
                    .setSystem("http://example.org/extra")
                    .setCode("anything"))
            .addCoding(
                new org.hl7.fhir.r4.model.Coding()
                    .setSystem(LIBRARY_TYPE_SYSTEM)
                    .setCode(SQL_QUERY_TYPE_CODE)));
    library
        .addContent()
        .setContentType("application/sql")
        .setData("SELECT 1".getBytes(StandardCharsets.UTF_8));

    final ParsedSqlQuery result = parser.parse(library);
    assertThat(result.getSql()).isEqualTo("SELECT 1");
  }

  // ---------------------------------------------------------------------------
  // relatedArtifact profile invariants.
  // ---------------------------------------------------------------------------

  @Test
  void rejectsRelatedArtifactWithMissingLabel() {
    final Library library = createMinimalLibrary("SELECT 1");
    library.addRelatedArtifact(
        new RelatedArtifact()
            .setType(RelatedArtifactType.DEPENDSON)
            .setResource("ViewDefinition/my-view"));

    assertThatThrownBy(() -> parser.parse(library))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("label");
  }

  @Test
  void rejectsRelatedArtifactWithMissingResource() {
    final Library library = createMinimalLibrary("SELECT 1");
    library.addRelatedArtifact(
        new RelatedArtifact().setType(RelatedArtifactType.DEPENDSON).setLabel("my_table"));

    assertThatThrownBy(() -> parser.parse(library))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("resource reference");
  }

  @Test
  void rejectsRelatedArtifactWithNonDependsOnType() {
    final Library library = createMinimalLibrary("SELECT 1");
    library.addRelatedArtifact(
        new RelatedArtifact()
            .setType(RelatedArtifactType.CITATION)
            .setLabel("patients")
            .setResource("ViewDefinition/patient-view"));

    assertThatThrownBy(() -> parser.parse(library))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("depends-on");
  }

  @Test
  void rejectsRelatedArtifactWithMissingType() {
    final Library library = createMinimalLibrary("SELECT 1");
    library.addRelatedArtifact(
        new RelatedArtifact().setLabel("patients").setResource("ViewDefinition/patient-view"));

    assertThatThrownBy(() -> parser.parse(library))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("depends-on");
  }

  @Test
  void rejectsRelatedArtifactLabelStartingWithDigit() {
    final Library library = createMinimalLibrary("SELECT 1");
    library.addRelatedArtifact(
        new RelatedArtifact()
            .setType(RelatedArtifactType.DEPENDSON)
            .setLabel("1patients")
            .setResource("ViewDefinition/patient-view"));

    assertThatThrownBy(() -> parser.parse(library))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("1patients")
        .hasMessageContaining("pattern");
  }

  @Test
  void rejectsRelatedArtifactLabelWithIllegalCharacters() {
    final Library library = createMinimalLibrary("SELECT 1");
    library.addRelatedArtifact(
        new RelatedArtifact()
            .setType(RelatedArtifactType.DEPENDSON)
            .setLabel("patients; DROP TABLE x")
            .setResource("ViewDefinition/patient-view"));

    assertThatThrownBy(() -> parser.parse(library))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("pattern");
  }

  @Test
  void acceptsRelatedArtifactLabelWithUnderscoresAndDigits() {
    final Library library = createMinimalLibrary("SELECT 1");
    library.addRelatedArtifact(
        new RelatedArtifact()
            .setType(RelatedArtifactType.DEPENDSON)
            .setLabel("patients_2024")
            .setResource("https://example.org/ViewDefinition/patient-view"));

    final ParsedSqlQuery result = parser.parse(library);
    assertThat(result.getViewReferences()).hasSize(1);
    assertThat(result.getViewReferences().get(0).getLabel()).isEqualTo("patients_2024");
  }

  // ---------------------------------------------------------------------------
  // relatedArtifact.resource canonical-form invariant.
  // ---------------------------------------------------------------------------

  @ParameterizedTest(name = "rejects non-canonical resource ''{0}''")
  @ValueSource(
      strings = {
        "ViewDefinition/abc",
        "Library/abc",
        "patient-view",
        "https://example.org/V#section"
      })
  void rejectsNonCanonicalResourceReference(final String resource) {
    final Library library = createMinimalLibrary("SELECT 1");
    library.addRelatedArtifact(
        new RelatedArtifact()
            .setType(RelatedArtifactType.DEPENDSON)
            .setLabel("t")
            .setResource(resource));

    assertThatThrownBy(() -> parser.parse(library))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("relatedArtifact.resource")
        .hasMessageContaining(resource)
        .hasMessageContaining("canonical URL");
  }

  @ParameterizedTest(name = "accepts canonical resource ''{0}''")
  @ValueSource(
      strings = {
        "https://example.org/ViewDefinition/patient-view",
        "https://example.org/ViewDefinition/patient-view|2.0",
        "http://example.org/Library/base",
        "urn:uuid:53fefa32-fcbb-4ff8-8a92-55ee120877b7"
      })
  void acceptsCanonicalResourceReference(final String resource) {
    final Library library = createMinimalLibrary("SELECT 1");
    library.addRelatedArtifact(
        new RelatedArtifact()
            .setType(RelatedArtifactType.DEPENDSON)
            .setLabel("t")
            .setResource(resource));

    final ParsedSqlQuery result = parser.parse(library);
    assertThat(result.getViewReferences()).hasSize(1);
    assertThat(result.getViewReferences().get(0).getCanonicalUrl()).isEqualTo(resource);
  }

  // ---------------------------------------------------------------------------
  // Parameter profile invariants.
  // ---------------------------------------------------------------------------

  @Test
  void rejectsParameterWithMissingName() {
    final Library library = createMinimalLibrary("SELECT 1");
    library.addParameter().setType("string").setUse(ParameterUse.IN);

    assertThatThrownBy(() -> parser.parse(library))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("name");
  }

  @Test
  void rejectsParameterWithMissingType() {
    final Library library = createMinimalLibrary("SELECT 1");
    library.addParameter().setName("param1").setUse(ParameterUse.IN);

    assertThatThrownBy(() -> parser.parse(library))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("type");
  }

  @Test
  void rejectsParameterWithOutUse() {
    final Library library = createMinimalLibrary("SELECT 1");
    library.addParameter().setName("param1").setType("string").setUse(ParameterUse.OUT);

    assertThatThrownBy(() -> parser.parse(library))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("param1")
        .hasMessageContaining("use = in");
  }

  @Test
  void rejectsParameterWithMissingUse() {
    final Library library = createMinimalLibrary("SELECT 1");
    library.addParameter().setName("param1").setType("string");

    assertThatThrownBy(() -> parser.parse(library))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("use = in");
  }

  // ---------------------------------------------------------------------------
  // Content type negotiation.
  // ---------------------------------------------------------------------------

  @Test
  void acceptsSqlContentTypeWithDialect() {
    final Library library = new Library();
    library.setStatus(PublicationStatus.ACTIVE);
    library.setType(sqlQueryTypeCoding());
    library
        .addContent()
        .setContentType("application/sql;dialect=spark")
        .setData("SELECT 1".getBytes(StandardCharsets.UTF_8));

    final ParsedSqlQuery result = parser.parse(library);
    assertThat(result.getSql()).isEqualTo("SELECT 1");
  }

  /** Creates a minimal SQLQuery Library resource with the given SQL as Base64-encoded content. */
  private Library createMinimalLibrary(final String sql) {
    final Library library = new Library();
    library.setStatus(PublicationStatus.ACTIVE);
    library.setType(sqlQueryTypeCoding());
    library
        .addContent()
        .setContentType("application/sql")
        .setData(sql.getBytes(StandardCharsets.UTF_8));
    return library;
  }

  /** Returns the SQLQuery profile's required Library.type coding. */
  private static CodeableConcept sqlQueryTypeCoding() {
    return new CodeableConcept()
        .addCoding(
            new org.hl7.fhir.r4.model.Coding()
                .setSystem(LIBRARY_TYPE_SYSTEM)
                .setCode(SQL_QUERY_TYPE_CODE));
  }
}
