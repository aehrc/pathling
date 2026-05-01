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

import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import jakarta.annotation.Nonnull;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import org.hl7.fhir.r4.model.Attachment;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Library;
import org.hl7.fhir.r4.model.ParameterDefinition;
import org.hl7.fhir.r4.model.ParameterDefinition.ParameterUse;
import org.hl7.fhir.r4.model.RelatedArtifact;
import org.hl7.fhir.r4.model.RelatedArtifact.RelatedArtifactType;
import org.springframework.stereotype.Component;

/**
 * Parses a FHIR R4 Library resource conforming to the SQLQuery profile. Extracts the SQL text,
 * ViewDefinition dependencies, and parameter declarations, and enforces the profile invariants.
 *
 * <p>The SQLQuery profile requires:
 *
 * <ul>
 *   <li>{@code Library.type} carrying a coding of {@code sql-query} from the SQL on FHIR library
 *       types code system.
 *   <li>A {@code content} entry with content type starting with {@code application/sql} containing
 *       Base64-encoded SQL text.
 *   <li>Each {@code relatedArtifact} of type {@code depends-on}, with a label matching {@code
 *       ^[A-Za-z][A-Za-z0-9_]*$} and a {@code resource} canonical URL pointing at the referenced
 *       ViewDefinition.
 *   <li>Each {@code parameter} declared with {@code use = in} and a name and type.
 * </ul>
 *
 * @see <a
 *     href="https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-SQLQuery.html">SQLQuery</a>
 */
@Component
public class SqlQueryLibraryParser {

  private static final String SQL_CONTENT_TYPE_PREFIX = "application/sql";

  private static final String LIBRARY_TYPE_SYSTEM =
      "https://sql-on-fhir.org/ig/CodeSystem/LibraryTypesCodes";

  private static final String LIBRARY_TYPE_CODE = "sql-query";

  private static final Pattern LABEL_PATTERN = Pattern.compile("^[A-Za-z]\\w*$");

  /**
   * Parses a Library resource into a {@link ParsedSqlQuery}.
   *
   * @param library the Library resource to parse
   * @return the parsed SQL query containing SQL text, view references, and parameter declarations
   * @throws InvalidRequestException if the Library does not conform to the SQLQuery profile
   */
  @Nonnull
  public ParsedSqlQuery parse(@Nonnull final Library library) {
    validateLibraryType(library);
    final String sql = extractSql(library);
    final List<ViewArtifactReference> viewReferences = extractViewReferences(library);
    final List<SqlParameterDeclaration> parameters = extractParameters(library);
    return new ParsedSqlQuery(sql, viewReferences, parameters);
  }

  /**
   * Verifies that the Library carries the SQLQuery profile's type coding. The check accepts any
   * coding with the expected system and code, regardless of additional codings, so that authors can
   * layer their own classifications without breaking conformance.
   */
  private void validateLibraryType(@Nonnull final Library library) {
    final CodeableConcept type = library.getType();
    if (type == null || type.isEmpty()) {
      throw new InvalidRequestException(
          "SQLQuery Library must declare Library.type with the SQLQuery coding ("
              + LIBRARY_TYPE_SYSTEM
              + "#"
              + LIBRARY_TYPE_CODE
              + ")");
    }
    for (final Coding coding : type.getCoding()) {
      if (LIBRARY_TYPE_SYSTEM.equals(coding.getSystem())
          && LIBRARY_TYPE_CODE.equals(coding.getCode())) {
        return;
      }
    }
    throw new InvalidRequestException(
        "SQLQuery Library.type must include a coding with system "
            + LIBRARY_TYPE_SYSTEM
            + " and code "
            + LIBRARY_TYPE_CODE);
  }

  /**
   * Extracts the SQL text from the Library's content entries.
   *
   * @param library the Library resource
   * @return the decoded SQL text
   * @throws InvalidRequestException if no SQL content is found or the content is invalid
   */
  @Nonnull
  private String extractSql(@Nonnull final Library library) {
    for (final Attachment attachment : library.getContent()) {
      final String contentType = attachment.getContentType();
      if (contentType != null && contentType.startsWith(SQL_CONTENT_TYPE_PREFIX)) {
        final byte[] data = attachment.getData();
        if (data == null || data.length == 0) {
          throw new InvalidRequestException(
              "SQLQuery Library has an application/sql content entry with no data");
        }
        // The data is Base64-encoded in the FHIR resource. HAPI decodes it automatically when
        // using getData(), so we can use it directly.
        return new String(data, StandardCharsets.UTF_8);
      }
    }
    throw new InvalidRequestException(
        "SQLQuery Library must contain a content entry with content type application/sql");
  }

  /**
   * Extracts ViewDefinition references from the Library's related artifacts, enforcing that each
   * artifact is of type {@code depends-on} with a label matching the SQLQuery profile pattern.
   *
   * @param library the Library resource
   * @return the list of view artifact references
   */
  @Nonnull
  private List<ViewArtifactReference> extractViewReferences(@Nonnull final Library library) {
    final List<ViewArtifactReference> references = new ArrayList<>();
    for (final RelatedArtifact artifact : library.getRelatedArtifact()) {
      if (artifact.getType() != RelatedArtifactType.DEPENDSON) {
        throw new InvalidRequestException(
            "SQLQuery Library relatedArtifact must have type 'depends-on', but found '"
                + (artifact.getType() == null ? "null" : artifact.getType().toCode())
                + "'");
      }
      final String label = artifact.getLabel();
      final String resource = artifact.getResource();
      if (label == null || label.isBlank()) {
        throw new InvalidRequestException(
            "Each relatedArtifact in the SQLQuery Library must have a label");
      }
      if (!LABEL_PATTERN.matcher(label).matches()) {
        throw new InvalidRequestException(
            "SQLQuery Library relatedArtifact label '"
                + label
                + "' does not match the required pattern "
                + LABEL_PATTERN.pattern());
      }
      if (resource == null || resource.isBlank()) {
        throw new InvalidRequestException(
            "Each relatedArtifact in the SQLQuery Library must have a resource reference");
      }
      references.add(new ViewArtifactReference(label, resource));
    }
    return references;
  }

  /**
   * Extracts parameter declarations from the Library's parameter entries, enforcing that each
   * declaration is an input ({@code use = in}) and carries both a name and a type.
   *
   * @param library the Library resource
   * @return the list of parameter declarations
   */
  @Nonnull
  private List<SqlParameterDeclaration> extractParameters(@Nonnull final Library library) {
    final List<SqlParameterDeclaration> parameters = new ArrayList<>();
    for (final ParameterDefinition param : library.getParameter()) {
      final String name = param.getName();
      final String type = param.getType();
      if (name == null || name.isBlank()) {
        throw new InvalidRequestException(
            "Each parameter in the SQLQuery Library must have a name");
      }
      if (type == null || type.isBlank()) {
        throw new InvalidRequestException(
            "Each parameter in the SQLQuery Library must have a type");
      }
      if (param.getUse() != ParameterUse.IN) {
        throw new InvalidRequestException(
            "SQLQuery Library parameter '"
                + name
                + "' must declare use = in, but found '"
                + (param.getUse() == null ? "null" : param.getUse().toCode())
                + "'");
      }
      parameters.add(new SqlParameterDeclaration(name, type));
    }
    return parameters;
  }
}
