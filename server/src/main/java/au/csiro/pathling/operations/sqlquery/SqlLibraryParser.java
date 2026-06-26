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
import jakarta.annotation.Nullable;
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
 * Parses a FHIR R4 Library resource conforming to the SQL on FHIR {@code SQLQuery} or {@code
 * SQLView} profile. Extracts the SQL text, dependency references, and (for {@code SQLQuery})
 * parameter declarations, and enforces the profile invariants common to both.
 *
 * <p>The two profiles are near-twins. Both require:
 *
 * <ul>
 *   <li>{@code Library.type} carrying a coding from the SQL on FHIR library types code system -
 *       {@code sql-query} for a {@code SQLQuery}, {@code sql-view} for a {@code SQLView}.
 *   <li>A {@code content} entry with content type starting with {@code application/sql} containing
 *       Base64-encoded SQL text.
 *   <li>Each {@code relatedArtifact} of type {@code depends-on}, with a label matching {@code
 *       ^[A-Za-z][A-Za-z0-9_]*$} and a {@code resource} reference pointing at the referenced {@code
 *       ViewDefinition} or {@code SQLView}.
 * </ul>
 *
 * <p>They differ in only one rule: a {@code SQLQuery} may declare input {@code parameter}s (each
 * {@code use = in}), whereas a {@code SQLView} SHALL NOT declare any parameter.
 *
 * @author John Grimes
 * @see <a
 *     href="https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-SQLQuery.html">SQLQuery</a>
 * @see <a
 *     href="https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-SQLView.html">SQLView</a>
 */
@Component
public class SqlLibraryParser {

  private static final String SQL_CONTENT_TYPE_PREFIX = "application/sql";

  /** Code system identifying the SQL on FHIR Library profiles. */
  public static final String LIBRARY_TYPE_SYSTEM =
      "https://sql-on-fhir.org/ig/CodeSystem/LibraryTypesCodes";

  /** {@link #LIBRARY_TYPE_SYSTEM} code identifying a {@code SQLQuery} Library. */
  public static final String SQL_QUERY_TYPE_CODE = "sql-query";

  /** {@link #LIBRARY_TYPE_SYSTEM} code identifying a {@code SQLView} Library. */
  public static final String SQL_VIEW_TYPE_CODE = "sql-view";

  private static final Pattern LABEL_PATTERN = Pattern.compile("^[A-Za-z]\\w*$");

  /**
   * Parses a Library resource into a {@link ParsedSqlQuery}, accepting either a {@code SQLQuery} or
   * a {@code SQLView}.
   *
   * @param library the Library resource to parse
   * @return the parsed query carrying SQL text, dependency references, parameter declarations, and
   *     the resolved library type code
   * @throws InvalidRequestException if the Library does not conform to either profile
   */
  @Nonnull
  public ParsedSqlQuery parse(@Nonnull final Library library) {
    final String typeCode = resolveLibraryTypeCode(library);
    final boolean isView = SQL_VIEW_TYPE_CODE.equals(typeCode);
    final String sql = extractSql(library, typeCode);
    final List<ViewArtifactReference> viewReferences = extractViewReferences(library);
    final List<SqlParameterDeclaration> parameters = extractParameters(library, isView);
    return new ParsedSqlQuery(sql, viewReferences, parameters, typeCode);
  }

  /**
   * Resolves the SQL on FHIR library type code carried by {@code Library.type}. The check accepts a
   * coding with the expected system and either the {@code sql-query} or {@code sql-view} code,
   * regardless of additional codings, so that authors can layer their own classifications without
   * breaking conformance.
   *
   * @return the matched type code ({@code sql-query} or {@code sql-view})
   * @throws InvalidRequestException if no recognised SQL on FHIR coding is present
   */
  @Nonnull
  private String resolveLibraryTypeCode(@Nonnull final Library library) {
    final CodeableConcept type = library.getType();
    if (type == null || type.isEmpty()) {
      throw new InvalidRequestException(
          "SQL on FHIR Library must declare Library.type with a coding from "
              + LIBRARY_TYPE_SYSTEM
              + " ("
              + SQL_QUERY_TYPE_CODE
              + " or "
              + SQL_VIEW_TYPE_CODE
              + ")");
    }
    for (final Coding coding : type.getCoding()) {
      if (LIBRARY_TYPE_SYSTEM.equals(coding.getSystem())) {
        final String code = coding.getCode();
        if (SQL_QUERY_TYPE_CODE.equals(code) || SQL_VIEW_TYPE_CODE.equals(code)) {
          return code;
        }
      }
    }
    throw new InvalidRequestException(
        "SQL on FHIR Library.type must include a coding with system "
            + LIBRARY_TYPE_SYSTEM
            + " and code "
            + SQL_QUERY_TYPE_CODE
            + " or "
            + SQL_VIEW_TYPE_CODE);
  }

  /**
   * Extracts the SQL text from the Library's content entries.
   *
   * @param library the Library resource
   * @param typeCode the resolved library type code, used in the error message
   * @return the decoded SQL text
   * @throws InvalidRequestException if no SQL content is found or the content is invalid
   */
  @Nonnull
  private String extractSql(@Nonnull final Library library, @Nonnull final String typeCode) {
    for (final Attachment attachment : library.getContent()) {
      final String contentType = attachment.getContentType();
      if (contentType != null && contentType.startsWith(SQL_CONTENT_TYPE_PREFIX)) {
        final byte[] data = attachment.getData();
        if (data == null || data.length == 0) {
          throw new InvalidRequestException(
              "SQL on FHIR Library has an application/sql content entry with no data");
        }
        // The data is Base64-encoded in the FHIR resource. HAPI decodes it automatically when
        // using getData(), so we can use it directly.
        return new String(data, StandardCharsets.UTF_8);
      }
    }
    throw new InvalidRequestException(
        "A "
            + typeCode
            + " Library must contain a content entry with content type application/sql");
  }

  /**
   * Extracts dependency references from the Library's related artifacts, enforcing that each
   * artifact is of type {@code depends-on} with a label matching the SQL on FHIR profile pattern.
   *
   * @param library the Library resource
   * @return the list of dependency references
   */
  @Nonnull
  private List<ViewArtifactReference> extractViewReferences(@Nonnull final Library library) {
    final List<ViewArtifactReference> references = new ArrayList<>();
    for (final RelatedArtifact artifact : library.getRelatedArtifact()) {
      if (artifact.getType() != RelatedArtifactType.DEPENDSON) {
        throw new InvalidRequestException(
            "SQL on FHIR Library relatedArtifact must have type 'depends-on', but found '"
                + (artifact.getType() == null ? "null" : artifact.getType().toCode())
                + "'");
      }
      final String label = artifact.getLabel();
      final String resource = artifact.getResource();
      if (label == null || label.isBlank()) {
        throw new InvalidRequestException(
            "Each relatedArtifact in the SQL on FHIR Library must have a label");
      }
      if (!LABEL_PATTERN.matcher(label).matches()) {
        throw new InvalidRequestException(
            "SQL on FHIR Library relatedArtifact label '"
                + label
                + "' does not match the required pattern "
                + LABEL_PATTERN.pattern());
      }
      if (resource == null || resource.isBlank()) {
        throw new InvalidRequestException(
            "Each relatedArtifact in the SQL on FHIR Library must have a resource reference");
      }
      references.add(new ViewArtifactReference(label, resource));
    }
    return references;
  }

  /**
   * Extracts parameter declarations from the Library's parameter entries. A {@code SQLView} SHALL
   * NOT declare any parameter and is rejected if it does. For a {@code SQLQuery}, each declaration
   * must be an input ({@code use = in}) and carry both a name and a type.
   *
   * @param library the Library resource
   * @param isView whether the Library is a {@code SQLView}
   * @return the list of parameter declarations (always empty for a {@code SQLView})
   * @throws InvalidRequestException if a {@code SQLView} declares any parameter, or a {@code
   *     SQLQuery} parameter is malformed
   */
  @Nonnull
  private List<SqlParameterDeclaration> extractParameters(
      @Nonnull final Library library, final boolean isView) {
    if (isView) {
      if (!library.getParameter().isEmpty()) {
        throw new InvalidRequestException(
            "A " + SQL_VIEW_TYPE_CODE + " Library must not declare any parameter");
      }
      return List.of();
    }

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

  /**
   * Indicates whether the given library type code denotes a {@code SQLView}.
   *
   * @param typeCode the SQL on FHIR library type code
   * @return {@code true} if the code is {@code sql-view}
   */
  public static boolean isView(@Nullable final String typeCode) {
    return SQL_VIEW_TYPE_CODE.equals(typeCode);
  }
}
