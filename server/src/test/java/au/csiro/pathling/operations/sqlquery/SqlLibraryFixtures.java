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

import jakarta.annotation.Nonnull;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.hl7.fhir.r4.model.Attachment;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.PublicationStatus;
import org.hl7.fhir.r4.model.Library;
import org.hl7.fhir.r4.model.ParameterDefinition;
import org.hl7.fhir.r4.model.ParameterDefinition.ParameterUse;
import org.hl7.fhir.r4.model.RelatedArtifact;
import org.hl7.fhir.r4.model.RelatedArtifact.RelatedArtifactType;

/**
 * Test fixtures for constructing {@code SQLQuery} and {@code SQLView} {@link Library} resources.
 * The builders mirror the SQL on FHIR profiles: a {@code Library.type} coding, one {@code
 * application/sql} content entry carrying the Base64-encoded SQL, {@code depends-on} {@code
 * relatedArtifact} dependencies (label plus resource reference), and, for {@code SQLQuery},
 * optional input parameters.
 *
 * @author John Grimes
 */
public final class SqlLibraryFixtures {

  private SqlLibraryFixtures() {
    // Utility class.
  }

  /**
   * Builds a {@code SQLQuery} Library carrying the given SQL, with no dependencies or parameters.
   *
   * @param sql the SQL text to embed as Base64 {@code application/sql} content
   * @return a minimal {@code SQLQuery} Library
   */
  @Nonnull
  public static Library sqlQuery(@Nonnull final String sql) {
    return baseLibrary(SQL_QUERY_TYPE_CODE, sql);
  }

  /**
   * Builds a {@code SQLQuery} Library carrying the given SQL and a single {@code depends-on}
   * dependency.
   *
   * @param sql the SQL text to embed
   * @param label the dependency table label
   * @param resource the dependency resource reference
   * @return a {@code SQLQuery} Library with one dependency
   */
  @Nonnull
  public static Library sqlQuery(
      @Nonnull final String sql, @Nonnull final String label, @Nonnull final String resource) {
    final Library library = baseLibrary(SQL_QUERY_TYPE_CODE, sql);
    addDependency(library, label, resource);
    return library;
  }

  /**
   * Builds a {@code SQLView} Library carrying the given SQL, with no dependencies or parameters.
   *
   * @param sql the SQL text to embed as Base64 {@code application/sql} content
   * @return a minimal {@code SQLView} Library
   */
  @Nonnull
  public static Library sqlView(@Nonnull final String sql) {
    return baseLibrary(SQL_VIEW_TYPE_CODE, sql);
  }

  /**
   * Builds a {@code SQLView} Library carrying the given SQL and a single {@code depends-on}
   * dependency.
   *
   * @param sql the SQL text to embed
   * @param label the dependency table label
   * @param resource the dependency resource reference
   * @return a {@code SQLView} Library with one dependency
   */
  @Nonnull
  public static Library sqlView(
      @Nonnull final String sql, @Nonnull final String label, @Nonnull final String resource) {
    final Library library = baseLibrary(SQL_VIEW_TYPE_CODE, sql);
    addDependency(library, label, resource);
    return library;
  }

  /**
   * Builds a {@code SQLView} Library carrying the given SQL and a set of {@code depends-on}
   * dependencies, preserving the iteration order of the supplied map.
   *
   * @param sql the SQL text to embed
   * @param dependenciesByLabel the dependencies keyed by table label, each value a resource
   *     reference
   * @return a {@code SQLView} Library with the given dependencies
   */
  @Nonnull
  public static Library sqlView(
      @Nonnull final String sql, @Nonnull final Map<String, String> dependenciesByLabel) {
    final Library library = baseLibrary(SQL_VIEW_TYPE_CODE, sql);
    dependenciesByLabel.forEach((label, resource) -> addDependency(library, label, resource));
    return library;
  }

  /**
   * Adds a {@code depends-on} {@code relatedArtifact} to the given Library.
   *
   * @param library the Library to extend
   * @param label the dependency table label
   * @param resource the dependency resource reference
   */
  public static void addDependency(
      @Nonnull final Library library, @Nonnull final String label, @Nonnull final String resource) {
    library.addRelatedArtifact(
        new RelatedArtifact()
            .setType(RelatedArtifactType.DEPENDSON)
            .setLabel(label)
            .setResource(resource));
  }

  /**
   * Adds an input parameter declaration to the given Library.
   *
   * @param library the Library to extend
   * @param name the parameter name
   * @param type the FHIR primitive type code
   */
  public static void addParameter(
      @Nonnull final Library library, @Nonnull final String name, @Nonnull final String type) {
    library.addParameter(
        new ParameterDefinition().setName(name).setType(type).setUse(ParameterUse.IN));
  }

  /** Builds a Library with the given SQL on FHIR library-type code and embedded SQL content. */
  @Nonnull
  private static Library baseLibrary(@Nonnull final String typeCode, @Nonnull final String sql) {
    final Library library = new Library();
    library.setStatus(PublicationStatus.ACTIVE);
    library.setType(
        new CodeableConcept()
            .addCoding(new Coding().setSystem(LIBRARY_TYPE_SYSTEM).setCode(typeCode)));
    final Attachment content = new Attachment();
    content.setContentType("application/sql");
    content.setData(sql.getBytes(StandardCharsets.UTF_8));
    library.addContent(content);
    return library;
  }
}
