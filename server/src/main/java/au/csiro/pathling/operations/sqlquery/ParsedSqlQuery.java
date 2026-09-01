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

import jakarta.annotation.Nonnull;
import java.util.List;
import lombok.Value;

/**
 * Represents a parsed SQL on FHIR Library resource ({@code SQLQuery} or {@code SQLView}) containing
 * the SQL text, dependency references, declared parameters, and the resolved library type code.
 *
 * @author John Grimes
 */
@Value
public class ParsedSqlQuery {

  /** The decoded SQL query text. */
  @Nonnull String sql;

  /** The dependency references (to ViewDefinitions and/or SQLViews) referenced in the SQL. */
  @Nonnull List<ViewArtifactReference> viewReferences;

  /** The declared parameters that can be bound at execution time. Always empty for a SQLView. */
  @Nonnull List<SqlParameterDeclaration> declaredParameters;

  /**
   * The SQL on FHIR library type code: {@link SqlLibraryParser#SQL_QUERY_TYPE_CODE} or {@link
   * SqlLibraryParser#SQL_VIEW_TYPE_CODE}.
   */
  @Nonnull String libraryTypeCode;

  /**
   * Indicates whether this parsed query came from a {@code SQLView} Library.
   *
   * @return {@code true} if the library type is {@code sql-view}
   */
  public boolean isView() {
    return SqlLibraryParser.isView(libraryTypeCode);
  }
}
