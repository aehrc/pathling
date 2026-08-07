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

/**
 * The kind of artefact a {@code $sql-run} or {@code $sql-export} subject resolves to. The kind
 * determines how the subject is evaluated, which conditional parameters apply, and which output
 * formats are offered.
 *
 * @author John Grimes
 */
public enum SubjectKind {

  /** A SQL on FHIR {@code ViewDefinition}, evaluated by the FhirView executor. */
  VIEW_DEFINITION,

  /** A {@code Library} conforming to the SQL on FHIR {@code SQLQuery} profile. */
  SQL_QUERY,

  /** A {@code Library} conforming to the SQL on FHIR {@code SQLView} profile. */
  SQL_VIEW;

  /**
   * Indicates whether this kind is one of the two SQL Library kinds, which share the SQL evaluation
   * engine and the conditional parameter rules that apply to it.
   *
   * @return true for {@code SQL_QUERY} and {@code SQL_VIEW}
   */
  public boolean isSql() {
    return this != VIEW_DEFINITION;
  }
}
