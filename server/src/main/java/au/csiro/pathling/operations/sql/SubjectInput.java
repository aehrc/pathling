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

import au.csiro.pathling.operations.sqlquery.PreparedSqlQuery;
import au.csiro.pathling.views.FhirView;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;

/**
 * One {@code subject} repetition of an {@code $sql-export} request, fully prepared at kick-off and
 * carried to background execution. Each subject input produces exactly one manifest output, named
 * by {@link #name()}.
 *
 * <p>Exactly one of {@code view} and {@code preparedQuery} is populated, according to the kind the
 * subject resolved to: the two go to different evaluation engines and share nothing beyond the
 * output name.
 *
 * @param kind the kind the subject resolved to
 * @param name the output name, already made unique across the job
 * @param view the parsed view, for a ViewDefinition subject
 * @param preparedQuery the prepared query, for a SQLQuery or SQLView subject
 * @author John Grimes
 */
public record SubjectInput(
    @Nonnull SubjectKind kind,
    @Nonnull String name,
    @Nullable FhirView view,
    @Nullable PreparedSqlQuery preparedQuery) {

  /**
   * Builds an input for a ViewDefinition subject.
   *
   * @param name the output name
   * @param view the parsed view
   * @return the input
   */
  @Nonnull
  public static SubjectInput ofView(@Nonnull final String name, @Nonnull final FhirView view) {
    return new SubjectInput(SubjectKind.VIEW_DEFINITION, name, view, null);
  }

  /**
   * Builds an input for a SQLQuery or SQLView subject.
   *
   * @param kind the SQL kind the subject resolved to
   * @param name the output name
   * @param preparedQuery the prepared query
   * @return the input
   */
  @Nonnull
  public static SubjectInput ofSql(
      @Nonnull final SubjectKind kind,
      @Nonnull final String name,
      @Nonnull final PreparedSqlQuery preparedQuery) {
    return new SubjectInput(kind, name, null, preparedQuery);
  }
}
