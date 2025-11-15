/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.views;

import au.csiro.pathling.views.validation.AtMostOneNonNull;
import au.csiro.pathling.views.validation.CompatibleUnionColumns;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Defines the 'select' backbone element.
 *
 * @author John Grimes
 * @see <a
 * href="https://sql-on-fhir.org/ig/2.0.0/StructureDefinition-ViewDefinition-definitions.html#ViewDefinition.select">ViewDefinition.select</a>
 */
@Data
@AllArgsConstructor()
@NoArgsConstructor()
@AtMostOneNonNull({"forEach", "forEachOrNull", "repeat"})
public class SelectClause implements SelectionElement {

  /**
   * Creates a new SelectClauseBuilder.
   *
   * @return a new builder instance
   */
  @Nonnull
  public static SelectClauseBuilder builder() {
    return new SelectClauseBuilder();
  }

  /**
   * A column to be produced in the resulting table. The column is relative to the select structure
   * that contains it.
   *
   * @see <a
   * href="https://sql-on-fhir.org/ig/2.0.0/StructureDefinition-ViewDefinition-definitions.html#diff_ViewDefinition.select.column">ViewDefinition.select.column</a>
   */
  @Nonnull
  @NotNull
  @Valid
  List<@Valid Column> column = Collections.emptyList();

  /**
   * Nested select relative to this.
   *
   * @see <a
   * href="https://sql-on-fhir.org/ig/2.0.0/StructureDefinition-ViewDefinition-definitions.html#diff_ViewDefinition.select.select">ViewDefinition.select.select</a>
   */
  @Nonnull
  @NotNull
  @Size()
  @Valid
  List<@Valid SelectClause> select = Collections.emptyList();

  /**
   * A FHIRPath expression to retrieve the parent element(s) used in the containing select, relative
   * to the root resource or parent `select`, if applicable. `forEach` will produce a row for each
   * element selected in the expression.
   *
   * @see <a
   * href="https://sql-on-fhir.org/ig/2.0.0/StructureDefinition-ViewDefinition-definitions.html#diff_ViewDefinition.select.forEach">ViewDefinition.select.forEach</a>
   */
  @Nullable
  String forEach;

  /**
   * Same as forEach, but produces a single row with null values in the nested expression if the
   * collection is empty.
   *
   * @see <a
   * href="https://sql-on-fhir.org/ig/2.0.0/StructureDefinition-ViewDefinition-definitions.html#diff_ViewDefinition.select.forEachOrNull">ViewDefinition.select.forEachOrNull</a>
   */
  @Nullable
  String forEachOrNull;

  /**
   * An array of FHIRPath expressions that define paths to recursively traverse. The view runner
   * will start at the current context node, evaluate each path expression, and for each result,
   * recursively apply the same path patterns. This continues until no more matches are found at
   * any depth. All results from all levels and all paths are unioned together.
   *
   * @see <a
   * href="https://sql-on-fhir.org/ig/2.0.0/StructureDefinition-ViewDefinition-definitions.html#diff_ViewDefinition.select.repeat">ViewDefinition.select.repeat</a>
   */
  @Nullable
  List<String> repeat;

  /**
   * A `unionAll` combines the results of multiple selection structures. Each structure under the
   * `unionAll` must produce the same column names and types.
   *
   * @see <a
   * href="https://sql-on-fhir.org/ig/2.0.0/StructureDefinition-ViewDefinition-definitions.html#diff_ViewDefinition.select.unionAll">ViewDefinition.select.unionAll</a>
   */
  @Nonnull
  @NotNull
  @Size()
  @Valid
  @CompatibleUnionColumns
  List<@Valid SelectClause> unionAll = Collections.emptyList();


  /**
   * Returns a stream of all columns defined in this select clause, including those in nested
   * selects and the unionAll.
   *
   * @return a stream of all columns
   */
  @Nonnull
  public Stream<Column> getAllColumns() {
    return Stream.of(
        getColumn().stream(),
        getSelect().stream().flatMap(SelectClause::getAllColumns),
        // get just the first unionAll because we assume that unionAlls have the same structure
        getUnionAll().stream().limit(1).flatMap(SelectClause::getAllColumns)
    ).flatMap(Function.identity());
  }

}
