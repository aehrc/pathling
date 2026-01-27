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

package au.csiro.pathling.views;

import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Additional metadata describing a column.
 *
 * @see <a
 *     href="https://sql-on-fhir.org/ig/2.0.0/StructureDefinition-ViewDefinition.html#type-hinting-with-tags">Type
 *     Hinting with Tags</a>
 */
@Data
@AllArgsConstructor(staticName = "of")
@NoArgsConstructor
public class ColumnTag {

  /**
   * Tag for ANSI SQL type hinting.
   *
   * <p>This tag is used to provide a hint about the SQL type of the column, which can be useful for
   * database-specific optimizations or type handling.
   */
  public static final String ANSI_TYPE_TAG = "ansi/type";

  /** Name of tag. */
  @NotNull String name;

  /** Value of tag. */
  @NotNull String value;
}
