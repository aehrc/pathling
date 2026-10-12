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

package au.csiro.pathling.fhirpath.evaluation;

import au.csiro.pathling.definition.NodeDefinition;
import jakarta.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.types.DataType;

/**
 * Renders the values of one element, as they are stored, as FHIR JSON text. {@link
 * SingleInstanceEvaluator} uses it for a result whose values are structures. The rendering belongs
 * to the {@code io} module, on which {@code fhirpath} depends for its tests only, so the caller
 * supplies it: {@code PathlingContext} passes the {@code writeElement} method of {@code io}'s JSON
 * writer (decision 91).
 *
 * @author Piotr Szul
 */
@FunctionalInterface
public interface ElementRenderer {

  /**
   * Returns the FHIR JSON text of each of the values of an element.
   *
   * @param element the definition of the element
   * @param stored the values, a structure or an array of them
   * @param type the type of the values
   * @return the JSON text of each value, as a string, or as an array of strings where the values
   *     are an array; null where a value is null
   */
  @Nonnull
  Column render(@Nonnull NodeDefinition element, @Nonnull Column stored, @Nonnull DataType type);
}
