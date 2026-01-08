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

package au.csiro.pathling.search;

import jakarta.annotation.Nonnull;
import lombok.Value;

/**
 * Represents the definition of a FHIR search parameter.
 *
 * @see <a href="https://hl7.org/fhir/searchparameter.html">SearchParameter</a>
 */
@Value
public class SearchParameterDefinition {

  /**
   * The code that identifies this search parameter (e.g., "gender").
   */
  @Nonnull
  String code;

  /**
   * The type of the search parameter.
   */
  @Nonnull
  SearchParameterType type;

  /**
   * The FHIRPath expression that defines the values for this parameter.
   */
  @Nonnull
  String expression;
}
