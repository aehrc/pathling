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

/**
 * Enum representing the types of FHIR search parameters.
 *
 * @see <a href="https://hl7.org/fhir/search.html#ptypes">Search Parameter Types</a>
 */
public enum SearchParameterType {

  /**
   * A token type search parameter matches a system and/or code.
   */
  TOKEN,

  /**
   * A string type search parameter matches string values.
   */
  STRING,

  /**
   * A date type search parameter matches date/time values.
   */
  DATE,

  /**
   * A quantity type search parameter matches quantity values with optional units.
   */
  QUANTITY,

  /**
   * A reference type search parameter matches references to other resources.
   */
  REFERENCE,

  /**
   * A number type search parameter matches numeric values.
   */
  NUMBER,

  /**
   * A URI type search parameter matches URI values.
   */
  URI,

  /**
   * A composite type search parameter combines multiple parameters.
   */
  COMPOSITE,

  /**
   * A special type search parameter has custom behavior.
   */
  SPECIAL
}
