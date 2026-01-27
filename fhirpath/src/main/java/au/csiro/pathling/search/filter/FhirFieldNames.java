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

package au.csiro.pathling.search.filter;

/**
 * Centralized constants for FHIR element field names used in search filters.
 *
 * <p>These constants are used across multiple search matchers to avoid duplication and ensure
 * consistency. All field names follow FHIR naming conventions.
 */
public final class FhirFieldNames {

  /**
   * System URI field used in Coding, CodeableConcept, Identifier, and Quantity types. Typically
   * paired with a code/value field to form a key-value pair.
   */
  public static final String SYSTEM = "system";

  /**
   * Code field used in Coding, CodeableConcept types. Represents the code value portion of a
   * key-value pair.
   */
  public static final String CODE = "code";

  /**
   * Value field used in Identifier, ContactPoint, and numeric types. Represents the value portion
   * of a key-value pair or the numeric value itself.
   */
  public static final String VALUE = "value";

  /**
   * Unit field used in Quantity type. Represents the unit of measurement for a numeric quantity.
   */
  public static final String UNIT = "unit";

  /**
   * Coding array field used in CodeableConcept type. Contains the array of Coding elements that
   * make up a CodeableConcept.
   */
  public static final String CODING = "coding";

  /**
   * Canonicalized value field for Quantity type (internal use). Generated field that stores the
   * UCUM-normalized numeric value for cross-unit matching.
   */
  public static final String CANONICALIZED_VALUE = "_value_canonicalized";

  /**
   * Canonicalized code field for Quantity type (internal use). Generated field that stores the
   * UCUM-normalized code for cross-unit matching.
   */
  public static final String CANONICALIZED_CODE = "_code_canonicalized";

  private FhirFieldNames() {
    // Utility class - no instances
  }
}
