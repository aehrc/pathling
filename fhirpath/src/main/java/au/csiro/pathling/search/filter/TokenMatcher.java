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

import static org.apache.spark.sql.functions.exists;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.lower;

import au.csiro.pathling.search.TokenSearchValue;
import jakarta.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Matches elements using token search semantics for various FHIR types.
 * <p>
 * Token search supports the following syntax:
 * <ul>
 *   <li>{@code code} - matches any resource with the given code (any system)</li>
 *   <li>{@code system|code} - matches resources with the given system and code</li>
 *   <li>{@code |code} - matches resources with the given code and no system</li>
 *   <li>{@code system|} - matches any resource with any code in the given system</li>
 * </ul>
 * <p>
 * Matching logic varies by FHIR type:
 * <ul>
 *   <li>{@code Coding} - matches system and code fields</li>
 *   <li>{@code CodeableConcept} - matches any Coding in the coding array</li>
 *   <li>{@code Identifier} - matches system and value fields</li>
 *   <li>{@code ContactPoint} - matches value field only (no system)</li>
 *   <li>{@code code}, {@code uri}, {@code id} - simple string equality</li>
 *   <li>{@code string} - case-insensitive string equality</li>
 *   <li>{@code boolean} - boolean equality</li>
 * </ul>
 *
 * @see <a href="https://hl7.org/fhir/search.html#token">Token Search</a>
 */
public class TokenMatcher implements ElementMatcher {

  private static final String SYSTEM_FIELD = "system";
  private static final String CODE_FIELD = "code";
  private static final String VALUE_FIELD = "value";
  private static final String CODING_FIELD = "coding";

  @Nonnull
  private final FHIRDefinedType fhirType;

  /**
   * Creates a TokenMatcher for the specified FHIR type.
   *
   * @param fhirType the FHIR type of the element being matched
   */
  public TokenMatcher(@Nonnull final FHIRDefinedType fhirType) {
    this.fhirType = fhirType;
  }

  @Override
  @Nonnull
  public Column match(@Nonnull final Column element, @Nonnull final String searchValue) {
    final TokenSearchValue token = TokenSearchValue.parse(searchValue);

    return switch (fhirType) {
      case CODING -> matchCoding(element, token);
      case CODEABLECONCEPT -> matchCodeableConcept(element, token);
      case IDENTIFIER -> matchIdentifier(element, token);
      case CONTACTPOINT -> matchContactPoint(element, token);
      case CODE, URI, ID -> matchSimpleValue(element, token);
      case STRING -> matchStringValue(element, token);
      case BOOLEAN -> matchBoolean(element, token);
      default -> throw new IllegalArgumentException(
          "Unsupported FHIR type for token search: " + fhirType);
    };
  }

  /**
   * Matches a Coding element against the token search value.
   *
   * @param element the Coding column (struct with system, code fields)
   * @param token the parsed token search value
   * @return a boolean column indicating match
   */
  @Nonnull
  private Column matchCoding(@Nonnull final Column element, @Nonnull final TokenSearchValue token) {
    return matchSystemAndCode(
        element.getField(SYSTEM_FIELD),
        element.getField(CODE_FIELD),
        token);
  }

  /**
   * Matches a CodeableConcept element against the token search value. Returns true if ANY coding in
   * the coding array matches.
   *
   * @param element the CodeableConcept column (struct with coding array)
   * @param token the parsed token search value
   * @return a boolean column indicating match
   */
  @Nonnull
  private Column matchCodeableConcept(@Nonnull final Column element,
      @Nonnull final TokenSearchValue token) {
    final Column codingArray = element.getField(CODING_FIELD);
    // Match if ANY coding in the array matches
    return exists(codingArray, coding -> matchSystemAndCode(
        coding.getField(SYSTEM_FIELD),
        coding.getField(CODE_FIELD),
        token));
  }

  /**
   * Matches an Identifier element against the token search value. Note: Identifier uses 'value'
   * field, not 'code'.
   *
   * @param element the Identifier column (struct with system, value fields)
   * @param token the parsed token search value
   * @return a boolean column indicating match
   */
  @Nonnull
  private Column matchIdentifier(@Nonnull final Column element,
      @Nonnull final TokenSearchValue token) {
    return matchSystemAndCode(
        element.getField(SYSTEM_FIELD),
        element.getField(VALUE_FIELD),  // Identifier uses 'value', not 'code'
        token);
  }

  /**
   * Matches a ContactPoint element against the token search value. ContactPoint only matches on
   * value - system|code syntax is not applicable.
   *
   * @param element the ContactPoint column (struct with value field)
   * @param token the parsed token search value
   * @return a boolean column indicating match
   */
  @Nonnull
  private Column matchContactPoint(@Nonnull final Column element,
      @Nonnull final TokenSearchValue token) {
    // ContactPoint only matches on value - system|code syntax not applicable
    return element.getField(VALUE_FIELD).equalTo(lit(token.requiresSimpleCode()));
  }

  /**
   * Matches a simple string value (code, uri, id) against the token search value. These types don't
   * have system - only the code value is matched.
   *
   * @param element the string column
   * @param token the parsed token search value
   * @return a boolean column indicating match
   */
  @Nonnull
  private Column matchSimpleValue(@Nonnull final Column element,
      @Nonnull final TokenSearchValue token) {
    // Simple types don't have system - just match the code value
    return element.equalTo(lit(token.requiresSimpleCode()));
  }

  /**
   * Matches a string type element against the token search value. Per FHIR spec, string token
   * searches are case-insensitive.
   *
   * @param element the string column
   * @param token the parsed token search value
   * @return a boolean column indicating match
   */
  @Nonnull
  private Column matchStringValue(@Nonnull final Column element,
      @Nonnull final TokenSearchValue token) {
    // String type token search is case-insensitive per FHIR spec
    return lower(element).equalTo(lit(token.requiresSimpleCode().toLowerCase()));
  }

  /**
   * Matches a boolean element against the token search value.
   *
   * @param element the boolean column
   * @param token the parsed token search value
   * @return a boolean column indicating match
   */
  @Nonnull
  private Column matchBoolean(@Nonnull final Column element,
      @Nonnull final TokenSearchValue token) {
    // Parse the code as boolean (case-insensitive)
    final boolean boolValue = Boolean.parseBoolean(token.requiresSimpleCode());
    return element.equalTo(lit(boolValue));
  }

  /**
   * Core matching logic for system|code pairs. Handles all four syntax variants.
   *
   * @param systemCol the system column
   * @param codeCol the code/value column
   * @param token the parsed token search value
   * @return a boolean column indicating match
   */
  @Nonnull
  private Column matchSystemAndCode(@Nonnull final Column systemCol,
      @Nonnull final Column codeCol,
      @Nonnull final TokenSearchValue token) {

    if (token.getSystem() != null && token.getCode() != null) {
      // system|code - both must match
      return systemCol.equalTo(lit(token.getSystem()))
          .and(codeCol.equalTo(lit(token.getCode())));
    } else if (token.getSystem() != null) {
      // system| - system matches, any code
      return systemCol.equalTo(lit(token.getSystem()));
    } else if (token.isExplicitNoSystem()) {
      // |code - code matches AND system must be null
      return systemCol.isNull().and(codeCol.equalTo(lit(token.getCode())));
    } else {
      // code - code matches, any system
      return codeCol.equalTo(lit(token.getCode()));
    }
  }
}
