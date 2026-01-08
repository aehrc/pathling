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
import jakarta.annotation.Nullable;
import lombok.Value;

/**
 * Represents a parsed token search value.
 * <p>
 * Token search values can be specified in several formats:
 * <ul>
 *   <li>{@code code} - matches any resource with the given code</li>
 *   <li>{@code system|code} - matches resources with the given system and code</li>
 *   <li>{@code |code} - matches resources with the given code and no system</li>
 *   <li>{@code system|} - matches any resource with any code in the given system</li>
 * </ul>
 *
 * @see <a href="https://hl7.org/fhir/search.html#token">Token Search</a>
 */
@Value
public class TokenSearchValue {

  /**
   * The system URI, or null if not specified.
   */
  @Nullable
  String system;

  /**
   * The code value, or null if only a system was specified.
   */
  @Nullable
  String code;

  /**
   * Whether an explicit empty system was specified (i.e., the value started with "|").
   */
  boolean explicitNoSystem;

  /**
   * Parses a token search value string.
   * <p>
   * Parsing rules:
   * <ul>
   *   <li>{@code "male"} → system=null, code="male", explicitNoSystem=false</li>
   *   <li>{@code "http://example.org|male"} → system="http://example.org", code="male",
   *       explicitNoSystem=false</li>
   *   <li>{@code "|male"} → system=null, code="male", explicitNoSystem=true</li>
   *   <li>{@code "http://example.org|"} → system="http://example.org", code=null,
   *       explicitNoSystem=false</li>
   * </ul>
   *
   * @param value the token value string to parse
   * @return the parsed token search value
   */
  @Nonnull
  public static TokenSearchValue parse(@Nonnull final String value) {
    final int pipeIndex = value.indexOf('|');

    if (pipeIndex < 0) {
      // No pipe: just a code
      return new TokenSearchValue(null, value, false);
    }

    final String systemPart = value.substring(0, pipeIndex);
    final String codePart = value.substring(pipeIndex + 1);

    final String system = systemPart.isEmpty() ? null : systemPart;
    final String code = codePart.isEmpty() ? null : codePart;
    final boolean explicitNoSystem = systemPart.isEmpty();

    return new TokenSearchValue(system, code, explicitNoSystem);
  }
}
