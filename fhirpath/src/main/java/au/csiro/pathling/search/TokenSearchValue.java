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
import lombok.Getter;
import java.util.Optional;

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
 * System and code use {@link Optional} semantics:
 * <ul>
 *   <li>{@code Optional.empty()} - no constraint for that field</li>
 *   <li>{@code Optional.of("value")} - must match exact value</li>
 * </ul>
 *
 * @see <a href="https://hl7.org/fhir/search.html#token">Token Search</a>
 */
public class TokenSearchValue {

  /**
   * The system URI, or empty if not specified.
   */
  @Nonnull
  private final Optional<String> system;

  /**
   * The code value, or empty if only a system was specified.
   */
  @Nonnull
  private final Optional<String> code;

  /**
   * Whether an explicit empty system was specified (i.e., the value started with "|").
   * -- GETTER --
   *  Returns whether an explicit empty system was specified.
   *
   * @return true if the value started with "|" (explicit no system), false otherwise

   */
  @Getter
  private final boolean explicitNoSystem;

  private TokenSearchValue(@Nonnull final Optional<String> system,
      @Nonnull final Optional<String> code,
      final boolean explicitNoSystem) {
    this.system = system;
    this.code = code;
    this.explicitNoSystem = explicitNoSystem;
  }

  /**
   * Parses a token search value string.
   * <p>
   * Parsing rules:
   * <ul>
   *   <li>{@code "male"} → system=empty, code="male", explicitNoSystem=false</li>
   *   <li>{@code "http://example.org|male"} → system="http://example.org", code="male",
   *       explicitNoSystem=false</li>
   *   <li>{@code "|male"} → system=empty, code="male", explicitNoSystem=true</li>
   *   <li>{@code "http://example.org|"} → system="http://example.org", code=empty,
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
      return new TokenSearchValue(Optional.empty(), Optional.of(value), false);
    }

    final String systemPart = value.substring(0, pipeIndex);
    final String codePart = value.substring(pipeIndex + 1);

    final Optional<String> system = systemPart.isEmpty() ? Optional.empty() : Optional.of(systemPart);
    final Optional<String> code = codePart.isEmpty() ? Optional.empty() : Optional.of(codePart);
    final boolean explicitNoSystem = systemPart.isEmpty();

    return new TokenSearchValue(system, code, explicitNoSystem);
  }

  /**
   * Gets the system URI.
   *
   * @return the system URI to match, or empty for no constraint (any system)
   */
  @Nonnull
  public Optional<String> getSystem() {
    return system;
  }

  /**
   * Gets the code value.
   *
   * @return the code to match, or empty for no constraint (any code)
   */
  @Nonnull
  public Optional<String> getCode() {
    return code;
  }

  /**
   * Returns the code value for types that only support simple code values (no system).
   * <p>
   * This method validates that:
   * <ul>
   *   <li>No system was specified (system|code syntax is not allowed)</li>
   *   <li>A code value is present</li>
   * </ul>
   * <p>
   * Use this for FHIR types that don't have a system field, such as:
   * {@code ContactPoint}, {@code code}, {@code uri}, {@code id}, {@code string}, {@code boolean}.
   *
   * @return the non-null code value
   * @throws IllegalArgumentException if a system was specified or code is missing
   */
  @Nonnull
  public String requiresSimpleCode() {
    if (system.isPresent()) {
      final String systemValue = system.get();
      final String codeValue = code.orElse("");
      throw new IllegalArgumentException(
          "System|code syntax is not supported for this search parameter type. "
              + "Use a simple code value instead of: " + systemValue + "|" + codeValue);
    }
    return code.orElseThrow(() -> new IllegalArgumentException(
        "A code value is required for this search parameter type."));
  }
}
