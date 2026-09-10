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

package au.csiro.pathling.terminology.local;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * Resolves a requested language tag to the SNOMED CT language reference set that serves it, which
 * is what decides which of a concept's synonyms is its preferred term. A tag that names no
 * reference set resolves to nothing, which expresses no preference rather than an error.
 *
 * <p>A tag resolves by one of three routes, highest precedence first: the private-use dialect
 * extension form, which carries its own identifier; a deployment-configured alias; and the built-in
 * table of the language reference sets defined in the SNOMED CT International edition. A configured
 * alias for a tag that is also built in replaces the built-in entry rather than merging with it, so
 * a deployment can correct one.
 *
 * <p>Pure and stateless once constructed.
 *
 * @author John Grimes
 */
public final class DialectResolver {

  /**
   * The marker introducing a language reference set identifier within a private-use dialect
   * extension tag, as emitted by {@code LocalTerminologyService.dialectLanguage} and by the
   * reference server.
   */
  private static final String EXTENSION_MARKER = "-x-sctlang-";

  /**
   * The dialects of the SNOMED CT International edition. Each identifier was retrieved from a
   * terminology server rather than recalled; the responses are recorded in the feature's research
   * notes. Reference sets defined inside a national extension are reached through a configured
   * alias or through the extension tag form instead.
   */
  private static final Map<String, String> BUILT_IN_ALIASES =
      Map.of(
          "en-gb", "900000000000508004",
          "en-us", "900000000000509007",
          "en-au", "32570271000036106",
          "es", "448879004",
          "fr", "722131000",
          "de", "722130004",
          "ja", "722129009",
          "zh", "722128001");

  @Nonnull private final Map<String, String> aliases;

  /**
   * Creates a resolver over the built-in dialect table, extended and overridden by a deployment's
   * own aliases.
   *
   * @param configuredAliases additional mappings from a language tag to a language reference set
   *     identifier, or null for the built-in table alone
   */
  public DialectResolver(@Nullable final Map<String, String> configuredAliases) {
    final Map<String, String> combined = new HashMap<>(BUILT_IN_ALIASES);
    if (configuredAliases != null) {
      for (final Map.Entry<String, String> alias : configuredAliases.entrySet()) {
        // An entry that could never resolve is dropped here rather than at lookup time, so that a
        // malformed one cannot shadow the built-in entry it names.
        if (alias.getKey() != null
            && !alias.getKey().isBlank()
            && isReferenceSetIdentifier(alias.getValue())) {
          combined.put(normalise(alias.getKey()), alias.getValue());
        }
      }
    }
    this.aliases = Map.copyOf(combined);
  }

  /**
   * Resolves a language tag to the identifier of the language reference set that serves it.
   *
   * @param tag the requested language tag, which may be null, blank or unrecognised
   * @return the reference set identifier, or empty if the tag names none
   */
  @Nonnull
  public Optional<String> resolve(@Nullable final String tag) {
    if (tag == null || tag.isBlank()) {
      return Optional.empty();
    }
    final String normalised = normalise(tag);
    return fromExtensionTag(normalised).or(() -> Optional.ofNullable(aliases.get(normalised)));
  }

  /**
   * Reads the language reference set identifier out of a private-use dialect extension tag, which
   * carries it in hyphen-separated chunks of eight characters (for example {@code
   * en-x-sctlang-90000000-00005080-04} for {@code 900000000000508004}). This is the exact inverse
   * of the form we emit as the language of a {@code preferredForLanguage} designation.
   *
   * @param normalised the lower-cased tag
   * @return the reference set identifier, or empty if the tag is not an extension tag or is
   *     malformed
   */
  @Nonnull
  private static Optional<String> fromExtensionTag(@Nonnull final String normalised) {
    final int marker = normalised.indexOf(EXTENSION_MARKER);
    if (marker < 0) {
      return Optional.empty();
    }
    final String identifier =
        normalised.substring(marker + EXTENSION_MARKER.length()).replace("-", "");
    return isReferenceSetIdentifier(identifier) ? Optional.of(identifier) : Optional.empty();
  }

  /**
   * Reports whether a value could be a SNOMED CT concept identifier: digits only, 6 to 18
   * characters long. The import option accepts a reference set named this way directly, so the same
   * test serves both there and here.
   *
   * @param value the value to test, which may be null
   * @return true if the value is shaped like a concept identifier
   */
  public static boolean isReferenceSetIdentifier(@Nullable final String value) {
    return value != null
        && value.length() >= 6
        && value.length() <= 18
        && value.chars().allMatch(Character::isDigit);
  }

  /** Lower-cases a tag so that it matches without regard to case. */
  @Nonnull
  private static String normalise(@Nonnull final String tag) {
    return tag.toLowerCase(Locale.ROOT);
  }
}
