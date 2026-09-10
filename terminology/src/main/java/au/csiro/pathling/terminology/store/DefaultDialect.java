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

package au.csiro.pathling.terminology.store;

import au.csiro.pathling.terminology.local.DialectResolver;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/**
 * Chooses the language reference set whose preferred synonyms become the stored display of every
 * concept in a release, and which therefore answers any query that names no dialect the store can
 * serve.
 *
 * <p>No SNOMED CT release declares which of its language reference sets is the default, so nothing
 * in the release metadata is consulted. The import option decides; failing that, a release holding
 * exactly one reference set uses it, and the International edition uses US English. A release that
 * holds several and offers neither of those answers is refused rather than guessed at, and the
 * refusal names every candidate so that the operator can choose one.
 *
 * @author John Grimes
 */
@Slf4j
public final class DefaultDialect {

  /** The SNOMED CT core module, which identifies the International edition. */
  private static final String INTERNATIONAL_EDITION = "900000000000207008";

  /** The US English language reference set, the default of the International edition. */
  private static final String US_ENGLISH = "900000000000509007";

  /**
   * Orders reference set identifiers by value. They are digit strings without leading zeros, so
   * ordering by length and then as text is numeric ordering, and a twelve-digit identifier
   * correctly precedes an eighteen-digit one.
   */
  private static final Comparator<String> BY_IDENTIFIER =
      Comparator.comparingInt(String::length).thenComparing(Comparator.naturalOrder());

  /** Stands in for the name of a reference set the release holds no description for. */
  private static final String UNNAMED = "(not named in this release)";

  private DefaultDialect() {
    // Utility class.
  }

  /**
   * Chooses the default language reference set for a release, reporting the choice and how it was
   * arrived at.
   *
   * @param requested the value of the {@code defaultDialect} import option, or null to derive the
   *     choice from the release
   * @param editionModule the detected edition module of the release, or null if unknown
   * @param candidatesByName the language reference sets the release holds, keyed by identifier and
   *     valued by the fully specified name the release gives that concept, which may be null
   * @return the chosen reference set identifier, or null when the release holds none
   * @throws TerminologyImportException if the option cannot be honoured, or the release holds
   *     several reference sets and no rule selects one
   */
  @Nullable
  public static String choose(
      @Nullable final String requested,
      @Nullable final String editionModule,
      @Nonnull final Map<String, String> candidatesByName) {
    if (requested != null && !requested.isBlank()) {
      return named(requested.trim(), candidatesByName);
    }
    if (candidatesByName.isEmpty()) {
      log.info(
          "The release holds no language reference set, so each concept's display will be its fully"
              + " specified name, or its code where it has none.");
      return null;
    }
    if (candidatesByName.size() == 1) {
      final String only = candidatesByName.keySet().iterator().next();
      log.info(
          "Using language reference set {} ({}) as the default dialect: it is the only one this"
              + " release holds.",
          only,
          nameOf(candidatesByName, only));
      return only;
    }
    if (INTERNATIONAL_EDITION.equals(editionModule) && candidatesByName.containsKey(US_ENGLISH)) {
      log.info(
          "Using language reference set {} ({}) as the default dialect: the default of the SNOMED"
              + " CT International edition.",
          US_ENGLISH,
          nameOf(candidatesByName, US_ENGLISH));
      return US_ENGLISH;
    }
    throw new TerminologyImportException(
        "The release holds "
            + candidatesByName.size()
            + " language reference sets and none of them is a clear default. Name one with the"
            + " defaultDialect import option:\n"
            + candidateList(candidatesByName));
  }

  /**
   * Resolves the value of the import option to a reference set the release holds.
   *
   * @param requested the trimmed, non-blank option value
   * @param candidatesByName the language reference sets the release holds
   * @return the chosen reference set identifier
   * @throws TerminologyImportException if the value names no reference set, or one the release does
   *     not hold
   */
  @Nonnull
  private static String named(
      @Nonnull final String requested, @Nonnull final Map<String, String> candidatesByName) {
    // A bare identifier is accepted directly, because the import receives no service configuration
    // and so has no way to reach a reference set outside the built-in alias table by a tag.
    final Optional<String> resolved =
        DialectResolver.isReferenceSetIdentifier(requested)
            ? Optional.of(requested)
            : new DialectResolver(null).resolve(requested);
    if (resolved.isEmpty()) {
      throw new TerminologyImportException(
          "The defaultDialect import option '"
              + requested
              + "' is not a recognised dialect tag, a private-use dialect extension tag, or a"
              + " language reference set identifier.");
    }
    final String refsetId = resolved.get();
    if (!candidatesByName.containsKey(refsetId)) {
      throw new TerminologyImportException(
          "The defaultDialect import option '"
              + requested
              + "' names language reference set "
              + refsetId
              + ", which this release does not hold. The release holds:\n"
              + candidateList(candidatesByName));
    }
    log.info(
        "Using language reference set {} ({}) as the default dialect: named by the defaultDialect"
            + " import option as '{}'.",
        refsetId,
        nameOf(candidatesByName, refsetId),
        requested);
    return refsetId;
  }

  /**
   * Renders the candidate reference sets as indented lines, in ascending identifier order so that
   * the message is itself reproducible whatever order the release's rows were read in.
   *
   * @param candidatesByName the language reference sets the release holds
   * @return the rendered lines, without a trailing newline
   */
  @Nonnull
  private static String candidateList(@Nonnull final Map<String, String> candidatesByName) {
    return candidatesByName.keySet().stream()
        .sorted(BY_IDENTIFIER)
        .map(refsetId -> "  " + refsetId + "  " + nameOf(candidatesByName, refsetId))
        .collect(Collectors.joining("\n"));
  }

  /** Returns the name the release gives a reference set, or a stand-in where it gives none. */
  @Nonnull
  private static String nameOf(
      @Nonnull final Map<String, String> candidatesByName, @Nonnull final String refsetId) {
    final String name = candidatesByName.get(refsetId);
    return name == null || name.isBlank() ? UNNAMED : name;
  }
}
