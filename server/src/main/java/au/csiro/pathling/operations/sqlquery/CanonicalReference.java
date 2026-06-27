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

package au.csiro.pathling.operations.sqlquery;

import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A canonical reference parsed into its {@code url} and optional {@code version}, with the
 * candidate-selection rule shared by the {@code Library} and {@code ViewDefinition} resolution
 * paths. A canonical reference takes the form {@code [url]} or {@code [url]|[version]}; matching is
 * always against the candidate resource's {@code url} element, never its logical id.
 *
 * <p>This is the single place the SQL on FHIR resolution chain parses a canonical and selects among
 * candidates sharing a url:
 *
 * <ul>
 *   <li>with an explicit version, the caller has already filtered to that exact version, so any
 *       remaining candidate suffices;
 *   <li>without a version, the latest active match wins - preferring {@code status = active}, then
 *       the lexicographically greatest version string (FHIR does not constrain the shape of a
 *       version, so this is a reasonable proxy for "latest").
 * </ul>
 *
 * @author John Grimes
 */
public final class CanonicalReference {

  @Nonnull private final String url;

  @Nullable private final String version;

  private CanonicalReference(@Nonnull final String url, @Nullable final String version) {
    this.url = url;
    this.version = version;
  }

  /**
   * Parses a canonical reference of the form {@code [url]} or {@code [url]|[version]}. An empty
   * version suffix (a trailing {@code |}) is treated as no version.
   *
   * @param canonical the canonical reference to parse
   * @return the parsed reference
   * @throws InvalidRequestException if the url segment is blank
   */
  @Nonnull
  public static CanonicalReference parse(@Nonnull final String canonical) {
    final int pipe = canonical.indexOf('|');
    final String url = pipe >= 0 ? canonical.substring(0, pipe) : canonical;
    final String rawVersion = pipe >= 0 ? canonical.substring(pipe + 1) : null;
    if (url.isBlank()) {
      throw new InvalidRequestException(
          "Canonical reference '" + canonical + "' is missing the url segment");
    }
    final String version = rawVersion != null && !rawVersion.isBlank() ? rawVersion : null;
    return new CanonicalReference(url, version);
  }

  /**
   * Indicates whether a value is an absolute canonical URL acceptable as a dependency reference: it
   * must use the {@code http://}, {@code https://}, or {@code urn:} scheme, may carry at most one
   * {@code |version} suffix, and must not carry a fragment ({@code #...}).
   *
   * @param value the value to test
   * @return {@code true} if the value is an absolute canonical URL
   */
  public static boolean isCanonical(@Nullable final String value) {
    if (value == null || value.isBlank()) {
      return false;
    }
    if (value.indexOf('#') >= 0) {
      // Fragments are not supported.
      return false;
    }
    final int pipe = value.indexOf('|');
    if (pipe >= 0 && value.indexOf('|', pipe + 1) >= 0) {
      // At most one version suffix is permitted.
      return false;
    }
    final String url = pipe >= 0 ? value.substring(0, pipe) : value;
    return url.startsWith("http://") || url.startsWith("https://") || url.startsWith("urn:");
  }

  /**
   * Computes the canonical key identifying a resolved resource: its {@code url} plus its {@code
   * version} when it has one ({@code url|version}), else the bare {@code url}. A bare-url reference
   * and an explicit {@code url|version} reference that resolve to the same stored resource share
   * this key, so the resource de-duplicates and materialises once.
   *
   * @param url the resolved resource's url
   * @param version the resolved resource's version, if any
   * @return the canonical key
   */
  @Nonnull
  public static String key(@Nonnull final String url, @Nullable final String version) {
    return version != null && !version.isBlank() ? url + "|" + version : url;
  }

  /**
   * Selects the most appropriate candidate among those already matched on this reference's url.
   * When this reference carries a version, the caller has already filtered to that exact version,
   * so the first candidate is returned. Otherwise the latest active candidate wins: preferring
   * {@code active} status, then the greatest version string.
   *
   * @param candidates the candidates already filtered to share this reference's url (and version,
   *     when one was supplied); must be non-empty
   * @param isActive predicate identifying a candidate with {@code active} status
   * @param versionOf extracts a candidate's version string (may return {@code null})
   * @param <T> the candidate resource type
   * @return the selected candidate
   * @throws IllegalStateException if no candidates are supplied
   */
  @Nonnull
  public <T> T select(
      @Nonnull final List<T> candidates,
      @Nonnull final Predicate<T> isActive,
      @Nonnull final Function<T, String> versionOf) {
    if (candidates.isEmpty()) {
      throw new IllegalStateException("Cannot select from an empty candidate list");
    }
    if (candidates.size() == 1 || version != null) {
      return candidates.get(0);
    }
    return candidates.stream()
        .max(
            Comparator.comparing(isActive::test)
                .thenComparing(candidate -> Objects.toString(versionOf.apply(candidate), "")))
        .orElseThrow(() -> new IllegalStateException("Cannot select from an empty candidate list"));
  }

  /**
   * Returns the url segment of this canonical reference.
   *
   * @return the non-blank url
   */
  @Nonnull
  public String getUrl() {
    return url;
  }

  /**
   * Returns the version segment of this canonical reference, if one was supplied.
   *
   * @return the version, or {@code null} when none was supplied
   */
  @Nullable
  public String getVersion() {
    return version;
  }

  /**
   * Indicates whether this canonical reference carries an explicit version.
   *
   * @return {@code true} if a version was supplied
   */
  public boolean hasVersion() {
    return version != null;
  }
}
