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

package au.csiro.pathling.operations.sql;

import au.csiro.pathling.views.FhirView;
import jakarta.annotation.Nonnull;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.hl7.fhir.r4.model.OperationOutcome.IssueType;

/**
 * The {@code context} entries supplied with a {@code $sql-run} or {@code $sql-export} request,
 * matched to the request's transitive dependency graph by canonical URL.
 *
 * <p>Entries are keyed by URL and outrank server resolution, so a request can run entirely ad-hoc
 * against artefacts that exist nowhere on the server. The collection records which entries were
 * consulted, because an entry that matches no dependency is a client error rather than a harmless
 * extra: it usually means a URL was mistyped, and silently ignoring it would run the request
 * against different artefacts than the client intended.
 *
 * <p>The match check runs after the whole graph has been traversed, since an entry may be reached
 * only through another supplied entry.
 *
 * @author John Grimes
 */
public class SuppliedArtefacts {

  /** The {@code expression} value naming the context parameter in error outcomes. */
  public static final String CONTEXT_EXPRESSION = "context";

  @Nonnull private final Map<String, SuppliedArtefact> byUrl;

  @Nonnull private final Set<String> matchedUrls = new LinkedHashSet<>();

  private SuppliedArtefacts(@Nonnull final Map<String, SuppliedArtefact> byUrl) {
    this.byUrl = byUrl;
  }

  /**
   * Returns an empty collection, for a request that supplies no {@code context}.
   *
   * @return an empty collection
   */
  @Nonnull
  public static SuppliedArtefacts empty() {
    return new SuppliedArtefacts(Map.of());
  }

  /**
   * Builds a collection from parsed entries, rejecting a duplicated canonical URL: two entries
   * claiming the same URL leave the artefact a dependency resolves to undetermined.
   *
   * @param entries the parsed entries, in request order
   * @return the collection
   * @throws ca.uhn.fhir.rest.server.exceptions.InvalidRequestException (400) if two entries share a
   *     canonical URL
   */
  @Nonnull
  public static SuppliedArtefacts of(@Nonnull final List<SuppliedArtefact> entries) {
    final Map<String, SuppliedArtefact> byUrl = new LinkedHashMap<>();
    for (final SuppliedArtefact entry : entries) {
      if (byUrl.putIfAbsent(entry.getUrl(), entry) != null) {
        throw SqlOperationError.badRequest(
            IssueType.INVALID,
            CONTEXT_EXPRESSION,
            "Two 'context' entries share the canonical URL '%s'; each entry must claim a distinct"
                    .formatted(entry.getUrl())
                + " URL.");
      }
    }
    return new SuppliedArtefacts(byUrl);
  }

  /**
   * Adapts a legacy map of views keyed by the URL they satisfy. Used by the callers that predate
   * the {@code context} parameter and do not enforce the unmatched-entry rule.
   *
   * @param views the views keyed by canonical URL
   * @return the collection
   */
  @Nonnull
  public static SuppliedArtefacts ofViews(@Nonnull final Map<String, FhirView> views) {
    final Map<String, SuppliedArtefact> byUrl = new LinkedHashMap<>();
    views.forEach((url, view) -> byUrl.put(url, SuppliedArtefact.ofView(url, null, view)));
    return new SuppliedArtefacts(byUrl);
  }

  /**
   * Finds the entry satisfying a dependency reference, recording the match. An entry satisfies the
   * reference when its URL matches and, where the reference pins a version, its version agrees.
   *
   * @param url the dependency's canonical URL, without any version suffix
   * @param version the version the dependency pins, or null when unpinned
   * @return the matching entry, or empty when none matches
   */
  @Nonnull
  public Optional<SuppliedArtefact> match(@Nonnull final String url, final String version) {
    final SuppliedArtefact entry = byUrl.get(url);
    if (entry == null || !entry.satisfiesVersion(version)) {
      return Optional.empty();
    }
    matchedUrls.add(url);
    return Optional.of(entry);
  }

  /**
   * Indicates whether any entries were supplied.
   *
   * @return true if the collection is empty
   */
  public boolean isEmpty() {
    return byUrl.isEmpty();
  }

  /**
   * Rejects any entry that matched no dependency in the traversed graph. Called once the whole
   * request's dependency graph has been resolved, since an entry may be reached only through
   * another supplied entry.
   *
   * @throws ca.uhn.fhir.rest.server.exceptions.InvalidRequestException (400) if an entry matched
   *     nothing
   */
  public void checkAllMatched() {
    final List<String> unmatched =
        byUrl.keySet().stream().filter(url -> !matchedUrls.contains(url)).toList();
    if (!unmatched.isEmpty()) {
      throw SqlOperationError.badRequest(
          IssueType.INVALID,
          CONTEXT_EXPRESSION,
          "The following 'context' entries match no dependency of any subject: %s."
              .formatted(String.join(", ", unmatched)));
    }
  }
}
