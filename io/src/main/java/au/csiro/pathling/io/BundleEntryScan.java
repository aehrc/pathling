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

package au.csiro.pathling.io;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * What a bundle route needs to know of one entry of a bundle: its full URL, the type, id and
 * version of its resource, and, where the resource is of the type wanted, the resource as a
 * document and the URNs it mentions (decision 83, FR-062).
 */
final class BundleEntryScan {

  @Nullable private String fullUrl;

  @Nullable private String type;

  @Nullable private String id;

  @Nullable private String versionId;

  @Nullable private String document;

  @Nonnull private Set<String> urns = Set.of();

  /**
   * Records the entry's full URL.
   *
   * @param fullUrl the full URL, or null where the entry has none
   */
  void setFullUrl(@Nullable final String fullUrl) {
    this.fullUrl = fullUrl;
  }

  /**
   * Records the entry's resource of an XML bundle.
   *
   * @param resourceType the type of the resource
   * @param scan the scan the resource was read with
   * @param wanted whether the resource is of the type wanted, so that the scan wrote it out
   */
  void setResource(
      @Nonnull final String resourceType, @Nonnull final ResourceScan scan, final boolean wanted) {
    setResource(
        resourceType,
        scan.getId().orElse(null),
        scan.getVersionId().orElse(null),
        wanted ? scan.document() : null,
        scan.getUrns());
  }

  /**
   * Records the entry's resource.
   *
   * @param resourceType the type of the resource
   * @param resourceId the id of the resource, or null where it has none
   * @param version the version its metadata names, or null where it names none
   * @param wantedDocument the resource as a document, where it is of the type wanted, else null
   * @param mentioned the URNs the resource's references hold
   */
  void setResource(
      @Nonnull final String resourceType,
      @Nullable final String resourceId,
      @Nullable final String version,
      @Nullable final String wantedDocument,
      @Nonnull final Set<String> mentioned) {
    this.type = resourceType;
    this.id = resourceId;
    this.versionId = version;
    this.document = wantedDocument;
    this.urns = mentioned;
  }

  /**
   * Returns the resources wanted from a bundle's entries, each with the resolutions of the URNs it
   * mentions.
   *
   * <p>A URN resolves to the relative reference of the entry whose full URL it is, {@code Type/id},
   * with {@code /_history/version} where the entry's metadata names a version. An entry without a
   * resource or an id is never resolved to, and where two entries share a full URL the first that
   * can be resolved to is taken.
   *
   * @param entries the entries, in the order the bundle carries them
   * @return the resources wanted, in that order
   */
  @Nonnull
  static List<BundleResource> resolve(@Nonnull final List<BundleEntryScan> entries) {
    final Map<String, String> targets = new HashMap<>();
    entries.forEach(
        entry -> entry.target().ifPresent(target -> targets.putIfAbsent(entry.fullUrl, target)));
    return entries.stream()
        .filter(entry -> entry.document != null)
        .map(
            entry ->
                new BundleResource(entry.document, XmlDocuments.resolutions(entry.urns, targets)))
        .toList();
  }

  /** Returns the relative reference a URN naming this entry resolves to, where it can be named. */
  @Nonnull
  private Optional<String> target() {
    if (fullUrl == null || !fullUrl.startsWith(XmlDocuments.URN) || type == null || id == null) {
      return Optional.empty();
    }
    return Optional.of(type + "/" + id + (versionId == null ? "" : "/_history/" + versionId));
  }
}
