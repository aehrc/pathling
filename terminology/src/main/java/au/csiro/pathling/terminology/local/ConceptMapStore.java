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

import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_CONCEPT_MAP_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_EQUIVALENCE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_ORDINAL;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_SOURCE_CODE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_SOURCE_DISPLAY;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_SOURCE_SYSTEM;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_SOURCE_VERSION;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_TARGET_CODE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_TARGET_DISPLAY;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_TARGET_SYSTEM;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_TARGET_VERSION;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.CONCEPT_MAPPING;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.ENTRY_TYPE_CONCEPT_MAP;

import au.csiro.pathling.terminology.conceptmap.ConceptMapContent;
import au.csiro.pathling.terminology.conceptmap.ConceptMapContentException;
import au.csiro.pathling.terminology.conceptmap.ConceptMapLimitExceededException;
import au.csiro.pathling.terminology.conceptmap.ConceptMapVersionException;
import au.csiro.pathling.terminology.conceptmap.ConceptMapping;
import au.csiro.pathling.terminology.store.ManifestEntry;
import au.csiro.pathling.terminology.store.TerminologyStoreReader;
import au.csiro.pathling.terminology.store.TerminologyStoreRow;
import au.csiro.pathling.terminology.store.TerminologyStoreSchema;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hl7.fhir.r4.model.Enumerations.ConceptMapEquivalence;

/**
 * Reads an imported FHIR ConceptMap as the rows of a concept map relation, on demand, from the
 * {@code concept_mapping} rows of one version of the map. The rows, together with the fault the
 * importer recorded in the manifest where the map cannot be carried as a relation, hold what {@link
 * ConceptMapContent#fromResource} needs, so the content read is what it would make of the imported
 * resource, which the store does not keep.
 *
 * <p>A map with a fault is rejected before any of its rows are read. Otherwise only the rows of the
 * chosen version's partition are read, and reading stops once there are more distinct rows than the
 * caller's cap, so a map too large for the caller costs memory in proportion to the cap rather than
 * to the map.
 *
 * @author John Grimes
 */
public final class ConceptMapStore {

  private ConceptMapStore() {
    // Static helper.
  }

  /**
   * Reads a concept map by canonical URL and optional version. A pinned version selects exactly
   * that version; an absent version selects the only version, or the latest of several under the
   * version-ordering rules the store applies to value sets.
   *
   * @param reader the store reader
   * @param url the canonical URL
   * @param version the requested version, or null for the latest
   * @param versionResolver the resolver that selects the latest of several versions
   * @param maxMappings the largest number of rows the caller will accept
   * @return the content, or empty if the store holds no map at the URL and version
   * @throws ConceptMapVersionException if several versions are held and no latest can be determined
   * @throws ConceptMapContentException if the content of the map cannot be represented, for the
   *     reason {@link ConceptMapContent#fromResource} would give
   * @throws ConceptMapLimitExceededException if the map has more than {@code maxMappings} rows and
   *     its content can otherwise be represented
   */
  @Nonnull
  public static Optional<ConceptMapContent> read(
      @Nonnull final TerminologyStoreReader reader,
      @Nonnull final String url,
      @Nullable final String version,
      @Nonnull final VersionResolver versionResolver,
      final int maxMappings) {
    final List<ManifestEntry> entries =
        reader.readManifest().stream()
            .filter(entry -> ENTRY_TYPE_CONCEPT_MAP.equals(entry.getEntryType()))
            .filter(entry -> url.equals(entry.getCanonicalUrl()))
            .toList();
    final ManifestEntry chosen;
    if (version != null) {
      chosen =
          entries.stream()
              .filter(entry -> version.equals(entry.getVersion()))
              .findFirst()
              .orElse(null);
    } else if (entries.size() <= 1) {
      chosen = entries.isEmpty() ? null : entries.get(0);
    } else {
      try {
        chosen = versionResolver.getLatestOfVersions(entries, ManifestEntry::getVersion, url);
      } catch (final AmbiguousVersionException e) {
        throw new ConceptMapVersionException(e.getMessage(), e);
      }
    }
    if (chosen == null) {
      return Optional.empty();
    }
    if (chosen.getFault() != null) {
      throw new ConceptMapContentException(chosen.getFault());
    }
    final Collector collector = new Collector(maxMappings);
    reader.readPartitionIfPresent(
        CONCEPT_MAPPING,
        COLUMN_CONCEPT_MAP_ID,
        TerminologyStoreSchema.conceptMapId(url, chosen.getVersion()),
        collector::add);
    return Optional.of(collector.content(url, chosen.getVersion()));
  }

  /**
   * Gathers the rows of one map version, which arrive in no particular order, keeping the first in
   * document order of each distinct row.
   */
  private static final class Collector {

    private final int maxMappings;
    @Nonnull private final Map<String, Positioned> retained = new HashMap<>();

    Collector(final int maxMappings) {
      this.maxMappings = maxMappings;
    }

    /**
     * Keeps a row unless an earlier row is the same mapping.
     *
     * @throws ConceptMapLimitExceededException once more distinct rows than the cap have been read;
     *     the rest of the rows are not read
     */
    void add(@Nonnull final TerminologyStoreRow row) {
      final Positioned mapping = new Positioned(row.getInt(COLUMN_ORDINAL), mapping(row));
      retained.merge(
          mapping.mapping().identity(),
          mapping,
          (kept, other) -> kept.ordinal() <= other.ordinal() ? kept : other);
      if (retained.size() > maxMappings) {
        throw new ConceptMapLimitExceededException(maxMappings);
      }
    }

    @Nonnull
    ConceptMapContent content(@Nonnull final String url, @Nullable final String version) {
      return ConceptMapContent.fromMappings(
          url,
          version,
          retained.values().stream()
              .sorted(Comparator.comparingInt(Positioned::ordinal))
              .map(Positioned::mapping)
              .toList(),
          maxMappings);
    }

    @Nonnull
    private static ConceptMapping mapping(@Nonnull final TerminologyStoreRow row) {
      return ConceptMapContent.mapping(
          row.getString(COLUMN_SOURCE_SYSTEM),
          row.getString(COLUMN_SOURCE_VERSION),
          row.getString(COLUMN_TARGET_SYSTEM),
          row.getString(COLUMN_TARGET_VERSION),
          row.getString(COLUMN_SOURCE_CODE),
          row.getString(COLUMN_SOURCE_DISPLAY),
          row.getString(COLUMN_TARGET_CODE),
          row.getString(COLUMN_TARGET_DISPLAY),
          ConceptMapEquivalence.fromCode(row.getString(COLUMN_EQUIVALENCE)));
    }
  }

  /** A row together with its position in the map. */
  private record Positioned(int ordinal, @Nonnull ConceptMapping mapping) {}
}
