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

import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_CANONICAL_URL;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_RESOURCE_JSON;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_VERSION;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.CONCEPT_MAP;

import au.csiro.pathling.terminology.conceptmap.ConceptMapVersionException;
import au.csiro.pathling.terminology.store.TerminologyStoreReader;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.hl7.fhir.r4.model.ConceptMap;

/**
 * Resolves an imported FHIR ConceptMap resource by canonical URL and version, on demand. The {@code
 * concept_map} table holds one row per imported map, so it is scanned each time rather than held
 * resident, and only the JSON of the chosen row is parsed.
 *
 * @author John Grimes
 */
public final class ConceptMapStore {

  private static FhirContext fhirContext;

  private ConceptMapStore() {
    // Static helper.
  }

  @Nonnull
  private static synchronized FhirContext fhirContext() {
    if (fhirContext == null) {
      fhirContext = FhirContext.forR4();
    }
    return fhirContext;
  }

  /**
   * Resolves a concept map by canonical URL and optional version. A pinned version selects exactly
   * that version; an absent version selects the only version, or the latest of several under the
   * version-ordering rules the store applies to value sets.
   *
   * @param reader the store reader
   * @param url the canonical URL
   * @param version the requested version, or null for the latest
   * @param versionResolver the resolver that selects the latest of several versions
   * @return the resource, or empty if the store holds no map at the URL and version
   * @throws ConceptMapVersionException if several versions are held and no latest can be determined
   */
  @Nonnull
  public static Optional<ConceptMap> resolve(
      @Nonnull final TerminologyStoreReader reader,
      @Nonnull final String url,
      @Nullable final String version,
      @Nonnull final VersionResolver versionResolver) {
    final List<VersionedConceptMapJson> candidates = new ArrayList<>();
    reader.readTableIfPresent(
        CONCEPT_MAP,
        row -> {
          if (url.equals(row.getString(COLUMN_CANONICAL_URL))) {
            final String json = row.getString(COLUMN_RESOURCE_JSON);
            if (json != null) {
              candidates.add(new VersionedConceptMapJson(row.getString(COLUMN_VERSION), json));
            }
          }
        });
    if (candidates.isEmpty()) {
      return Optional.empty();
    }
    final VersionedConceptMapJson chosen;
    if (version != null) {
      chosen =
          candidates.stream()
              .filter(candidate -> version.equals(candidate.getVersion()))
              .findFirst()
              .orElse(null);
    } else if (candidates.size() == 1) {
      chosen = candidates.get(0);
    } else {
      try {
        chosen =
            versionResolver.getLatestOfVersions(
                candidates, VersionedConceptMapJson::getVersion, url);
      } catch (final AmbiguousVersionException e) {
        throw new ConceptMapVersionException(e.getMessage(), e);
      }
    }
    return Optional.ofNullable(chosen).map(candidate -> parse(candidate.getJson()));
  }

  @Nonnull
  private static ConceptMap parse(@Nonnull final String json) {
    return (ConceptMap) fhirContext().newJsonParser().parseResource(json);
  }
}
