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
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.VALUE_SET;

import au.csiro.pathling.terminology.store.TerminologyStoreReader;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hl7.fhir.r4.model.ValueSet;

/**
 * The catalogue of imported FHIR ValueSet resources, keyed by canonical URL and version. It backs
 * explicit value set resolution and the nested value set references within a compose.
 *
 * @author John Grimes
 */
public final class ValueSetStore {

  private static FhirContext fhirContext;

  @Nonnull private final Map<String, List<VersionedValueSet>> byUrl;
  @Nonnull private final VersionResolver versionResolver;

  private ValueSetStore(
      @Nonnull final Map<String, List<VersionedValueSet>> byUrl,
      @Nonnull final VersionResolver versionResolver) {
    this.byUrl = byUrl;
    this.versionResolver = versionResolver;
  }

  @Nonnull
  private static synchronized FhirContext fhirContext() {
    if (fhirContext == null) {
      fhirContext = FhirContext.forR4();
    }
    return fhirContext;
  }

  /**
   * Loads the value set catalogue from the store, tolerating the absence of the value set table.
   *
   * @param reader the store reader
   * @param versionResolver the resolver used to select a default version
   * @return the loaded catalogue
   */
  @Nonnull
  public static ValueSetStore load(
      @Nonnull final TerminologyStoreReader reader,
      @Nonnull final VersionResolver versionResolver) {
    final Map<String, List<VersionedValueSet>> byUrl = new HashMap<>();
    reader.readTableIfPresent(
        VALUE_SET,
        row -> {
          final String url = row.getString(COLUMN_CANONICAL_URL);
          final String json = row.getString(COLUMN_RESOURCE_JSON);
          if (url == null || json == null) {
            return;
          }
          final ValueSet valueSet = (ValueSet) fhirContext().newJsonParser().parseResource(json);
          byUrl
              .computeIfAbsent(url, k -> new ArrayList<>())
              .add(new VersionedValueSet(row.getString(COLUMN_VERSION), valueSet));
        });
    return new ValueSetStore(byUrl, versionResolver);
  }

  /**
   * Resolves a value set by canonical URL and optional version. An explicit version selects exactly
   * that version; an absent version selects the default per the version-ordering rules.
   *
   * @param url the canonical URL
   * @param requestedVersion the requested version, or null for the default
   * @return the resolved value set, or empty if absent from the store
   */
  @Nonnull
  public Optional<ValueSet> resolve(
      @Nonnull final String url, @Nullable final String requestedVersion) {
    final List<VersionedValueSet> candidates = byUrl.get(url);
    if (candidates == null || candidates.isEmpty()) {
      return Optional.empty();
    }
    if (requestedVersion != null) {
      return candidates.stream()
          .filter(entry -> requestedVersion.equals(entry.version))
          .findFirst()
          .map(entry -> entry.valueSet);
    }
    if (candidates.size() == 1) {
      return Optional.of(candidates.get(0).valueSet);
    }
    final VersionedValueSet latest =
        versionResolver.getLatestOfVersions(candidates, entry -> entry.version, url);
    return Optional.ofNullable(latest).map(entry -> entry.valueSet);
  }

  /** A value set with the version it was imported under. */
  private static final class VersionedValueSet {
    @Nullable final String version;
    @Nonnull final ValueSet valueSet;

    VersionedValueSet(@Nullable final String version, @Nonnull final ValueSet valueSet) {
      this.version = version;
      this.valueSet = valueSet;
    }
  }
}
