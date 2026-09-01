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

package au.csiro.pathling.terminology.local.index;

import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_CANONICAL_URL;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_RESOURCE_JSON;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.CONCEPT_MAP;

import au.csiro.pathling.terminology.TerminologyService.Translation;
import au.csiro.pathling.terminology.store.TerminologyStoreReader;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.ConceptMap;
import org.hl7.fhir.r4.model.ConceptMap.ConceptMapGroupComponent;
import org.hl7.fhir.r4.model.ConceptMap.SourceElementComponent;
import org.hl7.fhir.r4.model.ConceptMap.TargetElementComponent;
import org.hl7.fhir.r4.model.codesystems.ConceptMapEquivalence;

/**
 * The concept map index: the mappings of every imported FHIR ConceptMap, keyed by canonical URL and
 * addressable in both directions. It backs local {@code translate} for explicit concept maps.
 *
 * @author John Grimes
 */
public final class ConceptMapIndex {

  private static FhirContext fhirContext;

  @Nonnull private final Map<String, Mappings> byUrl;

  private ConceptMapIndex(@Nonnull final Map<String, Mappings> byUrl) {
    this.byUrl = byUrl;
  }

  @Nonnull
  private static synchronized FhirContext fhirContext() {
    if (fhirContext == null) {
      fhirContext = FhirContext.forR4();
    }
    return fhirContext;
  }

  /**
   * Loads the concept map index from the store, tolerating the absence of the concept map table.
   *
   * @param reader the store reader
   * @return the loaded index
   */
  @Nonnull
  public static ConceptMapIndex load(@Nonnull final TerminologyStoreReader reader) {
    final Map<String, Mappings> byUrl = new HashMap<>();
    reader.readTableIfPresent(
        CONCEPT_MAP,
        row -> {
          final String url = row.getString(COLUMN_CANONICAL_URL);
          final String json = row.getString(COLUMN_RESOURCE_JSON);
          if (url == null || json == null) {
            return;
          }
          final ConceptMap conceptMap =
              (ConceptMap) fhirContext().newJsonParser().parseResource(json);
          byUrl.put(url, index(conceptMap));
        });
    return new ConceptMapIndex(byUrl);
  }

  @Nonnull
  private static Mappings index(@Nonnull final ConceptMap conceptMap) {
    final Mappings mappings = new Mappings();
    for (final ConceptMapGroupComponent group : conceptMap.getGroup()) {
      final String sourceSystem = group.getSource();
      final String targetSystem = group.getTarget();
      for (final SourceElementComponent element : group.getElement()) {
        for (final TargetElementComponent target : element.getTarget()) {
          final ConceptMapEquivalence equivalence =
              target.hasEquivalence()
                  ? ConceptMapEquivalence.fromCode(target.getEquivalence().toCode())
                  : ConceptMapEquivalence.RELATEDTO;
          mappings.add(
              sourceSystem, element.getCode(), targetSystem, target.getCode(), equivalence);
        }
      }
    }
    return mappings;
  }

  /**
   * Translates a coding through a concept map. Filtering to a target value set, when requested, is
   * applied by the caller against the resolved value set membership, matching remote-mode
   * behaviour.
   *
   * @param conceptMapUrl the canonical URL of the concept map
   * @param system the coding system
   * @param code the coding code
   * @param reverse whether to translate target to source instead of source to target
   * @return the translations, empty if the map or the code is unknown
   */
  @Nonnull
  public List<Translation> translate(
      @Nonnull final String conceptMapUrl,
      @Nonnull final String system,
      @Nonnull final String code,
      final boolean reverse) {
    final Mappings mappings = byUrl.get(conceptMapUrl);
    if (mappings == null) {
      return List.of();
    }
    final List<MapTarget> targets =
        (reverse ? mappings.reverse : mappings.forward).getOrDefault(key(system, code), List.of());
    final List<Translation> result = new ArrayList<>();
    for (final MapTarget mapTarget : targets) {
      result.add(
          Translation.of(
              mapTarget.equivalence,
              new Coding().setSystem(mapTarget.system).setCode(mapTarget.code)));
    }
    return result;
  }

  @Nonnull
  private static String key(@Nullable final String system, @Nonnull final String code) {
    return (system == null ? "" : system) + "|" + code;
  }

  /** Forward and reverse mappings of a single concept map. */
  private static final class Mappings {
    final Map<String, List<MapTarget>> forward = new HashMap<>();
    final Map<String, List<MapTarget>> reverse = new HashMap<>();

    void add(
        @Nullable final String sourceSystem,
        @Nonnull final String sourceCode,
        @Nullable final String targetSystem,
        @Nonnull final String targetCode,
        @Nonnull final ConceptMapEquivalence equivalence) {
      forward
          .computeIfAbsent(key(sourceSystem, sourceCode), k -> new ArrayList<>())
          .add(new MapTarget(targetSystem, targetCode, equivalence));
      reverse
          .computeIfAbsent(key(targetSystem, targetCode), k -> new ArrayList<>())
          .add(new MapTarget(sourceSystem, sourceCode, invert(equivalence)));
    }

    /** Inverts an equivalence for reverse translation, so that e.g. wider becomes narrower. */
    @Nonnull
    private static ConceptMapEquivalence invert(@Nonnull final ConceptMapEquivalence equivalence) {
      return switch (equivalence) {
        case WIDER -> ConceptMapEquivalence.NARROWER;
        case NARROWER -> ConceptMapEquivalence.WIDER;
        case SUBSUMES -> ConceptMapEquivalence.SPECIALIZES;
        case SPECIALIZES -> ConceptMapEquivalence.SUBSUMES;
        default -> equivalence;
      };
    }
  }

  /** One end of a mapping: a system, code, and the equivalence relationship. */
  private static final class MapTarget {
    @Nullable final String system;
    @Nonnull final String code;
    @Nonnull final ConceptMapEquivalence equivalence;

    MapTarget(
        @Nullable final String system,
        @Nonnull final String code,
        @Nonnull final ConceptMapEquivalence equivalence) {
      this.system = system;
      this.code = code;
      this.equivalence = equivalence;
    }
  }
}
