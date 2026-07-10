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

import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_ACTIVE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_CODE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_DEFINED;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_DENSE_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_DISPLAY;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_EFFECTIVE_TIME;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_MODULE_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_SYSTEM_VERSION_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.CONCEPT;

import au.csiro.pathling.terminology.store.TerminologyStoreReader;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.roaringbitmap.RoaringBitmap;

/**
 * The concept dictionary for one code system version: the mapping between concept codes and the
 * dense integer identifiers that address the runtime bitmaps, plus each concept's status, display,
 * and defining metadata. It is loaded first and gates every other index.
 *
 * @author John Grimes
 */
public final class ConceptDictionary {

  @Nonnull private final Map<String, Integer> codeToDense;
  @Nonnull private final String[] codes;
  @Nonnull private final String[] displays;
  @Nonnull private final boolean[] active;
  @Nonnull private final boolean[] defined;
  @Nonnull private final String[] moduleIds;
  @Nonnull private final String[] effectiveTimes;
  @Nonnull private final RoaringBitmap activeConcepts;

  private ConceptDictionary(
      @Nonnull final Map<String, Integer> codeToDense,
      @Nonnull final String[] codes,
      @Nonnull final String[] displays,
      @Nonnull final boolean[] active,
      @Nonnull final boolean[] defined,
      @Nonnull final String[] moduleIds,
      @Nonnull final String[] effectiveTimes,
      @Nonnull final RoaringBitmap activeConcepts) {
    this.codeToDense = codeToDense;
    this.codes = codes;
    this.displays = displays;
    this.active = active;
    this.defined = defined;
    this.moduleIds = moduleIds;
    this.effectiveTimes = effectiveTimes;
    this.activeConcepts = activeConcepts;
  }

  /** A concept row collected during loading, before the dense arrays are sized. */
  private record Entry(
      int dense,
      @Nonnull String code,
      @Nullable String display,
      boolean active,
      boolean defined,
      @Nullable String moduleId,
      @Nullable String effectiveTime) {}

  /**
   * Loads the concept dictionary for a code system version.
   *
   * @param reader the store reader
   * @param systemVersionId the code system version to load
   * @return the loaded dictionary
   */
  @Nonnull
  public static ConceptDictionary load(
      @Nonnull final TerminologyStoreReader reader, @Nonnull final String systemVersionId) {
    final List<Entry> entries = new ArrayList<>();
    reader.readTable(
        CONCEPT,
        row -> {
          if (!systemVersionId.equals(row.getString(COLUMN_SYSTEM_VERSION_ID))) {
            return;
          }
          entries.add(
              new Entry(
                  row.getInt(COLUMN_DENSE_ID),
                  row.getString(COLUMN_CODE),
                  row.getString(COLUMN_DISPLAY),
                  row.getBoolean(COLUMN_ACTIVE),
                  row.getBoolean(COLUMN_DEFINED),
                  row.getString(COLUMN_MODULE_ID),
                  row.getString(COLUMN_EFFECTIVE_TIME)));
        });
    int maxDense = -1;
    for (final Entry entry : entries) {
      maxDense = Math.max(maxDense, entry.dense());
    }

    final int size = maxDense + 1;
    final Map<String, Integer> codeToDense = new HashMap<>(size * 2);
    final String[] codes = new String[size];
    final String[] displays = new String[size];
    final boolean[] active = new boolean[size];
    final boolean[] defined = new boolean[size];
    final String[] moduleIds = new String[size];
    final String[] effectiveTimes = new String[size];
    final RoaringBitmap activeConcepts = new RoaringBitmap();
    for (final Entry entry : entries) {
      final int dense = entry.dense();
      codeToDense.put(entry.code(), dense);
      codes[dense] = entry.code();
      displays[dense] = entry.display();
      active[dense] = entry.active();
      defined[dense] = entry.defined();
      moduleIds[dense] = entry.moduleId();
      effectiveTimes[dense] = entry.effectiveTime();
      if (entry.active()) {
        activeConcepts.add(dense);
      }
    }
    return new ConceptDictionary(
        codeToDense, codes, displays, active, defined, moduleIds, effectiveTimes, activeConcepts);
  }

  /**
   * Returns the dense identifier of a code, or null if the code is not present.
   *
   * @param code the concept code
   * @return the dense identifier, or null
   */
  @Nullable
  public Integer denseId(@Nonnull final String code) {
    return codeToDense.get(code);
  }

  /**
   * Returns the code for a dense identifier.
   *
   * @param dense the dense identifier
   * @return the concept code
   */
  @Nonnull
  public String code(final int dense) {
    return codes[dense];
  }

  /**
   * Returns the default display term for a dense identifier.
   *
   * @param dense the dense identifier
   * @return the display term
   */
  @Nullable
  public String display(final int dense) {
    return displays[dense];
  }

  /**
   * Returns whether a concept is active.
   *
   * @param dense the dense identifier
   * @return true if the concept is active
   */
  public boolean isActive(final int dense) {
    return active[dense];
  }

  /**
   * Returns whether a concept is sufficiently defined.
   *
   * @param dense the dense identifier
   * @return true if the concept is sufficiently defined
   */
  public boolean isDefined(final int dense) {
    return defined[dense];
  }

  /**
   * Returns the module identifier of a concept.
   *
   * @param dense the dense identifier
   * @return the module identifier, or null
   */
  @Nullable
  public String moduleId(final int dense) {
    return moduleIds[dense];
  }

  /**
   * Returns the effectiveTime of a concept.
   *
   * @param dense the dense identifier
   * @return the effectiveTime, or null
   */
  @Nullable
  public String effectiveTime(final int dense) {
    return effectiveTimes[dense];
  }

  /**
   * Returns a copy of the bitmap of all active concepts in this version.
   *
   * @return the active concept universe
   */
  @Nonnull
  public RoaringBitmap activeConcepts() {
    return activeConcepts.clone();
  }

  /**
   * Returns a copy of the bitmap of every concept in this version, active or not.
   *
   * @return all concepts
   */
  @Nonnull
  public RoaringBitmap allConcepts() {
    final RoaringBitmap all = new RoaringBitmap();
    all.add(0L, (long) codes.length);
    return all;
  }

  /**
   * Returns the dense identifiers of the concepts satisfying a predicate.
   *
   * @param predicate a test applied to each concept's dense identifier
   * @return the matching concepts
   */
  @Nonnull
  public RoaringBitmap conceptsWhere(@Nonnull final java.util.function.IntPredicate predicate) {
    final RoaringBitmap result = new RoaringBitmap();
    for (int dense = 0; dense < codes.length; dense++) {
      if (predicate.test(dense)) {
        result.add(dense);
      }
    }
    return result;
  }

  /**
   * Returns the number of concepts in this version.
   *
   * @return the concept count
   */
  public int size() {
    return codes.length;
  }
}
