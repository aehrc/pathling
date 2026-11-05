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

import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_CONCEPT_MAP_ID;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_EQUIVALENCE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_ORDINAL;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_SOURCE_CODE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_SOURCE_SYSTEM;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_TARGET_CODE;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.COLUMN_TARGET_SYSTEM;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.CONCEPT_MAPPING;
import static au.csiro.pathling.terminology.store.TerminologyStoreSchema.ENTRY_TYPE_CONCEPT_MAP;

import au.csiro.pathling.terminology.TerminologyService.Translation;
import au.csiro.pathling.terminology.local.VersionResolver;
import au.csiro.pathling.terminology.store.ManifestEntry;
import au.csiro.pathling.terminology.store.TerminologyStoreException;
import au.csiro.pathling.terminology.store.TerminologyStoreReader;
import au.csiro.pathling.terminology.store.TerminologyStoreRow;
import au.csiro.pathling.terminology.store.TerminologyStoreSchema;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.codesystems.ConceptMapEquivalence;

/**
 * The concept map index: the mappings of the imported FHIR ConceptMaps, keyed by canonical URL and
 * addressable in both directions. It backs local {@code translate} for explicit concept maps.
 *
 * <p>Only the catalogue of maps is read up front, from the store manifest. The mappings of a map
 * are read the first time it is translated through, and only those of the version that answers: the
 * latest, where the store holds several. They are then held for the life of the index in a compact
 * form, with each distinct code stored once as UTF-8 bytes and each mapping as a handful of
 * integers, so that a map of millions of mappings fits in a modest heap, both while it is read and
 * afterwards.
 *
 * @author John Grimes
 */
public final class ConceptMapIndex {

  private static final ConceptMapEquivalence[] EQUIVALENCES = ConceptMapEquivalence.values();

  @Nonnull private final TerminologyStoreReader reader;
  @Nonnull private final VersionResolver versionResolver;
  @Nonnull private final Map<String, List<String>> versionsByUrl;
  @Nonnull private final Map<String, Mappings> loaded = new ConcurrentHashMap<>();

  private ConceptMapIndex(
      @Nonnull final TerminologyStoreReader reader,
      @Nonnull final VersionResolver versionResolver,
      @Nonnull final Map<String, List<String>> versionsByUrl) {
    this.reader = reader;
    this.versionResolver = versionResolver;
    this.versionsByUrl = versionsByUrl;
  }

  /**
   * Opens the concept map index over a store, reading the catalogue of imported maps from the
   * manifest. No mappings are read until a map is first translated through.
   *
   * @param reader the store reader, retained to read the mappings of each map on first use
   * @param versionResolver the resolver used to select the latest version of a map
   * @return the index
   */
  @Nonnull
  public static ConceptMapIndex load(
      @Nonnull final TerminologyStoreReader reader,
      @Nonnull final VersionResolver versionResolver) {
    final Map<String, List<String>> versionsByUrl = new HashMap<>();
    for (final ManifestEntry entry : reader.readManifest()) {
      if (ENTRY_TYPE_CONCEPT_MAP.equals(entry.getEntryType())) {
        versionsByUrl
            .computeIfAbsent(entry.getCanonicalUrl(), url -> new ArrayList<>())
            .add(entry.getVersion());
      }
    }
    return new ConceptMapIndex(reader, versionResolver, versionsByUrl);
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
   * @return the translations in the order the map lists them, empty if the map or the code is
   *     unknown
   * @throws au.csiro.pathling.terminology.local.AmbiguousVersionException if the store holds
   *     several versions of the map and none of them is clearly the latest
   */
  @Nonnull
  public List<Translation> translate(
      @Nonnull final String conceptMapUrl,
      @Nonnull final String system,
      @Nonnull final String code,
      final boolean reverse) {
    final List<String> versions = versionsByUrl.get(conceptMapUrl);
    if (versions == null) {
      return List.of();
    }
    // The versions in the store are fixed for the life of the index, so the version that answers
    // is resolved once, when the map is first loaded.
    final Mappings mappings =
        loaded.computeIfAbsent(
            conceptMapUrl,
            url ->
                Mappings.load(
                    reader,
                    TerminologyStoreSchema.conceptMapId(
                        url,
                        versionResolver.getLatestOfVersions(versions, Function.identity(), url)),
                    url));
    return mappings.translate(system, code, reverse);
  }

  /**
   * The mappings of one ConceptMap version, held as parallel arrays indexed by each mapping's
   * position in the map. Codes are interned, each mapping refers to its group for its systems, and
   * each direction is the order of the mappings sorted by code, so a lookup is a binary search that
   * yields the code's mappings in document order.
   */
  private static final class Mappings {

    /** The marker for a target that carries no code, which no reverse lookup can match. */
    private static final int NO_CODE = -1;

    @Nonnull private final CodeTable codes;
    @Nonnull private final String[] groupSource;
    @Nonnull private final String[] groupTarget;
    @Nonnull private final int[] group;
    @Nonnull private final int[] sourceCode;
    @Nonnull private final int[] targetCode;
    @Nonnull private final byte[] equivalence;
    @Nonnull private final int[] forward;
    @Nonnull private final int[] reverse;

    /**
     * Takes over the builder's arrays one at a time, trimming each and releasing the builder's
     * reference to it, so that the peak memory of the hand-over is one array rather than all of
     * them.
     */
    private Mappings(@Nonnull final Builder builder) {
      final int count = builder.count;
      codes = builder.codes.trim();
      groupSource = builder.groups.stream().map(systems -> systems.get(0)).toArray(String[]::new);
      groupTarget = builder.groups.stream().map(systems -> systems.get(1)).toArray(String[]::new);
      group = trimmed(builder.group, count);
      builder.group = null;
      sourceCode = trimmed(builder.sourceCode, count);
      builder.sourceCode = null;
      targetCode = trimmed(builder.targetCode, count);
      builder.targetCode = null;
      equivalence =
          builder.equivalence.length == count
              ? builder.equivalence
              : Arrays.copyOf(builder.equivalence, count);
      builder.equivalence = null;
      forward = orderByCode(sourceCode, codes.size());
      reverse = orderByCode(targetCode, codes.size());
    }

    /** Reads the mappings of one ConceptMap version from the store. */
    @Nonnull
    static Mappings load(
        @Nonnull final TerminologyStoreReader reader,
        @Nonnull final String conceptMapId,
        @Nonnull final String url) {
      final Builder builder = new Builder();
      reader.readPartitionIfPresent(
          CONCEPT_MAPPING, COLUMN_CONCEPT_MAP_ID, conceptMapId, builder::add);
      if (builder.count != builder.seen) {
        throw new TerminologyStoreException(
            "The stored mappings of ConceptMap "
                + url
                + " are incomplete; re-import the ConceptMap to repair the store.");
      }
      return new Mappings(builder);
    }

    @Nonnull
    List<Translation> translate(
        @Nonnull final String system, @Nonnull final String code, final boolean reversed) {
      final int codeIndex = codes.find(code);
      if (codeIndex == NO_CODE) {
        return List.of();
      }
      final int[] order = reversed ? reverse : forward;
      final int[] fromCode = reversed ? targetCode : sourceCode;
      final String[] fromSystems = reversed ? groupTarget : groupSource;
      final String[] toSystems = reversed ? groupSource : groupTarget;
      final int[] toCode = reversed ? sourceCode : targetCode;
      final List<Translation> result = new ArrayList<>();
      for (int i = lowerBound(order, fromCode, codeIndex);
          i < order.length && fromCode[order[i]] == codeIndex;
          i++) {
        final int mapping = order[i];
        // A group that names no system is keyed by the empty system, as it always has been.
        if (!system.equals(Objects.requireNonNullElse(fromSystems[group[mapping]], ""))) {
          continue;
        }
        final ConceptMapEquivalence mapped = EQUIVALENCES[equivalence[mapping]];
        result.add(
            Translation.of(
                reversed ? invert(mapped) : mapped,
                new Coding()
                    .setSystem(toSystems[group[mapping]])
                    .setCode(toCode[mapping] == NO_CODE ? null : codes.get(toCode[mapping]))));
      }
      return result;
    }

    @Nonnull
    private static int[] trimmed(@Nonnull final int[] array, final int length) {
      return array.length == length ? array : Arrays.copyOf(array, length);
    }

    /**
     * Returns the positions of the mappings that have a code, sorted by code and then by position,
     * so the mappings of one code appear in document order. A counting sort over the dense code
     * numbers does this in linear time, holding nothing larger than a count per code alongside.
     */
    @Nonnull
    private static int[] orderByCode(@Nonnull final int[] codeIndexes, final int codeCount) {
      final int[] next = new int[codeCount + 1];
      for (final int code : codeIndexes) {
        if (code != NO_CODE) {
          next[code + 1]++;
        }
      }
      for (int code = 0; code < codeCount; code++) {
        next[code + 1] += next[code];
      }
      final int[] order = new int[next[codeCount]];
      for (int mapping = 0; mapping < codeIndexes.length; mapping++) {
        final int code = codeIndexes[mapping];
        if (code != NO_CODE) {
          order[next[code]++] = mapping;
        }
      }
      return order;
    }

    /**
     * Returns the index in {@code order} of the first mapping whose code is not less than {@code
     * code}.
     */
    private static int lowerBound(
        @Nonnull final int[] order, @Nonnull final int[] codeIndexes, final int code) {
      int low = 0;
      int high = order.length;
      while (low < high) {
        final int mid = (low + high) >>> 1;
        if (codeIndexes[order[mid]] < code) {
          low = mid + 1;
        } else {
          high = mid;
        }
      }
      return low;
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

  /**
   * Accumulates the rows of one ConceptMap version, which may arrive in any order, at the positions
   * their ordinals give them.
   */
  private static final class Builder {

    private final CodeTable codes = new CodeTable();

    /** The distinct pairs of source and target system, of which a map has few. */
    private final List<List<String>> groups = new ArrayList<>();

    private final Map<List<String>, Integer> groupIndexes = new HashMap<>();
    private int[] group = new int[16];
    private int[] sourceCode = new int[16];
    private int[] targetCode = new int[16];
    private byte[] equivalence = new byte[16];

    /** One more than the greatest ordinal seen. */
    private int count;

    /** The number of rows seen, which equals {@link #count} when no ordinal is missing. */
    private int seen;

    void add(@Nonnull final TerminologyStoreRow row) {
      final int mapping = row.getInt(COLUMN_ORDINAL);
      ensureCapacity(mapping + 1);
      group[mapping] =
          group(row.getString(COLUMN_SOURCE_SYSTEM), row.getString(COLUMN_TARGET_SYSTEM));
      sourceCode[mapping] = codes.intern(row.getString(COLUMN_SOURCE_CODE));
      final String target = row.getString(COLUMN_TARGET_CODE);
      targetCode[mapping] = target == null ? Mappings.NO_CODE : codes.intern(target);
      equivalence[mapping] =
          (byte) ConceptMapEquivalence.fromCode(row.getString(COLUMN_EQUIVALENCE)).ordinal();
      count = Math.max(count, mapping + 1);
      seen++;
    }

    /** Interns a pair of systems, either of which is null where the group names none. */
    private int group(@Nullable final String sourceSystem, @Nullable final String targetSystem) {
      return groupIndexes.computeIfAbsent(
          Arrays.asList(sourceSystem, targetSystem),
          systems -> {
            groups.add(systems);
            return groups.size() - 1;
          });
    }

    private void ensureCapacity(final int capacity) {
      if (capacity <= sourceCode.length) {
        return;
      }
      // Growing by half rather than doubling keeps the spare capacity, which is only released
      // once the whole map has been read, to a third of the arrays at most.
      final int grown = Math.max(capacity, sourceCode.length + (sourceCode.length >> 1));
      group = Arrays.copyOf(group, grown);
      sourceCode = Arrays.copyOf(sourceCode, grown);
      targetCode = Arrays.copyOf(targetCode, grown);
      equivalence = Arrays.copyOf(equivalence, grown);
    }
  }

  /**
   * The distinct codes of a map, numbered in the order they were first seen and stored back to back
   * as UTF-8 bytes with an offset per code, and found through an open-addressing hash table of code
   * numbers. A code therefore costs its own length and about a dozen bytes, rather than a string
   * object and a hash map entry, both while the map is read and afterwards.
   */
  private static final class CodeTable {

    private static final int EMPTY = -1;

    private byte[] bytes = new byte[256];
    private int[] offsets = new int[17];
    private int size;
    private int[] slots = emptySlots(32);

    /** Returns the number of a code, numbering it if it has not been seen before. */
    int intern(@Nonnull final String code) {
      final byte[] key = code.getBytes(StandardCharsets.UTF_8);
      final int slot = slotOf(key);
      if (slots[slot] != EMPTY) {
        return slots[slot];
      }
      append(key);
      slots[slot] = size - 1;
      // Rehash at three quarters full, which keeps linear probing short.
      if (size * 4L > slots.length * 3L) {
        rehash(slots.length * 2);
      }
      return size - 1;
    }

    /** Returns the number of a code, or {@link Mappings#NO_CODE} if the map has no such code. */
    int find(@Nonnull final String code) {
      final int number = slots[slotOf(code.getBytes(StandardCharsets.UTF_8))];
      return number == EMPTY ? Mappings.NO_CODE : number;
    }

    /** Returns the number of distinct codes. */
    int size() {
      return size;
    }

    @Nonnull
    String get(final int number) {
      return new String(
          bytes, offsets[number], offsets[number + 1] - offsets[number], StandardCharsets.UTF_8);
    }

    /** Releases the spare capacity left by growth, once every code has been interned. */
    @Nonnull
    CodeTable trim() {
      bytes = Arrays.copyOf(bytes, offsets[size]);
      offsets = Arrays.copyOf(offsets, size + 1);
      return this;
    }

    /** Returns the slot holding a code, or the empty slot where it would be placed. */
    private int slotOf(@Nonnull final byte[] key) {
      final int mask = slots.length - 1;
      int slot = hash(key, 0, key.length) & mask;
      while (slots[slot] != EMPTY) {
        final int number = slots[slot];
        if (Arrays.equals(bytes, offsets[number], offsets[number + 1], key, 0, key.length)) {
          return slot;
        }
        slot = (slot + 1) & mask;
      }
      return slot;
    }

    private void append(@Nonnull final byte[] key) {
      final int end = offsets[size] + key.length;
      if (end > bytes.length) {
        bytes = Arrays.copyOf(bytes, Math.max(end, bytes.length + (bytes.length >> 1)));
      }
      System.arraycopy(key, 0, bytes, offsets[size], key.length);
      if (size + 2 > offsets.length) {
        offsets = Arrays.copyOf(offsets, offsets.length + (offsets.length >> 1));
      }
      offsets[++size] = end;
    }

    private void rehash(final int capacity) {
      slots = emptySlots(capacity);
      final int mask = capacity - 1;
      for (int number = 0; number < size; number++) {
        int slot = hash(bytes, offsets[number], offsets[number + 1]) & mask;
        while (slots[slot] != EMPTY) {
          slot = (slot + 1) & mask;
        }
        slots[slot] = number;
      }
    }

    private static int hash(@Nonnull final byte[] data, final int from, final int to) {
      int hash = 1;
      for (int i = from; i < to; i++) {
        hash = 31 * hash + data[i];
      }
      // Codes are often sequential, which the polynomial hash maps to runs of adjacent slots that
      // defeat linear probing, so the bits are mixed as in the MurmurHash3 finaliser.
      hash ^= hash >>> 16;
      hash *= 0x85ebca6b;
      hash ^= hash >>> 13;
      hash *= 0xc2b2ae35;
      return hash ^ (hash >>> 16);
    }

    @Nonnull
    private static int[] emptySlots(final int capacity) {
      final int[] empty = new int[capacity];
      Arrays.fill(empty, EMPTY);
      return empty;
    }
  }
}
