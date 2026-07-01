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

package au.csiro.pathling.sof.benchmark;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * The committed build-time lock artifact that sits beside a benchmark file (see {@code
 * benchmark-checkfile.schema.json}).
 *
 * <p>Under contract v2 the checkfile is the home for everything generated about a benchmark's
 * dataset and expected results: per-size resource counts, per-file sha256 checksums, and the
 * per-case per-size result assertions that used to live inline on each case. The runner sources its
 * correctness expectations from {@code assertions[caseId][size]}, its dataset-integrity checks from
 * {@code sizes[size].files[file].sha256}, and the report's {@code resourceCounts} from {@code
 * sizes[size].resourceCounts}.
 *
 * <p>The checkfile is resolved by swapping the benchmark file's extension: {@code <base>.json ->
 * <base>.check.json}. When it is absent, {@link #parseOrEmpty(Path)} returns an empty checkfile
 * that yields no expectations, counts or checksums, and reports {@link #isPresent()} as false.
 */
public final class Checkfile {

  @Nonnull private static final ObjectMapper MAPPER = new ObjectMapper();

  private final boolean present;

  /** Case id to size to expected output row count. */
  @Nonnull private final Map<String, Map<String, Integer>> assertions;

  /** Size to resource type to NDJSON row count. */
  @Nonnull private final Map<String, Map<String, Integer>> resourceCounts;

  /** Size to file name to sha256. */
  @Nonnull private final Map<String, Map<String, String>> fileChecksums;

  private Checkfile(
      final boolean present,
      @Nonnull final Map<String, Map<String, Integer>> assertions,
      @Nonnull final Map<String, Map<String, Integer>> resourceCounts,
      @Nonnull final Map<String, Map<String, String>> fileChecksums) {
    this.present = present;
    this.assertions = assertions;
    this.resourceCounts = resourceCounts;
    this.fileChecksums = fileChecksums;
  }

  /**
   * Resolves the checkfile path that sits beside a benchmark file by swapping {@code .json} for
   * {@code .check.json}.
   *
   * @param benchmarkFile the path to the benchmark {@code *.json} file
   * @return the sibling {@code *.check.json} path
   */
  @Nonnull
  public static Path siblingOf(@Nonnull final Path benchmarkFile) {
    final String fileName = benchmarkFile.getFileName().toString();
    final String base =
        fileName.endsWith(".json")
            ? fileName.substring(0, fileName.length() - ".json".length())
            : fileName;
    final Path parent = benchmarkFile.toAbsolutePath().getParent();
    final String checkfileName = base + ".check.json";
    return parent == null ? Path.of(checkfileName) : parent.resolve(checkfileName);
  }

  /**
   * Parses the checkfile beside a benchmark file, or returns an empty checkfile when none exists.
   *
   * @param benchmarkFile the path to the benchmark {@code *.json} file
   * @return the parsed checkfile, or an empty checkfile when the sibling is absent
   * @throws UncheckedIOException if the checkfile exists but cannot be read or parsed
   */
  @Nonnull
  public static Checkfile parseOrEmpty(@Nonnull final Path benchmarkFile) {
    final Path checkfile = siblingOf(benchmarkFile);
    if (!Files.isRegularFile(checkfile)) {
      return new Checkfile(false, Map.of(), Map.of(), Map.of());
    }
    final JsonNode root;
    try {
      root = MAPPER.readTree(Files.readAllBytes(checkfile));
    } catch (final IOException e) {
      throw new UncheckedIOException("Failed to read checkfile: " + checkfile, e);
    }
    return new Checkfile(
        true,
        parseNested(root.path("assertions"), Function.identity(), JsonNode::asInt),
        parseNested(root.path("sizes"), size -> size.path("resourceCounts"), JsonNode::asInt),
        parseNested(
            root.path("sizes"), size -> size.path("files"), file -> file.path("sha256").asText()));
  }

  /**
   * Parses a two-level JSON object into a nested map, selecting the inner object from each outer
   * value and extracting each inner value.
   *
   * @param outer the outer JSON object (its field names become the outer map keys)
   * @param innerSelector selects the inner JSON object to iterate from each outer value
   * @param valueExtractor extracts the typed value from each inner JSON value
   * @param <V> the inner value type
   * @return the nested map, preserving field order
   */
  @Nonnull
  private static <V> Map<String, Map<String, V>> parseNested(
      @Nonnull final JsonNode outer,
      @Nonnull final Function<JsonNode, JsonNode> innerSelector,
      @Nonnull final Function<JsonNode, V> valueExtractor) {
    final Map<String, Map<String, V>> result = new LinkedHashMap<>();
    final Iterator<Map.Entry<String, JsonNode>> outerFields = outer.fields();
    while (outerFields.hasNext()) {
      final Map.Entry<String, JsonNode> outerEntry = outerFields.next();
      final Map<String, V> inner = new LinkedHashMap<>();
      final Iterator<Map.Entry<String, JsonNode>> innerFields =
          innerSelector.apply(outerEntry.getValue()).fields();
      while (innerFields.hasNext()) {
        final Map.Entry<String, JsonNode> innerEntry = innerFields.next();
        inner.put(innerEntry.getKey(), valueExtractor.apply(innerEntry.getValue()));
      }
      result.put(outerEntry.getKey(), inner);
    }
    return result;
  }

  /**
   * Returns whether a checkfile was present beside the benchmark file.
   *
   * @return true when the checkfile exists
   */
  public boolean isPresent() {
    return present;
  }

  /**
   * Returns the expected output row count for a case at a size, if the checkfile asserts one.
   *
   * @param caseId the stable case id
   * @param size the size key
   * @return the expected output row count, or empty when no assertion is declared
   */
  @Nonnull
  public Optional<Integer> expectedCount(@Nonnull final String caseId, @Nonnull final String size) {
    return Optional.ofNullable(assertions.get(caseId)).map(bySize -> bySize.get(size));
  }

  /**
   * Returns the resource type to NDJSON row count map locked for a size.
   *
   * @param size the size key
   * @return the resource counts, or an empty map when the size is not locked
   */
  @Nonnull
  public Map<String, Integer> resourceCounts(@Nonnull final String size) {
    return Collections.unmodifiableMap(resourceCounts.getOrDefault(size, Map.of()));
  }

  /**
   * Returns the file name to sha256 map locked for a size.
   *
   * @param size the size key
   * @return the per-file checksums, or an empty map when the size is not locked
   */
  @Nonnull
  public Map<String, String> fileChecksums(@Nonnull final String size) {
    return Collections.unmodifiableMap(fileChecksums.getOrDefault(size, Map.of()));
  }
}
