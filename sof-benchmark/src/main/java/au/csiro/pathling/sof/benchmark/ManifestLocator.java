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
import java.util.stream.Stream;

/**
 * Locates a benchmark's materialized NDJSON by matching {@code manifest.json} files, rather than by
 * recomputing the reference materializer's recipe hash.
 *
 * <p>The materializer writes one directory per recipe under {@code <dataRoot>/<name>_<hash>/<size>}
 * containing the resource NDJSON files and a {@code manifest.json} recording the dataset {@code
 * name}, {@code size} and {@code population}. This locator scans those manifests and selects the
 * directory whose manifest matches all three, so the JS canonicalization that produces {@code
 * <hash>} is never reproduced.
 */
public final class ManifestLocator {

  @Nonnull private static final ObjectMapper MAPPER = new ObjectMapper();

  private ManifestLocator() {}

  /**
   * Locates the size-specific data directory whose manifest matches the requested dataset name,
   * size and population.
   *
   * @param dataRoot the root directory containing materialized datasets
   * @param name the dataset name to match
   * @param size the size key to match
   * @param population the population to match
   * @return the directory containing the matching size's NDJSON files and manifest
   * @throws IllegalStateException if no manifest under the data root matches all three
   */
  @Nonnull
  public static Path locate(
      @Nonnull final Path dataRoot,
      @Nonnull final String name,
      @Nonnull final String size,
      final int population) {
    if (!Files.isDirectory(dataRoot)) {
      throw new IllegalStateException(
          "Data root does not exist or is not a directory: " + dataRoot);
    }

    try (final Stream<Path> datasetDirs = Files.list(dataRoot)) {
      return datasetDirs
          .map(datasetDir -> datasetDir.resolve(size).resolve("manifest.json"))
          .filter(Files::isRegularFile)
          .filter(manifest -> manifestMatches(manifest, name, size, population))
          .map(Path::getParent)
          .findFirst()
          .orElseThrow(
              () ->
                  new IllegalStateException(
                      "No materialized dataset matches (name="
                          + name
                          + ", size="
                          + size
                          + ", population="
                          + population
                          + ") under "
                          + dataRoot
                          + ". Run the materializer first: cd sql-on-fhir/benchmark && bun run"
                          + " data <file> --size "
                          + size));
    } catch (final IOException e) {
      throw new UncheckedIOException("Failed to scan data root: " + dataRoot, e);
    }
  }

  private static boolean manifestMatches(
      @Nonnull final Path manifest,
      @Nonnull final String name,
      @Nonnull final String size,
      final int population) {
    try {
      final JsonNode node = MAPPER.readTree(Files.readAllBytes(manifest));
      return name.equals(node.path("name").asText())
          && size.equals(node.path("size").asText())
          && population == node.path("population").asInt(-1);
    } catch (final IOException e) {
      // A manifest we cannot read is simply not a match.
      return false;
    }
  }
}
