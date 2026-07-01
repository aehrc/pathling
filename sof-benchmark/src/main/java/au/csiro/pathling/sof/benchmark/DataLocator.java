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

import jakarta.annotation.Nonnull;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Resolves a benchmark's materialized NDJSON directory deterministically from the dataset's
 * explicit {@code (name, version)} identity.
 *
 * <p>Under contract v2 the materializer writes data to {@code <dataRoot>/<name>/<version>/<size>/},
 * so the runner resolves the directory directly from identity rather than scanning {@code
 * manifest.json} files or reproducing a recipe hash.
 */
public final class DataLocator {

  private DataLocator() {}

  /**
   * Resolves the size-specific data directory for a dataset identity.
   *
   * @param dataRoot the root directory containing materialized datasets
   * @param name the dataset name (first directory segment)
   * @param version the dataset version (second directory segment)
   * @param size the size key (third directory segment)
   * @return the directory containing the size's NDJSON files
   * @throws IllegalStateException if the resolved directory does not exist
   */
  @Nonnull
  public static Path locate(
      @Nonnull final Path dataRoot,
      @Nonnull final String name,
      @Nonnull final String version,
      @Nonnull final String size) {
    final Path dataDir = dataRoot.resolve(name).resolve(version).resolve(size);
    if (!Files.isDirectory(dataDir)) {
      throw new IllegalStateException(
          "No materialized data at "
              + dataDir
              + " for (name="
              + name
              + ", version="
              + version
              + ", size="
              + size
              + "). Run the materializer first: cd sql-on-fhir/benchmark && bun run data <file>"
              + " --size "
              + size);
    }
    return dataDir;
  }
}
