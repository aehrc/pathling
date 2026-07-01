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
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

/**
 * Verifies materialized NDJSON files against a checkfile's per-file sha256 lock.
 *
 * <p>This is a dataset-integrity guard, run outside any timed region, that confirms the runner is
 * benchmarking byte-identical data to what the checkfile locked. It reports drift (a mismatching or
 * missing file) rather than silently benchmarking stale or wrong data.
 */
public final class ChecksumVerifier {

  @Nonnull private static final HexFormat HEX = HexFormat.of();

  private ChecksumVerifier() {}

  /**
   * Verifies each locked file's sha256 against the corresponding NDJSON in the data directory.
   *
   * @param dataDir the directory containing the materialized {@code <ResourceType>.ndjson} files
   * @param lockedChecksums the checkfile's file name to sha256 map for the size
   * @return the list of human-readable drift messages; empty when every locked file matches
   * @throws UncheckedIOException if a present file cannot be read
   */
  @Nonnull
  public static List<String> verify(
      @Nonnull final Path dataDir, @Nonnull final Map<String, String> lockedChecksums) {
    final List<String> drift = new ArrayList<>();
    for (final Map.Entry<String, String> entry : lockedChecksums.entrySet()) {
      final String fileName = entry.getKey();
      final String locked = entry.getValue();
      final Path file = dataDir.resolve(fileName);
      if (!Files.isRegularFile(file)) {
        drift.add(fileName + ": missing (locked " + locked + ")");
        continue;
      }
      final String actual = sha256(file);
      if (!actual.equals(locked)) {
        drift.add(fileName + ": sha256 drift (locked " + locked + ", actual " + actual + ")");
      }
    }
    return drift;
  }

  /**
   * Computes the sha256 of a file as a lower-case hex string.
   *
   * @param file the file to hash
   * @return the lower-case hex sha256
   * @throws UncheckedIOException if the file cannot be read
   */
  @Nonnull
  public static String sha256(@Nonnull final Path file) {
    try {
      final MessageDigest digest = MessageDigest.getInstance("SHA-256");
      return HEX.formatHex(digest.digest(Files.readAllBytes(file)));
    } catch (final IOException e) {
      throw new UncheckedIOException("Failed to read file for checksum: " + file, e);
    } catch (final NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is not available", e);
    }
  }
}
