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

import au.csiro.pathling.test.Rf2Mini;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

/**
 * Builds temporary copies of the {@code rf2-mini} base release whose association reference set file
 * gains rows, so that a test can exercise associations the shared fixture does not hold without
 * changing it.
 *
 * @author John Grimes
 */
final class Rf2MiniReleaseCopy {

  private Rf2MiniReleaseCopy() {
    // Static helper.
  }

  /**
   * Copies the base release into a directory and appends rows to its association reference set
   * file.
   *
   * @param release the directory to copy the release into, which must not yet exist
   * @param associationRows the data rows to append, as built by {@link #associationRow}
   * @return the directory of the copy
   */
  @Nonnull
  static Path withAssociationRows(
      @Nonnull final Path release, @Nonnull final List<String> associationRows) {
    copyOfBaseRelease(release);
    appendAssociationRows(release, associationRows);
    return release;
  }

  /**
   * Builds an active association reference set row of the base release.
   *
   * @param id the member identifier, unique within the release
   * @param refset the association reference set
   * @param referenced the referenced concept
   * @param target the association target
   * @return the tab-separated row
   */
  @Nonnull
  static String associationRow(
      @Nonnull final String id,
      @Nonnull final String refset,
      @Nonnull final String referenced,
      @Nonnull final String target) {
    return String.join("\t", id, "20230601", "1", Rf2Mini.CORE_MODULE, refset, referenced, target);
  }

  /** Copies the base release into a directory. */
  private static void copyOfBaseRelease(@Nonnull final Path release) {
    try (final Stream<Path> paths = Files.walk(Rf2Mini.baseRelease())) {
      for (final Path source : paths.sorted().toList()) {
        final Path target = release.resolve(Rf2Mini.baseRelease().relativize(source).toString());
        if (Files.isDirectory(source)) {
          Files.createDirectories(target);
        } else {
          Files.createDirectories(target.getParent());
          Files.copy(source, target);
        }
      }
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Appends data rows to the release's association reference set file. */
  private static void appendAssociationRows(
      @Nonnull final Path release, @Nonnull final List<String> rows) {
    try (final Stream<Path> paths = Files.walk(release)) {
      final Path file =
          paths
              .filter(path -> path.getFileName().toString().startsWith("der2_cRefset_Association"))
              .min(Comparator.naturalOrder())
              .orElseThrow(() -> new IllegalStateException("No association reference set file"));
      final List<String> lines = new ArrayList<>(Files.readAllLines(file));
      lines.addAll(rows);
      Files.write(file, lines);
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
