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

package au.csiro.pathling.util;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

/**
 * Teardown helper for integration tests whose working directories are also deleted asynchronously
 * by the server. Cancelling a job causes the server to delete that job's directory in the
 * background, so a teardown running {@code FileUtils.cleanDirectory} over the enclosing jobs
 * directory can find entries vanishing underneath it and fail with {@link NoSuchFileException} (see
 * issue #2711). This helper treats a concurrently deleted entry as already cleaned up.
 *
 * @author John Grimes
 */
public final class DirectoryCleanup {

  private DirectoryCleanup() {}

  /**
   * Deletes the contents of a directory, preserving the directory itself. Entries that disappear
   * while the cleanup is running, or a directory that does not exist at all, are treated as already
   * cleaned up rather than as failures.
   *
   * @param directory the directory whose contents should be removed
   * @throws IOException if an entry could not be deleted for a reason other than it disappearing
   */
  public static void cleanDirectoryTolerantly(@Nonnull final Path directory) throws IOException {
    for (final Path entry : listChildrenTolerantly(directory)) {
      deleteRecursivelyTolerantly(entry);
    }
  }

  /**
   * Lists the children of a directory, returning an empty list if the directory has disappeared.
   *
   * @param directory the directory to list
   * @return the children present at the time of listing, or an empty list if the directory is gone
   * @throws IOException if the listing failed for a reason other than the directory disappearing
   */
  @Nonnull
  private static List<Path> listChildrenTolerantly(@Nonnull final Path directory)
      throws IOException {
    try (final Stream<Path> children = Files.list(directory)) {
      return children.toList();
    } catch (final NoSuchFileException e) {
      return List.of();
    }
  }

  /**
   * Deletes a file or directory tree, tolerating any part of it disappearing concurrently. Symbolic
   * links are deleted without following them.
   *
   * @param path the file or directory to delete
   * @throws IOException if a deletion failed for a reason other than the entry disappearing
   */
  private static void deleteRecursivelyTolerantly(@Nonnull final Path path) throws IOException {
    if (Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)) {
      for (final Path child : listChildrenTolerantly(path)) {
        deleteRecursivelyTolerantly(child);
      }
    }
    Files.deleteIfExists(path);
  }
}
