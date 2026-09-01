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

package au.csiro.pathling.terminology.store;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermissions;

/**
 * Creates temporary directories that are readable and writable only by their owner, so that
 * intermediate terminology import files are never exposed to other users sharing the host.
 *
 * @author John Grimes
 */
final class SecureTempDirectory {

  private SecureTempDirectory() {}

  /**
   * Creates a temporary directory restricted to the owning user.
   *
   * <p>On POSIX file systems the directory is created atomically with {@code rwx------}
   * permissions, closing the window in which a world-accessible directory would otherwise exist. On
   * non-POSIX file systems (such as Windows) the per-user temporary location already isolates the
   * directory, so it is created without explicit permission attributes.
   *
   * @param prefix the prefix for the directory name
   * @return the path of the newly created directory
   * @throws IOException if the directory cannot be created
   */
  // The non-POSIX fallback creates the directory without explicit permission attributes. This is
  // safe because it is only reached on non-POSIX file systems such as Windows, where the per-user
  // temporary location already isolates the directory from other users; the POSIX path above
  // restricts permissions atomically at creation.
  @SuppressWarnings("java:S5443")
  @Nonnull
  static Path create(@Nonnull final String prefix) throws IOException {
    if (FileSystems.getDefault().supportedFileAttributeViews().contains("posix")) {
      final FileAttribute<?> ownerOnly =
          PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------"));
      return Files.createTempDirectory(prefix, ownerOnly);
    }
    return Files.createTempDirectory(prefix);
  }
}
