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

import static org.assertj.core.api.Assertions.assertThat;

import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.sink.DataSinkBuilder;
import au.csiro.pathling.library.io.source.NdjsonSource;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author Felix Naumann
 */
@Slf4j
@Component
public class TestDataSetup {

  @Nonnull private final PathlingContext pathlingContext;

  @Autowired
  public TestDataSetup(@Nonnull final PathlingContext pathlingContext) {
    this.pathlingContext = pathlingContext;
  }

  /**
   * Returns the path to the read-only test data directory. Tests that only read data (e.g., export
   * operations) can use this path directly instead of copying data to a temp directory.
   *
   * @return The absolute path to the read-only test data directory.
   */
  @Nonnull
  public static Path getReadOnlyTestDataPath() {
    return Path.of("src/test/resources/test-data/bulk/fhir/delta").toAbsolutePath();
  }

  public static void copyTestDataToTempDir(
      @Nonnull final Path tempDir, @Nullable final String... resourceTypes) {
    try {
      final Path deltaPath = Path.of("src/test/resources/test-data/bulk/fhir/delta");
      if (resourceTypes == null || resourceTypes.length == 0) {
        final File deltaTestData = deltaPath.toFile();
        FileUtils.copyDirectoryToDirectory(deltaTestData, tempDir.toFile());
      } else {
        for (final String resourceType : resourceTypes) {
          final File deltaSpecificTestResourceData =
              deltaPath.resolve(resourceType + ".parquet").toFile();
          FileUtils.copyDirectoryToDirectory(deltaSpecificTestResourceData, tempDir.toFile());
          assertFileWasCopiedCorrectly(tempDir, resourceType);
        }
      }
    } catch (final IOException e) {
      throw new RuntimeException(e);
    } finally {
      logDirectoryContents(tempDir);
    }
  }

  private static void assertFileWasCopiedCorrectly(
      @Nonnull final Path tempDir, @Nonnull final String resourceType) {
    assertThat(tempDir.resolve(resourceType + ".parquet"))
        .exists()
        .isDirectoryContaining(path -> path.toString().endsWith(".parquet"));
  }

  public void setupTestData(@Nonnull final Path ndjsonTestDataDir) {
    try {
      final Path deltaPath = ndjsonTestDataDir.resolve("delta");
      Files.createDirectories(deltaPath);
      final NdjsonSource ndjsonSource =
          new NdjsonSource(pathlingContext, ndjsonTestDataDir.toAbsolutePath().toString());
      new DataSinkBuilder(pathlingContext, ndjsonSource)
          // If the data already exists, skip the import process.
          .saveMode("ignore")
          .delta(deltaPath.toAbsolutePath().toString());
    } catch (final Exception e) {
      throw new RuntimeException("Failed to setup warehouse test data", e);
    } finally {
      logDirectoryContents(ndjsonTestDataDir);
    }
  }

  public static void logDirectoryContents(@Nonnull final Path directory) {
    try {
      log.debug("Directory tree for: {}", directory);
      final List<Path> paths;
      try (final var stream = Files.walk(directory)) {
        paths = stream.sorted().toList();
      }

      printTree(paths, directory);
    } catch (final IOException e) {
      log.error("Failed to list directory: {}", directory, e);
    }
  }

  private static void printTree(@Nonnull final List<Path> paths, @Nonnull final Path root) {
    for (final Path path : paths) {
      if (path.equals(root)) {
        continue; // Skip root
      }

      final Path relativePath = root.relativize(path);
      final int depth = relativePath.getNameCount();

      final StringBuilder tree = new StringBuilder();

      // Build the tree structure
      for (int d = 1; d < depth; d++) {
        final Path ancestor = getAncestorAtDepth(relativePath, d);
        final Path fullAncestor = root.resolve(ancestor);

        if (isLastSiblingInPaths(paths, fullAncestor)) {
          tree.append("    ");
        } else {
          tree.append("│   ");
        }
      }

      // Add the final branch
      if (isLastSiblingInPaths(paths, path)) {
        tree.append("└── ");
      } else {
        tree.append("├── ");
      }

      tree.append(relativePath.getFileName());
      if (Files.isDirectory(path)) {
        tree.append("/");
      }

      log.debug(tree.toString());
    }
  }

  @Nonnull
  private static Path getAncestorAtDepth(@Nonnull final Path relativePath, final int depth) {
    Path result = relativePath;
    for (int i = relativePath.getNameCount(); i > depth; i--) {
      result = result.getParent();
    }
    return result;
  }

  private static boolean isLastSiblingInPaths(
      @Nonnull final List<Path> paths, @Nonnull final Path path) {
    final Path parent = path.getParent();
    if (parent == null) {
      return true;
    }

    final int pathIndex = paths.indexOf(path);
    if (pathIndex == -1) {
      return true;
    }

    // Check if there are any more siblings after this path
    for (int i = pathIndex + 1; i < paths.size(); i++) {
      final Path sibling = paths.get(i);
      if (parent.equals(sibling.getParent())) {
        return false; // Found a sibling
      }
      // If we've moved to a different parent level, stop checking
      if (sibling.getNameCount() <= path.getNameCount() && !sibling.startsWith(parent)) {
        break;
      }
    }
    return true;
  }
}
