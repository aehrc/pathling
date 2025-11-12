package au.csiro.pathling.util;

import static org.assertj.core.api.Assertions.assertThat;

import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.sink.DataSinkBuilder;
import au.csiro.pathling.library.io.source.NdjsonSource;
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


  private final PathlingContext pathlingContext;

  @Autowired
  public TestDataSetup(PathlingContext pathlingContext) {
    this.pathlingContext = pathlingContext;
  }

  public static void staticCopyTestDataToTempDir(Path tempDir, String... resourceTypes) {
    try {
      Path deltaPath = Path.of("src/test/resources/test-data/bulk/fhir/delta");
      if (resourceTypes == null || resourceTypes.length == 0) {
        File deltaTestData = deltaPath.toFile();
        FileUtils.copyDirectoryToDirectory(deltaTestData, tempDir.toFile());
      } else {
        for (String resourceType : resourceTypes) {
          File deltaSpecificTestResourceData = deltaPath.resolve(resourceType + ".parquet")
              .toFile();
          FileUtils.copyDirectoryToDirectory(deltaSpecificTestResourceData, tempDir.toFile());
          assert_file_was_copied_correctly(tempDir, resourceType);
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      logDirectoryContents(tempDir);
    }
  }

  private static void assert_file_was_copied_correctly(Path tempDir, String resourceType) {
    assertThat(tempDir.resolve(resourceType + ".parquet")).exists()
        .isDirectoryContaining(path -> path.toString().endsWith(".parquet"));
  }

  /**
   * Copies the generated parquet data from src/test/resources/test-data/fhir/delta/** to the
   * tempdir, preserving the directory structure. I.e. delta/Encounter.parquet/* is copied with the
   * directory structure so it copies to tempdir/Encounter.parquet/*
   *
   * @param tempDir Where to copy to
   */
  public void copyTestDataToTempDir(Path tempDir) {
    staticCopyTestDataToTempDir(tempDir);
  }

  public void setupTestData(Path ndjsonTestDataDir) {
    try {
      Path deltaPath = ndjsonTestDataDir.resolve("delta");
      Files.createDirectories(deltaPath);
      NdjsonSource ndjsonSource = new NdjsonSource(pathlingContext,
          ndjsonTestDataDir.toAbsolutePath().toString());
      new DataSinkBuilder(pathlingContext, ndjsonSource).saveMode("overwrite")
          .delta(deltaPath.toAbsolutePath().toString());
    } catch (Exception e) {
      throw new RuntimeException("Failed to setup warehouse test data", e);
    } finally {
      logDirectoryContents(ndjsonTestDataDir);
    }

  }

  public static void logDirectoryContents(Path directory) {
    try {
      log.debug("Directory tree for: {}", directory);
      List<Path> paths = Files.walk(directory)
          .sorted()
          .toList();

      printTree(paths, directory);
    } catch (IOException e) {
      log.error("Failed to list directory: {}", directory, e);
    }
  }

  private static void printTree(List<Path> paths, Path root) {
    for (Path path : paths) {
      if (path.equals(root)) {
        continue; // Skip root
      }

      Path relativePath = root.relativize(path);
      int depth = relativePath.getNameCount();

      StringBuilder tree = new StringBuilder();

      // Build the tree structure
      for (int d = 1; d < depth; d++) {
        Path ancestor = getAncestorAtDepth(relativePath, d);
        Path fullAncestor = root.resolve(ancestor);

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

  private static Path getAncestorAtDepth(Path relativePath, int depth) {
    Path result = relativePath;
    for (int i = relativePath.getNameCount(); i > depth; i--) {
      result = result.getParent();
    }
    return result;
  }

  private static boolean isLastSiblingInPaths(List<Path> paths, Path path) {
    Path parent = path.getParent();
    if (parent == null) {
      return true;
    }

    int pathIndex = paths.indexOf(path);
    if (pathIndex == -1) {
      return true;
    }

    // Check if there are any more siblings after this path
    for (int i = pathIndex + 1; i < paths.size(); i++) {
      Path sibling = paths.get(i);
      if (parent.equals(sibling.getParent())) {
        return false; // Found a sibling
      }
      // If we've moved to a different parent level, stop checking
      if (sibling.getNameCount() <= path.getNameCount() &&
          !sibling.startsWith(parent)) {
        break;
      }
    }
    return true;
  }
}
