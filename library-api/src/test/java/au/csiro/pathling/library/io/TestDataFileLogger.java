package au.csiro.pathling.library.io;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Felix Naumann
 */
@Slf4j
public class TestDataFileLogger {

  public static void logDirectoryContents(org.apache.hadoop.fs.Path directory) {
    logDirectoryContents(Path.of(directory.toUri()));
  }
  
  public static void logDirectoryContents(Path directory) {
    try {
      log.info("Directory tree for: {}", directory);
      List<Path> paths = Files.walk(directory)
          .sorted()
          .collect(Collectors.toList());

      printTree(paths, directory);
    } catch (IOException e) {
      log.error("Failed to list directory: {}", directory, e);
    }
  }

  private static void printTree(List<Path> paths, Path root) {
    for (Path path : paths) {
      if (path.equals(root)) continue; // Skip root

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

      log.info(tree.toString());
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
    if (parent == null) return true;

    int pathIndex = paths.indexOf(path);
    if (pathIndex == -1) return true;

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
