package au.csiro.pathling.util;

import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.sink.DataSinkBuilder;
import au.csiro.pathling.library.io.sink.FileInfo;
import au.csiro.pathling.library.io.source.DataSourceBuilder;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.JsonNode;
import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Felix Naumann
 */
@Slf4j
@Component
public class TestDataSetup {


  private final WebClient webClient;
  private final PathlingContext pathlingContext;

  @Autowired
  public TestDataSetup(PathlingContext pathlingContext) {
    this.pathlingContext = pathlingContext;
    this.webClient = WebClient.builder()
        .baseUrl("https://bulk-data.smarthealthit.org")
        .codecs(clientCodecConfigurer -> clientCodecConfigurer
            .defaultCodecs()
            .maxInMemorySize(50 * 1024 * 1024))
        .build();
  }

  /**
   * Download the test data from https://bulk-data.smarthealthit.org/fhir/$export A 202 is returned
   * and polled (blocking) until the result is returned from the server. The data is then written
   * into src/test/resources/test-data/fhir/*.ndjson for each FHIR resources
   */
  public void downloadFromSmartHealthBlocking() {
    Path downloadDir = Path.of("src/test/resources/test-data/fhir");
    try {
      Files.createDirectories(downloadDir);
    } catch (IOException e) {
      throw new RuntimeException("Failed to create download directory", e);
    }
    initiateExport()
        .flatMap(this::pollForCompletion)
        .flatMap(responseBody -> downloadFiles(responseBody, downloadDir))
        .block();
  }

  private Mono<String> initiateExport() {
    log.info("Calling to remote server");
    return webClient.get()
        .uri("/fhir/$export")
        .header("Accept", "application/fhir+json")
        .header("Prefer", "respond-async")
        .retrieve()
        .toEntity(String.class)
        .map(response -> {
          log.info("Fetching Content-Location");
          String statusUrl = response.getHeaders().getFirst("Content-Location");
          if (statusUrl == null) {
            throw new RuntimeException("No Content-Location header in response");
          }
          return statusUrl;
        });
  }

  private Mono<String> pollForCompletion(String statusUrl) {
    return Mono.defer(() -> checkStatus(statusUrl))
        .flatMap(response -> {
          if (response.getStatusCode().is2xxSuccessful()) {
            log.info("Fetched data");
            String body = response.getBody();
            if (body == null) {
              log.info("Received 2xx but body is empty, try again later");
              return Mono.delay(Duration.ofSeconds(5))
                  .then(pollForCompletion(statusUrl));
              //return Mono.error(new RuntimeException("Response successful but body is null for %s".formatted(statusUrl)));
            }
            return Mono.just(body);
          } else if (response.getStatusCode().value() == 202) {
            log.info("Still processing, try again later");
            // Still processing, wait and retry
            return Mono.delay(Duration.ofSeconds(5))
                .then(pollForCompletion(statusUrl));
          } else {
            return Mono.error(new RuntimeException("Export failed: " + response.getStatusCode()));
          }
        })
        .retry(50); // Max 50 retries (about 4 minutes)
  }

  private Mono<ResponseEntity<String>> checkStatus(String statusUrl) {
    return webClient.get()
        .uri(statusUrl)
        .retrieve()
        .toEntity(String.class);
  }

  private Mono<Void> downloadFiles(String responseBody, Path downloadDir) {
    log.info("Downloading files");
    List<FileInfo> files = parseFileUrls(responseBody);
    return Flux.fromIterable(files)
        .flatMap(fileInfo -> downloadFile(fileInfo, downloadDir))
        .then();
  }

  private Mono<Void> downloadFile(FileInfo fileInfo, Path downloadDir) {
    String filename = fileInfo.fhirResourceType() + ".ndjson";
    Path filePath = downloadDir.resolve(filename);
    log.info("Downloading {} to {}", filename, filePath.toAbsolutePath());
    File file = filePath.toFile();
    if (file.exists() && file.isFile()) {
      log.info("Skipping downloading {} because a file already exists at {}", filename,
          filePath.toAbsolutePath());
      return Mono.empty();
    }

    return webClient.get()
        .uri(fileInfo.absoluteUrl())
        .retrieve()
        .bodyToMono(byte[].class)
        .flatMap(bytes -> {
          try {
            Files.write(filePath, bytes);
            return Mono.empty();
          } catch (IOException e) {
            return Mono.error(new RuntimeException("Failed to write file: " + filename, e));
          }
        });
  }

  private List<FileInfo> parseFileUrls(String responseBody) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      JsonNode root = mapper.readTree(responseBody);
      JsonNode outputArray = root.get("output");

      List<FileInfo> files = new ArrayList<>();
      for (JsonNode item : outputArray) {
        files.add(new FileInfo(
            item.get("type").asText(),
            item.get("url").asText(),
            item.get("count").asLong()
        ));
      }
      return files;
    } catch (Exception e) {
      throw new RuntimeException("Failed to parse response", e);
    }
  }
  
  public static void staticCopyTestDataToTempDir(Path tempDir, String... resourceTypes) {
    try {
      Path deltaPath = Path.of("src/test/resources/test-data/fhir/delta");
      if (resourceTypes == null || resourceTypes.length == 0) {
        File deltaTestData = deltaPath.toFile();
        FileUtils.copyDirectoryToDirectory(deltaTestData, tempDir.toFile());
      } else {
        for(String resourceType : resourceTypes) {
          File deltaSpecificTestResourceData = deltaPath.resolve(resourceType + ".parquet").toFile();
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
      QueryableDataSource dataSource = new DataSourceBuilder(pathlingContext).ndjson(
          ndjsonTestDataDir.toAbsolutePath().toString());
      new DataSinkBuilder(pathlingContext, dataSource).saveMode("overwrite")
          .delta(deltaPath.toAbsolutePath().toString());
    } catch (Exception e) {
      throw new RuntimeException("Failed to setup warehouse test data", e);
    } finally {
      logDirectoryContents(ndjsonTestDataDir);
    }

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
