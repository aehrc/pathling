package au.csiro.pathling;

import au.csiro.pathling.util.TestDataSetup;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceAccessMode;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;

import static au.csiro.pathling.util.ExportOperationUtil.doPolling;
import static au.csiro.pathling.util.ExportOperationUtil.kickOffRequest;
import static org.awaitility.Awaitility.await;

/**
 * @author Felix Naumann
 */
@Slf4j
@Tag("IntegrationTest")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ResourceLock(value = "wiremock", mode = ResourceAccessMode.READ_WRITE)
@ActiveProfiles({"core", "server", "integration-test"})
public class ExportOperationCacheIT {

  @LocalServerPort
  int port;

  @Autowired
  WebTestClient webTestClient;

  @TempDir
  private static Path warehouseDir;

  @DynamicPropertySource
  static void configureProperties(DynamicPropertyRegistry registry) {
    TestDataSetup.staticCopyTestDataToTempDir(warehouseDir);
    registry.add("pathling.storage.warehouseUrl",
        () -> "file://" + warehouseDir.resolve("delta").toAbsolutePath());
  }

  @AfterEach
  void cleanup() throws IOException {
    try (var files = Files.list(warehouseDir)) {
      files.forEach(path -> {
        try {
          if (Files.isDirectory(path)) {
            FileUtils.deleteDirectory(path.toFile());
          } else {
            Files.delete(path);
          }
        } catch (IOException e) {
          log.warn("Failed to delete: " + path, e);
        }
      });
    }
  }
  
  @Test
  void test() {
    String uri = "http://localhost:" + port
        + "/fhir/$export?_outputFormat=application/fhir+ndjson&_since=2017-01-01T00:00:00Z&_type=Patient,Encounter&_elements=identifier,Patient.name,Encounter.subject";
    String pollUrl = kickOffRequest(webTestClient, uri);
    await()
        .atMost(30, TimeUnit.SECONDS)
        .pollInterval(3, TimeUnit.SECONDS)
        .until(() -> doPolling(webTestClient, pollUrl, result -> {
          
        }));
  }
}
