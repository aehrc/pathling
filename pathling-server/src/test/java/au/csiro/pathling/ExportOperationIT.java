package au.csiro.pathling;

import static au.csiro.pathling.library.io.sink.DataSink.FileInfo;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.awaitility.Awaitility.await;

import au.csiro.pathling.export.ExportExecutor;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.JsonNode;
import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import au.csiro.pathling.util.ExportOperationUtil;
import au.csiro.pathling.util.TestDataSetup;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.StreamSupport;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Encounter;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Resource;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceAccessMode;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.EntityExchangeResult;
import org.springframework.test.web.reactive.server.WebTestClient;

/**
 * @author Felix Naumann
 */
@Slf4j
@Tag("IntegrationTest")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ResourceLock(value = "wiremock", mode = ResourceAccessMode.READ_WRITE)
@ActiveProfiles({"core", "server", "integration-test"})
    //@Execution(ExecutionMode.CONCURRENT)
class ExportOperationIT {
  
  @LocalServerPort
  int port;

  @Autowired
  WebTestClient webTestClient;

  @TempDir
  private static Path warehouseDir;
  private String warehouseUrl;
  @Autowired
  private TestDataSetup testDataSetup;
  @Autowired
  private FhirContext fhirContext;
  @Autowired
  private PathlingContext pathlingContext;
  private IParser parser;
  @Autowired
  private QueryableDataSource deltaLake;
  @Autowired
  private SparkSession sparkSession;
  @Autowired
  private ExportExecutor exportExecutor;

  @DynamicPropertySource
  static void configureProperties(DynamicPropertyRegistry registry) {
    TestDataSetup.staticCopyTestDataToTempDir(warehouseDir);
    registry.add("pathling.storage.warehouseUrl", () -> "file://" + warehouseDir.resolve("delta").toAbsolutePath());
  }

  @BeforeEach
  void setup() {
    
    
    warehouseUrl = "file://" + warehouseDir.toAbsolutePath();
    // exportExecutor = new ExportExecutor(pathlingContext, deltaLake, fhirContext, sparkSession,
    //     warehouseUrl);
    //exportExecutor.setWarehouseUrl(Paths.get(warehouseUrl).toString());
    parser = fhirContext.newJsonParser();
    
    webTestClient = webTestClient.mutate()
        .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(100 * 1024 * 1024)).build(); // 100 MB
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
  void test_missing_respond_async_header_lenient_runs() {
    testDataSetup.copyTestDataToTempDir(warehouseDir);
    String uri = "http://localhost:" + port
        + "/fhir/$export?_outputFormat=application/fhir+ndjson&_since=2017-01-01T00:00:00Z";
    webTestClient.get()
        .uri(uri)
        .header("Accept", "application/fhir+json")
        .header("Prefer", "handling=lenient")
        .exchange()
        .expectStatus().is2xxSuccessful();
  }

  @Test
  void test_missing_respond_async_header_strict_returns_error() {
    testDataSetup.copyTestDataToTempDir(warehouseDir);

    String uri = "http://localhost:" + port
        + "/fhir/$export?_outputFormat=application/fhir+ndjson&_since=2017-01-01T00:00:00Z";
    webTestClient.get()
        .uri(uri)
        .header("Accept", "application/fhir+json")
        .exchange()
        .expectStatus().is4xxClientError().expectBody();
  }

  @Test
  void test_cancelling_request_returns_202() {
    testDataSetup.copyTestDataToTempDir(warehouseDir);

    String uri = "http://localhost:" + port
        + "/fhir/$export?_outputFormat=application/fhir+ndjson&_since=2017-01-01T00:00:00Z";
    String pollUrl = kickOffRequest(uri);

    // send a DELETE request after 3 seconds
    await().pollDelay(3, TimeUnit.SECONDS)
        .atMost(4, TimeUnit.SECONDS)
        .until(() -> true);

    webTestClient.delete()
        .uri(pollUrl)
        .exchange()
        .expectStatus().isEqualTo(202);
  }

  @Test
  void test_polling_cancelled_request_returns_404() {
    testDataSetup.copyTestDataToTempDir(warehouseDir);

    String uri = "http://localhost:" + port
        + "/fhir/$export?_outputFormat=application/fhir+ndjson&_since=2017-01-02T00:00:00Z";
    String pollUrl = kickOffRequest(uri);

    // Send DELETE after 2 seconds
    await().pollDelay(2, TimeUnit.SECONDS)
        .atMost(3, TimeUnit.SECONDS)
        .until(() -> true); // Just wait

    webTestClient.delete().uri(pollUrl).exchange().expectStatus().isAccepted();

    // Now wait for the GET to return 404 (polls until condition is met)
    await()
        .atMost(10, TimeUnit.SECONDS)
        .pollInterval(500, TimeUnit.MILLISECONDS)
        .untilAsserted(() -> {
          webTestClient.get()
              .uri(pollUrl)
              .exchange()
              .expectStatus().isEqualTo(404);
        });
  }


  @Test
  void test_invalid_kickoff_request() {
    testDataSetup.copyTestDataToTempDir(warehouseDir);

    String uri = "http://localhost:" + port
        + "/fhir/$export?_outputFormat=application/fhir+ndjson&_since=2017-01-01T00:00:00Z&_type=Patient,Encounter";
    webTestClient.get()
        .uri(uri)
        .header("Accept", "INVALID")
        .header("Prefer", "respond-async")
        .exchange()
        .expectStatus().isBadRequest();
  }

  @Test
  void export_valid() {
    testDataSetup.copyTestDataToTempDir(warehouseDir);

    String uri = "http://localhost:" + port
        + "/fhir/$export?_outputFormat=application/fhir+ndjson&_since=2017-01-01T00:00:00Z&_type=Patient,Encounter&_elements=identifier,Patient.name,Encounter.subject";
    String pollUrl = kickOffRequest(uri);
    await()
        .atMost(30, TimeUnit.SECONDS)
        .pollInterval(3, TimeUnit.SECONDS)
        .until(() -> doPolling(pollUrl, result -> {
          try {
            assert_complete_result(uri, result.getResponseBody(), result.getResponseHeaders());
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }));
  }

  private boolean doPolling(String pollUrl, Consumer<EntityExchangeResult<String>> consumer) {
    EntityExchangeResult<String> pollResult = webTestClient.get()
        .uri(pollUrl)
        .exchange()
        .expectStatus().is2xxSuccessful()
        .expectBody(String.class)
        .returnResult();
    HttpStatusCode status = pollResult.getStatus();
    HttpHeaders headers = pollResult.getResponseHeaders();
    if (status == HttpStatus.ACCEPTED) {
      // assertThat(headers).containsKey("X-Progress");
      log.info("Polling... {}", headers.get("X-Progress"));
      return false; // keep polling
    }
    if (status == HttpStatus.OK) {
      log.info("Polling complete.");
      consumer.accept(pollResult);
      return true;
    }
    throw new AssertionError("Unexpected polling status: %s".formatted(status));
  }

  private @NotNull String kickOffRequest(String uri) {
    String pollUrl = webTestClient.get()
        .uri(uri)
        .header("Accept", "application/fhir+json")
        .header("Prefer", "respond-async")
        .exchange()
        .expectStatus().is2xxSuccessful()
        .expectHeader().exists("Content-Location")
        .returnResult(String.class)
        .getResponseHeaders()
        .getFirst("Content-Location");

    assertThat(pollUrl).isNotNull();
    return pollUrl;
  }

  private void assert_complete_result(String originalRequestUri, String responseBody,
      HttpHeaders headers) throws IOException {
    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode node = objectMapper.readTree(responseBody);

    assertThat(headers).containsKey("Expires");
    assertThat(headers.getFirst("Content-Type"))
        .isNotNull()
        .isEqualTo("application/json");

    assertThat(node.has("transactionTime")).isTrue();
    assertThat(node.get("request").asText()).isEqualTo(originalRequestUri);
    assertThat(node.get("requiresAccessToken").asBoolean()).isFalse();
    assertThat(node.has("deleted")).isTrue();
    assertThat(node.has("error")).isTrue();
    JsonNode output = node.get("output");
    assertThat(output)
        .isNotNull()
        .hasSize(2);

    List<FileInfo> actualFileInfos = StreamSupport.stream(output.spliterator(), false)
        .map(jsonNode -> new FileInfo(
            jsonNode.get("type").asText(),
            jsonNode.get("url").asText(),
            jsonNode.get("count").asLong()
        ))
        .toList();
    
    assertThat(actualFileInfos).isNotEmpty();

    Map<String, List<? extends IBaseResource>> downloadedResources = new HashMap<>();
    actualFileInfos.forEach(fileInfo -> {
      String fileContent = webTestClient.get()
          .uri(fileInfo.absoluteUrl())
          .exchange()
          .expectStatus().isOk()
          .expectBody(String.class)
          .returnResult()
          .getResponseBody();
      assertThat(fileContent).isNotNull();
      List<Resource> resources = ExportOperationUtil.parseNDJSON(parser, fileContent, fileInfo.fhirResourceType());
      downloadedResources.put(fileInfo.fhirResourceType(), resources);
    });
    assertThat(downloadedResources).isNotEmpty();

    // It's hard to assert that all other columns are not returned. I carefully checked that the test-data (Patient.ndjson and Encounter.ndjson)
    // have a Patient.active, Encounter.reasonCode, Encounter.serviceProvider values. Now it is checked that they
    // are not present in the returned ndjson
    
    downloadedResources.forEach((string, iBaseResources) -> {
      switch (string) {
        case "Patient" -> {
          List<Patient> patients = (List<Patient>) iBaseResources;
          patients.forEach(patient -> {
            assertThat(patient.hasIdentifier()).isTrue();
            assertThat(patient.hasName()).isTrue();
            assertThat(patient.hasGender()).isFalse();
          });
        }
        case "Encounter" -> {
          List<Encounter> encounters = (List<Encounter>) iBaseResources;
          encounters.forEach(encounter -> {
            assertThat(encounter.hasSubject()).isTrue();
            assertThat(encounter.hasReasonCode()).isFalse();
            assertThat(encounter.hasServiceProvider()).isFalse();
          });
        }
        default -> fail("Unexpected resource type %s".formatted(string));
      }
    });
    

    // List<FileInfo> expectedFileInfos = List.of(
    //     new FileInfo("Patient", RESOLVE_PATIENT.apply(warehouseUrl), PATIENT_COUNT),
    //     new FileInfo("Encounter", RESOLVE_ENCOUNTER.apply(warehouseUrl), ENCOUNTER_COUNT)
    // );
    // assertThat(actualFileInfos).containsExactlyInAnyOrderElementsOf(expectedFileInfos);
  }
}
