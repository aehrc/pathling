package au.csiro.pathling.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

import au.csiro.pathling.library.io.sink.FileInformation;
import au.csiro.pathling.library.io.sink.WriteDetails;
import au.csiro.pathling.operations.bulkexport.ExportOutputFormat;
import au.csiro.pathling.operations.bulkexport.ExportRequest;
import au.csiro.pathling.operations.bulkexport.ExportRequest.ExportLevel;
import au.csiro.pathling.operations.bulkexport.ExportResponse;
import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.JsonNode;
import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.node.ArrayNode;
import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.node.ObjectNode;
import ca.uhn.fhir.parser.IParser;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.InstantType;
import org.hl7.fhir.r4.model.Resource;
import org.jetbrains.annotations.NotNull;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.test.web.reactive.server.EntityExchangeResult;
import org.springframework.test.web.reactive.server.WebTestClient;

/**
 * @author Felix Naumann
 */
@Slf4j
public class ExportOperationUtil {

  public static JsonNode json(
      final ObjectMapper mapper, final String request, final WriteDetails writeDetails) {
    return json(mapper, InstantType.now(), request, false, writeDetails.fileInfos());
  }

  public static JsonNode json(
      final ObjectMapper mapper,
      final InstantType transactionTime,
      final String request,
      final boolean requiresAccessToken,
      final List<FileInformation> output) {
    final ObjectNode node = mapper.createObjectNode();
    node.put("transactionTime", transactionTime.toString());
    node.put("request", request);
    node.put("requiresAccessToken", requiresAccessToken);
    final List<ObjectNode> outputList =
        output.stream()
            .map(
                fileInfo ->
                    mapper
                        .createObjectNode()
                        .put("type", fileInfo.fhirResourceType())
                        .put("url", fileInfo.absoluteUrl()))
            .toList();
    final ArrayNode arrayNode = mapper.createArrayNode().addAll(outputList);
    node.set("output", arrayNode);
    node.set("deleted", mapper.createArrayNode());
    node.set("error", mapper.createArrayNode());
    return node;
  }

  public static ExportResponse res(final ExportRequest request, final WriteDetails writeDetails) {
    return res(request, writeDetails, false);
  }

  public static ExportResponse res(
      final ExportRequest request,
      final WriteDetails writeDetails,
      final boolean requiresAccessToken) {
    return new ExportResponse(
        request.originalRequest(), request.serverBaseUrl(), writeDetails, requiresAccessToken);
  }

  public static ExportRequest req(
      final String base,
      final ExportOutputFormat outputFormat,
      final InstantType since,
      final List<String> includeResourceTypeFilters) {
    String originalRequest =
        base
            + "_outputFormat="
            + ExportOutputFormat.asParam(outputFormat)
            + "&_since="
            + since.toString();
    if (!includeResourceTypeFilters.isEmpty()) {
      originalRequest += "&_type=" + String.join(",", includeResourceTypeFilters);
    }
    return new ExportRequest(
        originalRequest,
        deriveServerBaseUrl(base),
        outputFormat,
        since,
        null,
        includeResourceTypeFilters,
        List.of(),
        false,
        ExportLevel.SYSTEM,
        Set.of());
  }

  public static ExportRequest req(
      @Nonnull final String base, @Nullable final List<String> elements) {
    return req(base, List.of(), elements);
  }

  public static ExportRequest req(
      @Nonnull final String base,
      @Nullable final InstantType since,
      @Nullable final InstantType until) {
    return req(base, since, until, null, null);
  }

  public static ExportRequest req(
      @Nonnull final String base,
      @Nullable final List<String> includeResourceTypeFilters,
      @Nullable final List<String> elements) {
    return req(base, null, null, includeResourceTypeFilters, elements);
  }

  public static ExportRequest req(
      @Nonnull final String base,
      @Nullable InstantType since,
      @Nullable final InstantType until,
      @Nullable final List<String> includeResourceTypeFilters,
      @Nullable final List<String> elements) {
    if (since == null) {
      since = InstantType.now();
    }
    String originalRequest =
        base
            + "_outputFormat="
            + ExportOutputFormat.asParam(ExportOutputFormat.NDJSON)
            + "&_since="
            + since;

    if (until != null) {
      originalRequest += "&_until=" + until;
    }
    if (includeResourceTypeFilters != null && !includeResourceTypeFilters.isEmpty()) {
      originalRequest += "&_type=" + String.join(",", includeResourceTypeFilters);
    }

    final List<ExportRequest.FhirElement> fhirElements = new ArrayList<>();
    if (elements != null && !elements.isEmpty()) {
      originalRequest += "&_elements=" + String.join(",", elements);
      for (final String el : elements) {
        final String[] split = el.split("\\.");
        if (split.length == 1) {
          fhirElements.add(new ExportRequest.FhirElement(null, split[0]));
        }
        if (split.length == 2) {
          fhirElements.add(new ExportRequest.FhirElement(split[0], split[1]));
        }
      }
    }
    return new ExportRequest(
        originalRequest,
        deriveServerBaseUrl(base),
        ExportOutputFormat.NDJSON,
        since,
        until,
        List.of(),
        fhirElements,
        false,
        ExportLevel.SYSTEM,
        Set.of());
  }

  private static String deriveServerBaseUrl(@Nonnull final String requestUrl) {
    final int exportIndex = requestUrl.indexOf("$export");
    if (exportIndex > 0) {
      String base = requestUrl.substring(0, exportIndex);
      if (base.endsWith("/")) {
        base = base.substring(0, base.length() - 1);
      }
      return base;
    }
    // Remove query string if present.
    final int queryIndex = requestUrl.indexOf("?");
    if (queryIndex > 0) {
      return requestUrl.substring(0, queryIndex);
    }
    return requestUrl;
  }

  public static WriteDetails writeDetails(final List<FileInformation> fileInfos) {
    return new WriteDetails(fileInfos);
  }

  public static WriteDetails writeDetails(final FileInformation... fileInfos) {
    return new WriteDetails(Arrays.asList(fileInfos));
  }

  public static FileInformation fileInfo(final String fhirResourceType, final String absoluteUrl) {
    return new FileInformation(fhirResourceType, absoluteUrl);
  }

  @Nullable
  public static InstantType date(final String date) {
    return date != null ? new InstantType(date + "T00:00:00Z") : null;
  }

  public static List<Resource> parseNdjson(
      final IParser parser, final String jsonContent, final String expectedType) {
    return jsonContent
        .lines()
        .filter(line -> !line.trim().isEmpty()) // Skip empty lines
        .map(
            line -> {
              try {
                final Resource resource = (Resource) parser.parseResource(line);
                // Verify the resource type matches what's expected
                assertThat(resource.getResourceType().name()).isEqualTo(expectedType);
                return resource;
              } catch (final Exception e) {
                fail(
                    "Failed to parse FHIR resource from line: "
                        + line
                        + ". Error: "
                        + e.getMessage());
                return null;
              }
            })
        .filter(Objects::nonNull)
        .toList();
  }

  public static <T extends IBaseResource> T readNdjson(
      final IParser parser, final FileInformation fileInfo, final Class<T> clazz)
      throws IOException {
    return parser.parseResource(
        clazz, Files.readString(Paths.get(URI.create(fileInfo.absoluteUrl()))));
  }

  public static ExportResponse resolveTempDirIn(
      final ExportResponse exportResponse, final Path tempDir, final UUID fakeJobId) {
    final List<FileInformation> newFileInfos =
        exportResponse.getWriteDetails().fileInfos().stream()
            .map(
                fileInfo ->
                    fileInfo(
                        fileInfo.fhirResourceType(),
                        fileInfo
                            .absoluteUrl()
                            .replace(
                                "WAREHOUSE_PATH",
                                "file:" + tempDir.toAbsolutePath() + "/jobs/" + fakeJobId)))
            .toList();
    return new ExportResponse(
        exportResponse.getKickOffRequestUrl(),
        "http://localhost:8080/fhir",
        writeDetails(newFileInfos),
        exportResponse.isRequiresAccessToken());
  }

  public static @NotNull String kickOffRequest(
      final WebTestClient webTestClient, final String uri) {
    final String pollUrl =
        webTestClient
            .get()
            .uri(uri)
            .header("Accept", "application/fhir+json")
            .header("Prefer", "respond-async")
            .exchange()
            .expectStatus()
            .is2xxSuccessful()
            .expectHeader()
            .exists("Content-Location")
            .returnResult(String.class)
            .getResponseHeaders()
            .getFirst("Content-Location");

    assertThat(pollUrl).isNotNull();
    return pollUrl;
  }

  public static boolean doPolling(
      final WebTestClient webTestClient,
      final String pollUrl,
      final Consumer<EntityExchangeResult<String>> consumer) {
    final EntityExchangeResult<String> pollResult =
        webTestClient
            .get()
            .uri(pollUrl)
            .exchange()
            .expectStatus()
            .is2xxSuccessful()
            .expectBody(String.class)
            .returnResult();
    final HttpStatusCode status = pollResult.getStatus();
    final HttpHeaders headers = pollResult.getResponseHeaders();
    if (status == HttpStatus.ACCEPTED) {
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
}
