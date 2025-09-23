package au.csiro.pathling.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

import au.csiro.pathling.export.ExportOutputFormat;
import au.csiro.pathling.export.ExportRequest;
import au.csiro.pathling.export.ExportResponse;
import au.csiro.pathling.library.io.sink.FileInfo;
import au.csiro.pathling.library.io.sink.NdjsonWriteDetails;
import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.JsonNode;
import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.node.ArrayNode;
import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.node.ObjectNode;
import ca.uhn.fhir.parser.IParser;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.BufferedReader;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations;
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

    public static JsonNode json(ObjectMapper mapper, String request, NdjsonWriteDetails writeDetails) {
        return json(mapper, InstantType.now(), request, false, writeDetails.fileInfos());
    }

    public static JsonNode json(ObjectMapper mapper, InstantType transactionTime, String request, boolean requiresAccessToken, List<FileInfo> output) {
        ObjectNode node = mapper.createObjectNode();
        node.put("transactionTime", transactionTime.toString());
        node.put("request", request);
        node.put("requiresAccessToken", requiresAccessToken);
        List<ObjectNode> outputList = output.stream()
                .map(fileInfo -> mapper.createObjectNode()
                        .put("type", fileInfo.fhirResourceType())
                        .put("url", fileInfo.absoluteUrl())
                        .put("count", fileInfo.count())
                )
                .toList();
        ArrayNode arrayNode = mapper.createArrayNode().addAll(outputList);
        node.set("output", arrayNode);
        node.set("deleted", mapper.createArrayNode());
        node.set("error", mapper.createArrayNode());
        return node;
    }

    public static ExportResponse res(ExportRequest request, NdjsonWriteDetails writeDetails) {
        return new ExportResponse(request.originalRequest(), writeDetails);
    }

    public static ExportRequest req(String base,
                                    ExportOutputFormat outputFormat,
                                    InstantType since,
                                    List<Enumerations.ResourceType> includeResourceTypeFilters) {
        String originalRequest = base + "_outputFormat=" + ExportOutputFormat.asParam(outputFormat) + "&_since=" + since.toString();
        if(!includeResourceTypeFilters.isEmpty()) {
            originalRequest += "&_type=" + includeResourceTypeFilters.stream().map(Enumerations.ResourceType::toCode).collect(Collectors.joining(","));
        }
        return new ExportRequest(originalRequest, outputFormat, since, null, includeResourceTypeFilters, List.of());
    }

    public static ExportRequest req(@Nonnull String base, @Nullable List<String> elements) {
        return req(base, List.of(), elements);
    }
    
    public static ExportRequest req(@Nonnull String base, @Nullable InstantType since,@Nullable InstantType until) {
      return req(base, since, until, null, null);
    }

    public static ExportRequest req(@Nonnull String base, @Nullable List<Enumerations.ResourceType> includeResourceTypeFilters, @Nullable List<String> elements) {
      return req(base, null, null, includeResourceTypeFilters, elements);
    }
    
    public static ExportRequest req(
        @Nonnull String base, 
        @Nullable InstantType since,
        @Nullable InstantType until,
        @Nullable List<Enumerations.ResourceType> includeResourceTypeFilters,
        @Nullable List<String> elements) {
      if (since == null) {
        since = InstantType.now();
      }
      String originalRequest =
          base + "_outputFormat=" + ExportOutputFormat.asParam(ExportOutputFormat.ND_JSON)
              + "&_since=" + since;

      if (until != null) {
        originalRequest += "&_until=" + until;
      }
      if (includeResourceTypeFilters != null && !includeResourceTypeFilters.isEmpty()) {
        originalRequest +=
            "&_type=" + includeResourceTypeFilters.stream().map(Enumerations.ResourceType::toCode)
                .collect(Collectors.joining(","));
      }

      List<ExportRequest.FhirElement> fhirElements = new ArrayList<>();
      if (elements != null && !elements.isEmpty()) {
        originalRequest += "&_elements=" + String.join(",", elements);
        for (String el : elements) {
          String[] split = el.split("\\.");
          if (split.length == 1) {
            fhirElements.add(new ExportRequest.FhirElement(null, split[0]));
          }
          if (split.length == 2) {
            fhirElements.add(
                new ExportRequest.FhirElement(Enumerations.ResourceType.fromCode(split[0]),
                    split[1]));
          }
        }
      }
      return new ExportRequest(originalRequest, ExportOutputFormat.ND_JSON, since, until, List.of(), fhirElements);
    }

    public static NdjsonWriteDetails write_details(List<FileInfo> fileInfos) {
        return new NdjsonWriteDetails(fileInfos);
    }

    public static NdjsonWriteDetails write_details(FileInfo... fileInfos) {
        return new NdjsonWriteDetails(Arrays.asList(fileInfos));
    }

    public static FileInfo fi(String fhirResourceType, String absoluteUrl, long count) {
        return new FileInfo(fhirResourceType, absoluteUrl, count);
    }

    public static InstantType date(String date) {
        return date != null ? new InstantType(date + "T00:00:00Z") : null;
    }

  public static List<Resource> parseNDJSON(IParser parser, String jsonContent, String expectedType) {
    return jsonContent.lines()
        .filter(line -> !line.trim().isEmpty()) // Skip empty lines
        .map(line -> {
          try {
            Resource resource = (Resource) parser.parseResource(line);
            // Verify the resource type matches what's expected
            assertThat(resource.getResourceType().name()).isEqualTo(expectedType);
            return resource;
          } catch (Exception e) {
            fail("Failed to parse FHIR resource from line: " + line + ". Error: " + e.getMessage());
            return null;
          }
        })
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

    public static <T extends IBaseResource> T read_first_from_multiple_lines_ndjson(IParser parser, FileInfo fileInfo, Class<T> clazz) {
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(URI.create(fileInfo.absoluteUrl())))) {
            String firstLine = reader.readLine();
            if (firstLine != null && !firstLine.trim().isEmpty()) {
                // Parse firstLine as JSON
                return parser.parseResource(clazz, firstLine);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        throw new RuntimeException();
    }

    public static <T extends IBaseResource> T read_ndjson(IParser parser, FileInfo fileInfo, Class<T> clazz) throws IOException {
        return parser.parseResource(clazz, Files.readString(Paths.get(URI.create(fileInfo.absoluteUrl()))));
    }

  public static ExportResponse resolveTempDirIn(ExportResponse exportResponse, Path tempDir, UUID fakeJobId) {
      List<FileInfo> newFileInfos = exportResponse.getWriteDetails().fileInfos().stream()
              .map(
                      fileInfo -> fi(fileInfo.fhirResourceType(),
                              fileInfo.absoluteUrl().replace("WAREHOUSE_PATH", "file:" + tempDir.toAbsolutePath().toString() + "/jobs/" + fakeJobId.toString()),
                              fileInfo.count()))
              .toList();
      return new ExportResponse(
              exportResponse.getKickOffRequestUrl(),
              write_details(newFileInfos)
      );
  }

    public static @NotNull String kickOffRequest(WebTestClient webTestClient, String uri) {
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

  public static boolean doPolling(WebTestClient webTestClient, String pollUrl, Consumer<EntityExchangeResult<String>> consumer) {
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
}
