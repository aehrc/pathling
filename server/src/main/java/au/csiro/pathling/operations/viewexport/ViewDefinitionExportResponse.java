/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.operations.viewexport;

import au.csiro.pathling.OperationResponse;
import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.node.ArrayNode;
import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.node.ObjectNode;
import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import lombok.Getter;
import org.apache.http.client.utils.URIBuilder;
import org.hl7.fhir.r4.model.Binary;
import org.hl7.fhir.r4.model.InstantType;

/**
 * Represents the response from a ViewDefinition export operation, containing the export manifest.
 *
 * @author John Grimes
 */
@Getter
public class ViewDefinitionExportResponse implements OperationResponse<Binary> {

  @Nonnull
  private final ObjectMapper mapper = new ObjectMapper();

  @Nonnull
  private final String kickOffRequestUrl;

  @Nonnull
  private final String serverBaseUrl;

  @Nonnull
  private final List<ViewExportOutput> outputs;

  /**
   * Whether an access token is required to retrieve results.
   */
  private final boolean requiresAccessToken;

  /**
   * Creates a new ViewDefinitionExportResponse.
   *
   * @param kickOffRequestUrl the original export request URL (used in the manifest)
   * @param serverBaseUrl the FHIR server base URL (used for constructing result URLs)
   * @param outputs the list of view export outputs
   * @param requiresAccessToken whether access token is required to retrieve results
   */
  public ViewDefinitionExportResponse(
      @Nonnull final String kickOffRequestUrl,
      @Nonnull final String serverBaseUrl,
      @Nonnull final List<ViewExportOutput> outputs,
      final boolean requiresAccessToken) {
    this.kickOffRequestUrl = kickOffRequestUrl;
    this.serverBaseUrl = serverBaseUrl;
    this.outputs = outputs;
    this.requiresAccessToken = requiresAccessToken;
  }

  @Nonnull
  @Override
  public Binary toOutput() {
    final String manifestJSON;
    try {
      manifestJSON = buildManifest();
    } catch (final IOException e) {
      throw new IllegalStateException(e);
    }
    final Binary binary = new Binary();
    binary.setContentType("application/json");
    binary.setData(manifestJSON.getBytes(StandardCharsets.UTF_8));
    return binary;
  }

  @Nonnull
  private String buildManifest() throws IOException {
    final ObjectNode manifest = mapper.createObjectNode();

    // Ensure the base URL ends with a slash for proper URL construction.
    final String normalizedBaseUrl =
        serverBaseUrl.endsWith("/")
            ? serverBaseUrl
            : serverBaseUrl + "/";

    manifest.put("transactionTime", InstantType.now().getValueAsString());
    manifest.put("request", kickOffRequestUrl);
    manifest.put("requiresAccessToken", requiresAccessToken);

    final ArrayNode outputArray = mapper.createArrayNode();
    for (final ViewExportOutput output : outputs) {
      for (final String fileUrl : output.fileUrls()) {
        final ObjectNode outputNode = mapper.createObjectNode();
        outputNode.put("name", output.name());
        outputNode.put("url", buildResultUrl(normalizedBaseUrl, fileUrl));
        outputArray.add(outputNode);
      }
    }
    manifest.set("output", outputArray);

    // Not supported yet but required by the specification.
    manifest.set("error", mapper.createArrayNode());

    return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(manifest);
  }

  /**
   * Converts a local file URL to a remote result URL.
   *
   * @param baseUrl the normalised base server URL
   * @param localUrl the local file URL containing the job ID and filename
   * @return the remote result URL
   */
  @Nonnull
  private static String buildResultUrl(@Nonnull final String baseUrl,
      @Nonnull final String localUrl) {
    try {
      final String[] parts = localUrl.split("/jobs/")[1].split("/");
      final String jobUUID = parts[0];
      final String file = parts[1];

      return new URIBuilder(baseUrl + "$result")
          .addParameter("job", jobUUID)
          .addParameter("file", file)
          .build()
          .toString();
    } catch (final URISyntaxException e) {
      throw new InternalErrorException(e);
    }
  }

}
