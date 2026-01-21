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

package au.csiro.pathling.operations.bulkexport;

import au.csiro.pathling.OperationResponse;
import au.csiro.pathling.library.io.sink.FileInformation;
import au.csiro.pathling.library.io.sink.WriteDetails;
import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.node.ArrayNode;
import au.csiro.pathling.shaded.com.fasterxml.jackson.databind.node.ObjectNode;
import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import jakarta.annotation.Nonnull;
import java.net.URISyntaxException;
import java.util.Objects;
import lombok.Getter;
import org.apache.http.client.utils.URIBuilder;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.InstantType;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.hl7.fhir.r4.model.UriType;

/**
 * Represents the response from a bulk export operation, containing the export manifest.
 *
 * @author Felix Naumann
 * @author John Grimes
 */
public class ExportResponse implements OperationResponse<Parameters> {

  private static final String NATIVE_JSON_KEY = "nativeJson";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Nonnull private final String kickOffRequestUrl;

  @Nonnull private final String serverBaseUrl;

  @Nonnull private final WriteDetails writeDetails;

  /** Whether an access token is required to retrieve results. */
  @Getter private final boolean requiresAccessToken;

  /**
   * Creates a new ExportResponse.
   *
   * @param kickOffRequestUrl the original export request URL (used in the manifest)
   * @param serverBaseUrl the FHIR server base URL (used for constructing result URLs)
   * @param writeDetails the write details containing file information
   * @param requiresAccessToken whether access token is required to retrieve results
   */
  public ExportResponse(
      @Nonnull final String kickOffRequestUrl,
      @Nonnull final String serverBaseUrl,
      @Nonnull final WriteDetails writeDetails,
      final boolean requiresAccessToken) {
    this.kickOffRequestUrl = kickOffRequestUrl;
    this.serverBaseUrl = serverBaseUrl;
    this.writeDetails = writeDetails;
    this.requiresAccessToken = requiresAccessToken;
  }

  @Nonnull
  @Override
  public Parameters toOutput() {
    // Ensure the base URL ends with a slash for proper URL construction.
    final String normalizedBaseUrl =
        serverBaseUrl.endsWith("/") ? serverBaseUrl : serverBaseUrl + "/";

    // Build native JSON first.
    final ObjectNode json = buildJson(normalizedBaseUrl);

    // Convert to Parameters for FHIR compatibility.
    final Parameters parameters = jsonToParameters(json, normalizedBaseUrl);

    // Attach native JSON for direct extraction by the interceptor.
    parameters.setUserData(NATIVE_JSON_KEY, json.toString());

    return parameters;
  }

  /**
   * Builds the native JSON representation of the export manifest.
   *
   * @param normalizedBaseUrl the normalised base server URL
   * @return the JSON object
   */
  @Nonnull
  private ObjectNode buildJson(@Nonnull final String normalizedBaseUrl) {
    final ObjectNode json = MAPPER.createObjectNode();
    json.put("transactionTime", InstantType.now().getValueAsString());
    json.put("request", kickOffRequestUrl);
    json.put("requiresAccessToken", requiresAccessToken);

    // Output is always an array.
    final ArrayNode outputArray = json.putArray("output");
    for (final FileInformation fileInfo : writeDetails.fileInfos()) {
      final ObjectNode entry = outputArray.addObject();
      entry.put("type", fileInfo.fhirResourceType());
      entry.put("url", buildResultUrl(normalizedBaseUrl, fileInfo.absoluteUrl()));
    }

    // Error is always an array (even when empty).
    json.putArray("error");

    return json;
  }

  /**
   * Converts the JSON manifest to FHIR Parameters for compatibility.
   *
   * @param json the JSON object
   * @param normalizedBaseUrl the normalised base server URL
   * @return the Parameters resource
   */
  @Nonnull
  private Parameters jsonToParameters(
      @Nonnull final ObjectNode json, @Nonnull final String normalizedBaseUrl) {
    final Parameters parameters = new Parameters();

    // Add transactionTime parameter.
    parameters
        .addParameter()
        .setName("transactionTime")
        .setValue(new InstantType(json.get("transactionTime").asText()));

    // Add request parameter.
    parameters.addParameter().setName("request").setValue(new UriType(kickOffRequestUrl));

    // Add requiresAccessToken parameter.
    parameters
        .addParameter()
        .setName("requiresAccessToken")
        .setValue(new BooleanType(requiresAccessToken));

    // Add output parameters.
    for (final FileInformation fileInfo : writeDetails.fileInfos()) {
      final ParametersParameterComponent outputParam = parameters.addParameter().setName("output");
      outputParam.addPart().setName("type").setValue(new CodeType(fileInfo.fhirResourceType()));
      outputParam
          .addPart()
          .setName("url")
          .setValue(new UriType(buildResultUrl(normalizedBaseUrl, fileInfo.absoluteUrl())));
    }

    // Add empty error parameter to match JSON structure.
    parameters.addParameter().setName("error");

    return parameters;
  }

  /**
   * Converts a local file URL to a remote result URL.
   *
   * @param baseUrl the normalised base server URL
   * @param localUrl the local file URL containing the job ID and filename
   * @return the remote result URL
   */
  @Nonnull
  private static String buildResultUrl(
      @Nonnull final String baseUrl, @Nonnull final String localUrl) {
    try {
      final String[] parts = localUrl.split("/jobs/")[1].split("/");
      final String jobUuid = parts[0];
      final String file = parts[1];

      return new URIBuilder(baseUrl + "$result")
          .addParameter("job", jobUuid)
          .addParameter("file", file)
          .build()
          .toString();
    } catch (final URISyntaxException e) {
      throw new InternalErrorException(e);
    }
  }

  /**
   * Returns the original kick-off request URL.
   *
   * @return the kick-off request URL
   */
  @Nonnull
  public String getKickOffRequestUrl() {
    return kickOffRequestUrl;
  }

  /**
   * Returns the write details containing file information.
   *
   * @return the write details
   */
  @Nonnull
  public WriteDetails getWriteDetails() {
    return writeDetails;
  }

  @Override
  public boolean equals(final Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final ExportResponse that = (ExportResponse) o;
    return requiresAccessToken == that.requiresAccessToken
        && Objects.equals(kickOffRequestUrl, that.kickOffRequestUrl)
        && Objects.equals(writeDetails, that.writeDetails);
  }

  @Override
  public int hashCode() {
    return Objects.hash(kickOffRequestUrl, writeDetails, requiresAccessToken);
  }

  @Override
  public String toString() {
    return "ExportResponse{"
        + "kickOffRequestUrl='"
        + kickOffRequestUrl
        + '\''
        + ", writeDetails="
        + writeDetails
        + ", requiresAccessToken="
        + requiresAccessToken
        + '}';
  }
}
