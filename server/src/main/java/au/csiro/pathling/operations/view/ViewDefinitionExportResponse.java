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

package au.csiro.pathling.operations.view;

import au.csiro.pathling.OperationResponse;
import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import jakarta.annotation.Nonnull;
import java.net.URISyntaxException;
import java.util.List;
import lombok.Getter;
import org.apache.http.client.utils.URIBuilder;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.InstantType;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.UriType;

/**
 * Represents the response from a ViewDefinition export operation, containing the export manifest.
 *
 * @author John Grimes
 */
@Getter
public class ViewDefinitionExportResponse implements OperationResponse<Parameters> {

  @Nonnull private final String kickOffRequestUrl;

  @Nonnull private final String serverBaseUrl;

  @Nonnull private final List<ViewExportOutput> outputs;

  /** Whether an access token is required to retrieve results. */
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
  public Parameters toOutput() {
    final Parameters parameters = new Parameters();

    // Ensure the base URL ends with a slash for proper URL construction.
    final String normalizedBaseUrl =
        serverBaseUrl.endsWith("/") ? serverBaseUrl : serverBaseUrl + "/";

    // Add transactionTime parameter.
    parameters.addParameter().setName("transactionTime").setValue(InstantType.now());

    // Add request parameter.
    parameters.addParameter().setName("request").setValue(new UriType(kickOffRequestUrl));

    // Add requiresAccessToken parameter.
    parameters
        .addParameter()
        .setName("requiresAccessToken")
        .setValue(new BooleanType(requiresAccessToken));

    // Add output parameters.
    for (final ViewExportOutput output : outputs) {
      for (final String fileUrl : output.fileUrls()) {
        final ParametersParameterComponent outputParam =
            parameters.addParameter().setName("output");
        outputParam.addPart().setName("name").setValue(new StringType(output.name()));
        outputParam
            .addPart()
            .setName("url")
            .setValue(new UriType(buildResultUrl(normalizedBaseUrl, fileUrl)));
      }
    }

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
